import pickle
import string
from datetime import datetime, timedelta
import nltk
from consts import Consts
from minhash_lsh_ttl import MinHashLSHTTL
from redis import ConnectionPool, Redis
from datasketch import MinHash
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import logging
import time
import os
import metrics3_docker.metrics as metrics
from elasticsearch import Elasticsearch
from concurrent.futures import ProcessPoolExecutor, as_completed

redis_pool = ConnectionPool(host=Consts.REDIS_HOST, port=Consts.REDIS_PORT, db=Consts.REDIS_DB)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.INFO)

logger = logging.getLogger()

logging.getLogger('redis').setLevel(logging.WARNING)
logging.getLogger('rediscluster').setLevel(logging.WARNING)

# Check if 'punkt' is already downloaded
# downloaded in Dockerfile
# try:
#    nltk.data.find('tokenizers/punkt')
# except LookupError:
#    logger.info("The 'punkt' resource is not downloaded. You may want to download it.")
#    nltk.download('punkt')

# try:
#    nltk.data.find('corpora/stopwords')
# except LookupError:
#    logger.info("The 'stopwords' resource is not downloaded. You may want to download it.")
#    nltk.download('stopwords')

nltk_data_path = os.getenv('NLTK_DATA', '/usr/local/share/nltk_data')
nltk.data.path.append(nltk_data_path)

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', download_dir=nltk_data_path)

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    logger.info("The 'stopwords' resource is not downloaded. You may want to download it.")
    nltk.download('stopwords')


# Preprocess and tokenize the input text
def preprocess_and_tokenize(text, language):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words(language))
    tokens = [token for token in tokens if token not in stop_words]
    return tokens


# Generate MinHash signature for a document
def minhash_signature(document, language, num_perm=128):
    minhash = MinHash(num_perm=num_perm)
    tokens = preprocess_and_tokenize(document, language)
    for token in tokens:
        minhash.update(token.encode('utf-8'))
    return minhash


def get_es_connection():
    try:
        client = Elasticsearch("http://spirit-004:9200")
        return client
    except Exception as e:
        print(f"Failed to connect to Elasticsearch with the following error: {e}")
        return None


def get_query(scroll_id=None, page_size=500, max_hours=Consts.MAX_HOURS_FOR_RECOVERY):
    today = datetime.now()
    yesterday = today - timedelta(hours=max_hours)
    today = today.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    yesterday = yesterday.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    query = {
        "_source": ["text", "_id", "thread.site"],
        "query": {
            "bool": {
                "must": [
                    {"match": {"thread.site_type": "news"}},
                    {"match": {"language": "english"}},
                    {"match": {"is_first": True}},
                    {
                        "range": {
                            "sys_info.crawled": {
                                "format": "strict_date_optional_time",
                                "gte": yesterday,
                                "lte": today
                            }
                        }
                    }
                ]
            }
        },
        "size": page_size
    }

    if scroll_id:
        query["_scroll_id"] = scroll_id
    return query


def get_texts_from_es(language="english"):
    """
    Retrieve texts from Elasticsearch and insert MinHash signatures into LSH.
    """
    documents = []
    es_client = get_es_connection()
    if not es_client:
        logger.error("Failed to connect to Elasticsearch.")
        return documents

    # Perform the initial search to get the initial scroll ID
    initial_query = get_query()
    try:
        result = es_client.search(index="webhose*", body=initial_query, scroll="5m")
    except Exception as e:
        logger.error(f"Error during initial search: {e}")
        return documents

    scroll_id = result.get("_scroll_id")
    total_hits = result["hits"]["total"]["value"]

    total_pages = total_hits // 500 + 1

    for i in range(total_pages):
        logger.info(f"Processing page {i + 1} of {total_pages}")
        hits = result["hits"]["hits"]

        if not hits:
            break  # No more results, break out of the loop

        for hit in hits:
            text = hit.get("_source", {}).get("text")
            if not text:
                continue
            article_id = hit.get("_id")
            article_domain = hit.get("_source", {}).get("thread", {}).get("site")
            documents.append({
                "article_id": article_id,
                "article_domain": article_domain,
                "text": text,
            })

        try:
            # Use the scroll ID to retrieve the next batch of results
            scroll_id = result.get("_scroll_id")
            result = es_client.scroll(scroll_id=scroll_id, scroll="5m")
        except Exception as e:
            logger.error(f"Error during scrolling: {e}")
            break

    # Clear the scroll to release resources on the server
    try:
        es_client.clear_scroll(scroll_id=scroll_id)
    except Exception as e:
        logger.error(f"Error during clearing scroll: {e}")

    return documents


def process_batch(documents):
    results = []
    for doc in documents:
        minhash = minhash_signature(doc.get('text'), doc.get('language'))
        results.append({
            "article_id": doc.get('article_id'),
            "article_domain": doc.get('article_domain'),
            "minhash": minhash
        })
    logger.info(f"Processed {len(results)} documents.")
    return results


def process_batches(lsh_with_ttl, documents, workers=4, batch_size=1000):
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for i in range(0, len(documents), batch_size):
            logger.info(f"Processing batch {i // batch_size + 1}...")
            batch = documents[i:i + batch_size]
            future = executor.submit(process_batch, batch)
            futures[future] = i // batch_size + 1

        results = {}
        for future in as_completed(futures):
            batch_number = futures[future]
            results[batch_number] = future.result()

    logger.info("Finished processing all batches.")
    logger.info(f"Inserting MinHash signatures into LSH...")
    # Insert into LSH with batch insertion
    with lsh_with_ttl.lsh.insertion_session() as session:
        for batch_number, batch_results in results.items():
            logger.info(f"Inserting batch {batch_number}...")
            for doc in batch_results:
                key = f"{doc.get('article_id')}|{doc.get('article_domain')}"
                session.insert(key, doc.get('minhash'))


def fast_recovery():
    """
    Initialize LSH with TTL and load documents from Elasticsearch.
    """
    start_time = time.time()
    logger.info("Starting fast recovery...")
    lsh_with_ttl = MinHashLSHTTL(threshold=0.9, num_perm=128)
    documents = get_texts_from_es()
    process_batches(lsh_with_ttl, documents)
    end_time = time.time()
    logger.info(f"Fast recovery took {end_time - start_time:.4f} seconds.")
    return lsh_with_ttl


# Retrieve LSH object from Redis
def get_lsh_from_redis(lsh_key=None):
    lsh_with_ttl = None
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            if not lsh_key:
                return None
            serialized_lsh = redis_connection.get(lsh_key)
            if serialized_lsh:
                lsh_with_ttl = pickle.loads(serialized_lsh)
            else:
                raise TypeError("LSH object not found in Redis.")
    except TypeError:
        metrics.count(Consts.TOTAL_LSH_OBJECT_CREATED)
        logger.error("LSH object not found in Redis. Creating new LSH.")
        lsh_with_ttl = fast_recovery()
    except Exception as e:
        logger.error(f"Error while getting LSH from Redis: {str(e)}")
    finally:
        return lsh_with_ttl


# Save LSH objects to Redis
def save_lsh_to_redis(lsh_cache_dict):
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            for language, lsh_with_ttl in lsh_cache_dict.items():
                lsh_key = f"{language}:lsh_index"
                serialized_lsh = pickle.dumps(lsh_with_ttl)
                redis_connection.set(lsh_key, serialized_lsh)
    except Exception as e:
        logger.critical(f"Failed to save LSH to Redis: {str(e)}")


# Update Redis with candidates for duplicate detection
def update_candidates_duplicates_in_redis(article_id, candidates):
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            redis_connection.sadd(article_id, *candidates)
    except Exception as e:
        logger.critical(f"Failed to update candidates duplicates in Redis: {str(e)}")


# Determine status from candidate pairs
def get_status_from_candidates(article_domain, candidate_pairs, article_id):
    candidate_pairs = [pair for pair in candidate_pairs if article_id not in pair]
    if candidate_pairs:
        status = Consts.DUPLICATE if any(
            pair.split('|')[1] == article_domain for pair in candidate_pairs) else Consts.SIMILARITY
    else:
        status = Consts.UNIQUE
    return status


# Run LSH check to determine document status
def run_lsh_check(**kwargs):
    content = kwargs.get('content')
    language = kwargs.get('language')
    article_domain = kwargs.get('article_domain')
    article_id = kwargs.get('article_id')
    lsh_cache = kwargs.get('lsh_cache')

    if not lsh_cache:
        return None

    minhash = minhash_signature(content, language)
    lsh_cache.insert(f"{article_id}|{article_domain}", minhash)
    candidate_pairs = lsh_cache.query(minhash)
    return get_status_from_candidates(article_domain, candidate_pairs, article_id)


# Store article in Redis queue
def store_article_in_redis(url, queue_name="similarity"):
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            redis_connection.sadd(queue_name, url)
    except Exception as e:
        logger.critical(f"Failed to store article in Redis: {str(e)}")
