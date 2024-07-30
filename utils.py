import pickle
import string
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


# Decorator to measure function execution time
def timeit_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = (end_time - start_time)
        logger.info(f"Function '{func.__name__}' took {execution_time:.4f} seconds to execute.")
        return result

    return wrapper


# Preprocess and tokenize the input text
def preprocess_and_tokenize(text, language):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words(language))
    tokens = [token for token in tokens if token not in stop_words]
    return tokens


# Generate MinHash signature for a document
async def minhash_signature(document, language, num_perm=128):
    minhash = MinHash(num_perm=num_perm)
    tokens = preprocess_and_tokenize(document, language)
    for token in tokens:
        minhash.update(token.encode('utf-8'))
    return minhash


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
        lsh_with_ttl = MinHashLSHTTL(threshold=0.95, num_perm=128)
    except Exception as e:
        logger.error(f"Error while getting LSH from Redis: {str(e)}")
    finally:
        return lsh_with_ttl


# Save LSH objects to Redis
async def save_lsh_to_redis(lsh_cache_dict):
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
async def run_lsh_check(**kwargs):
    content = kwargs.get('content')
    language = kwargs.get('language')
    article_domain = kwargs.get('article_domain')
    article_id = kwargs.get('article_id')
    lsh_cache = kwargs.get('lsh_cache')

    if not lsh_cache:
        return None

    minhash = await minhash_signature(content, language)
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
