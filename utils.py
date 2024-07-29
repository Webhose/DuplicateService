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
from metrics3 import metrics

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


def timeit_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = (end_time - start_time)
        print(f"Function '{func.__name__}' took {execution_time:.4f} seconds to execute.")
        return result

    return wrapper


def preprocess_and_tokenize(text, language):
    # Lowercase the text
    text = text.lower()

    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))

    # Tokenize the text using NLTK
    tokens = word_tokenize(text)

    # Remove stopwords
    stop_words = set(stopwords.words(f'{language}'))
    tokens = [token for token in tokens if token not in stop_words]

    return tokens


async def minhash_signature(document, language, num_perm=128):
    minhash = MinHash(num_perm=num_perm)

    # Tokenize and preprocess the document
    tokens = preprocess_and_tokenize(document, language)

    # Update the Minhash with each token
    for token in tokens:
        minhash.update(token.encode('utf-8'))

    return minhash


def get_lsh_from_redis(lsh_key=None):
    """
    Get the LSH object from Redis. If the object is not found, create a new LSH object.
    """
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
        # need to create new lsh
        metrics.count(Consts.TOTAL_LSH_OBJECT_CREATED)
        logger.error(f"Error while getting LSH from Redis: Creating new LSH")
        lsh_with_ttl = MinHashLSHTTL(threshold=0.95, num_perm=128)
    except Exception as e:
        logger.error(f"Error while getting LSH from Redis: {str(e)}")
    finally:
        return lsh_with_ttl


async def save_lsh_to_redis(lsh_cache_dict):
    """
    Save the LSH object to Redis.
    """
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            for language, lsh_with_ttl in lsh_cache_dict.items():
                lsh_key = f"{language}:lsh_index"
                serialized_lsh = pickle.dumps(lsh_with_ttl)
                redis_connection.set(lsh_key, serialized_lsh)
    except Exception as e:
        logger.critical(f"Failed to save LSH to Redis: {str(e)}")


def update_candidates_duplicates_in_redis(article_id, candidates):
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            redis_connection.sadd(f"{article_id}", *candidates)
    except Exception as e:
        logger.critical(f"Failed to update candidates duplicates in Redis: {str(e)}")


def get_status_from_candidates(article_domain, candidate_pairs, article_id):
    # Simplified and optimized version
    candidate_pairs = [pair for pair in candidate_pairs if article_id not in pair]

    if candidate_pairs:
        # Check if any candidate pair has the same article_domain as the current article
        status = Consts.DUPLICATE if any(
            pair.split('|')[1] == article_domain for pair in candidate_pairs) else Consts.SIMILARITY
        # Update candidates in Redis for potential duplicates
        # update_candidates_duplicates_in_redis(article_id=article_id, candidates=candidate_pairs)
    else:
        # No candidate pairs found
        status = Consts.UNIQUE
    return status


async def run_lsh_check(**kwargs):
    """
    Get the status of the document (duplicate or similarity) based on the content, language, article_domain, and article_id.
    """
    content = kwargs.get('content')
    language = kwargs.get('language')
    article_domain = kwargs.get('article_domain')
    article_id = kwargs.get('article_id')
    lsh_cache = kwargs.get('lsh_cache')

    if not lsh_cache:
        return None

    # calculate minhash for the new text
    minhash = await minhash_signature(content, language)
    # Insert Minhash into LSH
    lsh_cache.insert(f"{article_id}|{article_domain}", minhash)

    # Check for duplicates or similarity
    candidate_pairs = lsh_cache.query(minhash)
    return get_status_from_candidates(article_domain, candidate_pairs, article_id)


def store_article_in_redis(url, queue_name="similarity"):
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            redis_connection.sadd(queue_name, url)
    except Exception as e:
        logger.critical(f"Failed to store article in Redis: {str(e)}")
