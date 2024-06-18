import pickle
import string
import nltk
from consts import Consts
from redis import ConnectionPool, Redis
from datasketch import MinHash, MinHashLSH
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import logging
import time

redis_pool = ConnectionPool(host=Consts.REDIS_HOST, port=Consts.REDIS_PORT, db=Consts.REDIS_DB)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()

# Check if 'punkt' is already downloaded
# downloaded in Dockerfile
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    logger.info("The 'punkt' resource is not downloaded. You may want to download it.")
    nltk.download('punkt')

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


def get_lsh_from_redis(redis_pool=redis_pool, lsh_key=None):
    lsh = None
    try:
        with Redis(connection_pool=redis_pool) as redis_connection:
            if not lsh_key:
                return None
            serialized_lsh = redis_connection.get(lsh_key)
            lsh = pickle.loads(serialized_lsh)
    except TypeError:
        # need to create new lsh
        logger.error(f"Error while getting LSH from Redis: Creating new LSH")
        lsh = MinHashLSH(threshold=0.9, num_perm=128)
    except Exception as e:
        logger.error(f"Error while getting LSH from Redis: {str(e)}")
    finally:
        return lsh


async def update_lsh_in_redis_batch(lsh_cache):
    try:
        # Create a connection from the pool
        with Redis(connection_pool=redis_pool) as redis_connection:
            # Serialize and store the LSH model in Redis
            serialized_lsh = pickle.dumps(lsh_cache)
            redis_connection.set("en:lsh_index", serialized_lsh)
    except Exception as e:
        logger.critical(f"Failed to update LSH in Redis: {str(e)}")


def update_lsh_in_redis(lsh, minhash, article_id, article_domain, redis_pool=redis_pool, lsh_key=None):
    try:
        # Create a connection from the pool
        with Redis(connection_pool=redis_pool) as redis_connection:
            # Insert Minhash into LSH
            lsh.insert(f"{article_id}|{article_domain}", minhash)

            # Serialize and store the LSH model in Redis
            serialized_lsh = pickle.dumps(lsh)
            redis_connection.set(lsh_key, serialized_lsh)
    except Exception as e:
        logger.critical(f"Failed to update LSH in Redis: {str(e)}")


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
        update_candidates_duplicates_in_redis(article_id=article_id, candidates=candidate_pairs)
    else:
        # No candidate pairs found
        status = None
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
