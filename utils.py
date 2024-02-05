import pickle
import string
import nltk
from consts import Consts
from redis import ConnectionPool, Redis
from datasketch import MinHash
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import logging

redis_pool = ConnectionPool(host=Consts.REDIS_HOST, port=Consts.REDIS_PORT, db=Consts.REDIS_DB)

logging.basicConfig(filename="duplicate_service_utils.log",
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()

# Check if 'punkt' is already downloaded
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
    with Redis(connection_pool=redis_pool) as redis_connection:
        if not lsh_key:
            return None
        serialized_lsh = redis_connection.get(lsh_key)
        lsh = pickle.loads(serialized_lsh)
        return lsh


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
