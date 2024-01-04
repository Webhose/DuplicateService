import pickle
import string
import nltk
import redis
from datasketch import MinHash, MinHashLSH
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nltk.download('punkt')
nltk.download('stopwords')


def preprocess_and_tokenize(text):
    # Lowercase the text
    text = text.lower()

    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))

    # Tokenize the text using NLTK
    tokens = word_tokenize(text)

    # Remove stopwords
    stop_words = set(stopwords.words('english'))
    tokens = [token for token in tokens if token not in stop_words]

    return tokens


def minhash_signature(document, num_perm=128):
    minhash = MinHash(num_perm=num_perm)

    # Tokenize and preprocess the document
    tokens = preprocess_and_tokenize(document)

    # Update the Minhash with each token
    for token in tokens:
        minhash.update(token.encode('utf-8'))

    return minhash


def get_lsh_from_redis(redis_connection=redis.Redis(db=3), lsh_key=None):
    if not lsh_key:
        return None
    serialized_lsh = redis_connection.get(lsh_key)
    lsh = pickle.loads(serialized_lsh)
    return lsh


def update_lsh_in_redis(lsh, minhash, article_id, article_domain, redis_connection=redis.Redis(db=3), lsh_key=None):
    try:
        # Insert Minhash into LSH
        lsh.insert(f"{article_id}|{article_domain}", minhash)
        # Serialize and store the LSH model in Redis
        serialized_lsh = pickle.dumps(lsh)
        redis_connection.set(lsh_key, serialized_lsh)
    except Exception as e:
        print(e)
        pass