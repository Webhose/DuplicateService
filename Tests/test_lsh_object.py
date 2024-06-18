import redis
import requests
from utils import *
import concurrent.futures
from itertools import islice

page_size = 1000
total_limit = 10000  # Set the total limit for processed documents
redis_connection = redis.Redis(host=Consts.REDIS_HOST, port=Consts.REDIS_PORT, db=Consts.REDIS_DB)


def test_query(new_text, article_domain, article_id):
    try:
        data = {
            "content": new_text,
            "language": "english",
            "domain": article_domain,
            "article_id": article_id
        }

        response = requests.post(f'http://{Consts.HOST}:9039/is_duplicate', json=data)
        if response.ok:
            if "duplicate" in response.text:
                redis_connection.sadd("duplicate", article_id)
            elif "similarity" in response.text:
                redis_connection.sadd("similarity", article_id)
        else:
            logger.critical(f"Failed to get response from DuplicateService with the following error: {response.text}")
    except Exception as e:
        logger.critical(f"Failed to get data from ES with the following error: {e}")
        return


@timeit_decorator
def run_test(documens):
    logger.info("Starting running test...")
    for text, article_domain, article_id in documens:
        test_query(text, article_domain, article_id)


# def fetch_article_data_batch(batch):
#     pipe = redis_connection.pipeline()
#     for article in batch:
#         article_id = article.decode().split(":")[1]
#         pipe.hgetall(f"test-texts:{article_id}")
#     batch_data = pipe.execute()
#
#     results = []
#     for article, article_data in zip(batch, batch_data):
#         article_id = article.decode().split(":")[1]
#         text = article_data[b'text'].decode()
#         article_domain = article_data[b'article_domain'].decode()
#         results.append((text, article_domain, article_id))
#
#     return results
#
#
# # Function to get articles in batches
# def batched(iterable, batch_size):
#     it = iter(iterable)
#     while batch := list(islice(it, batch_size)):
#         yield batch
#
#
# def get_documents_from_redis(batch_size=100):
#     documents = []
#     articles = redis_connection.keys("test-texts:*")
#
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         futures = [executor.submit(fetch_article_data_batch, batch) for batch in batched(articles, batch_size)]
#         for future in concurrent.futures.as_completed(futures):
#             documents.extend(future.result())
#
#     return documents
def fetch_article_data(article):
    article_id = article.decode().split(":")[1]
    article_data = redis_connection.hgetall(f"test-texts:{article_id}")
    text = article_data[b'text'].decode()
    article_domain = article_data[b'article_domain'].decode()
    return text, article_domain, article_id


def get_documents_from_redis():
    documents = []
    articles = redis_connection.keys("test-texts:*")

    # Using ThreadPoolExecutor to fetch article data in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(fetch_article_data, articles))

    # Append results to documents
    documents.extend(results)
    return documents


if __name__ == "__main__":
    documents = get_documents_from_redis()
    run_test(documents)
