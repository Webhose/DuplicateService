import time
import redis
from consts import Consts
import requests


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
            print(f"Failed to get response from DuplicateService with the following error: {response.text}")
    except Exception as e:
        print(f"Failed to get data from ES with the following error: {e}")
        return


def run_test():
    print("Starting running test...")
    start_time = time.time()

    articles = redis_connection.keys("test-texts:*")
    for article in articles:
        article_id = article.decode().split(":")[1]
        text = redis_connection.hget(f"test-texts:{article_id}", "text").decode()
        article_domain = redis_connection.hget(f"test-texts:{article_id}", "article_domain").decode()
        test_query(text, article_domain, article_id)

    elapsed_time = (time.time() - start_time) / 60
    print(f"Elapsed time for getting text from ES: {elapsed_time:.2f} minutes")


if __name__ == "__main__":
    run_test()

