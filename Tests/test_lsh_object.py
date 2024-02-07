import time
import redis
from consts import Consts
from elasticsearch import Elasticsearch
import requests


page_size = 1000
total_limit = 10000  # Set the total limit for processed documents
redis_connection = redis.Redis(host=Consts.REDIS_HOST, port=Consts.REDIS_PORT, db=Consts.REDIS_DB)


def get_es_connection():
    try:
        client = Elasticsearch("http://spirit-004:9200")
        return client
    except Exception as e:
        print(f"Failed to connect to Elasticsearch with the following error: {e}")
        return None


def get_query(scroll_id=None):
    query = {
        "_source": ["text", "_id", "thread.site"],
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "thread.site_type": "news"
                        }
                    },
                    {
                        "match": {
                            "language": "english"
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


def get_texts_from_es():
    print("Starting getting text from ES...")
    start_time = time.time()

    es_client = get_es_connection()
    # Perform the initial search to get the initial scroll ID
    initial_query = get_query()
    result = es_client.search(index="webhose*", body=initial_query, scroll="5m")
    scroll_id = result.get("_scroll_id")
    total_hits = result["hits"]["total"]["value"]

    # Set the initial page size and number
    page_number = 1
    processed_documents = 0

    while scroll_id and total_hits > 0 and processed_documents < total_limit:
        # Use the scroll ID to retrieve the next batch of results
        result = es_client.scroll(scroll_id=scroll_id, scroll="5m")
        scroll_id = result.get("_scroll_id")
        hits = result["hits"]["hits"]

        if not hits:
            break  # No more results, break out of the loop

        for hit in hits:
            text = hit.get("_source").get("text")
            article_id = hit.get("_id")
            article_domain = hit.get("_source").get("thread").get("site")
            if text:
                redis_connection.hset(f"test-texts:{article_id}", mapping={"text": text, "article_domain": article_domain})
                processed_documents += 1

        total_hits -= len(hits)

        # Print some progress information
        print(f"Processed {processed_documents} documents, remaining: {total_limit - processed_documents}")

        # Increment the page number for the next iteration
        page_number += 1

    # Clear the scroll to release resources on the server
    es_client.clear_scroll(scroll_id=scroll_id)
    elapsed_time = (time.time() - start_time) / 60
    print(f"Elapsed time for getting text from ES: {elapsed_time:.2f} minutes")


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
    # get_texts_from_es()
    run_test()

