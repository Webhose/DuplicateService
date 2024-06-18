import time
import redis
from consts import Consts
from elasticsearch import Elasticsearch


page_size = 1000
total_limit = 600000  # Set the total limit for processed documents
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
                    },
                    {
                        "range": {
                            "sys_info.crawled": {
                                "format": "strict_date_optional_time",
                                "gte": "2024-06-15T21:00:00.000Z",
                                "lte": "2024-06-16T21:00:00.000Z"
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
            if text and processed_documents < total_limit:
                redis_connection.hset(f"full-test-texts:{article_id}", mapping={"text": text, "article_domain": article_domain})
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


if __name__ == "__main__":
    """
    The script is used to generate test data for the DuplicateService.
    gets 1000 documents from the ES and stores the first 500 chars in the Redis.

    """
    get_texts_from_es()

