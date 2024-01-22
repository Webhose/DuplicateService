from elasticsearch import Elasticsearch
import redis
import time

page_size = 1000
redis_connection = redis.Redis(host='crawler16', port=6379, db=4)


def get_es_connection():
    try:
        client = Elasticsearch("http://spirit-004:9200")
        return client
    except Exception as e:
        print(f"Failed to connect to Elasticsearch with the following error: {e}")
        return None


def get_query(ids):
    query = {
        "_source": ["title", "thread.site", "_id"],
        "query": {
            "terms": {
                "_id": ids
            }
        },
        "size": page_size
    }

    return query


def get_domain_and_title_from_es(ids):
    try:
        print("Starting getting text from ES...")
        es_client = get_es_connection()
        # Perform the initial search to get the initial scroll ID
        initial_query = get_query(ids)
        result = es_client.search(index="webhose*", body=initial_query)
        total_hits = result["hits"]["hits"]

        domain_title_list = []
        for hit in total_hits:
            domain = hit["_source"]["thread"]["site"]
            title = hit["_source"]["title"]
            _id = hit["_id"]
            domain_title_list.append((domain, title, _id))

        return domain_title_list
    except Exception as e:
        print(f"Failed to get text from ES with the following error: {e}")
        return None


def main():
    keys = redis_connection.keys()
    domain_titles_list = []
    for key in keys:
        try:
            domain_titles_dict = {}
            ids = redis_connection.smembers(key)
            ids = list(ids)
            ids = [id.decode('utf-8').split('|')[0] for id in ids]
            ids.append(key.decode('utf-8'))

            domain_title_list = get_domain_and_title_from_es(ids)
            for domain, title, _id in domain_title_list:
                try:
                    print(_id)
                    if domain not in domain_titles_dict:
                        domain_titles_dict[domain] = []
                    domain_titles_dict[domain].append(title)
                except Exception as e:
                    pass
            domain_titles_list.append(domain_titles_dict)
            for key in domain_titles_dict:
                values = domain_titles_dict[key]
                unique_titles_count = set(values)
                print(f"{key}: {len(values)} - {len(unique_titles_count)}")
        except Exception as e:
            print(f"Failed to get text from ES with the following error: {e}")


if __name__ == "__main__":
    main()
