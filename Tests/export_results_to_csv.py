import redis
from utils import *
import collections

redis_connection = redis.Redis(host=Consts.REDIS_HOST, port=Consts.REDIS_PORT, db=Consts.REDIS_DB)


def get_results():
    total_ids = 0
    results = collections.defaultdict(list)
    keys = redis_connection.smembers("similarity")
    for i, key in enumerate(keys):
        print(f"Processing key {i + 1}/{len(keys)}")
        data = redis_connection.smembers(key.decode())
        total_ids += 1 + len(data)
        for item in data:
            article_id, article_domain = item.decode().split("|")
            results[key.decode()].append((article_id, article_domain))
    print(f"Total articles: {total_ids}")
    return results


def write_to_csv(results):
    headers = ["doc UUID", "domain", "similar doc UUID", "similar to domain"]
    with open('results.csv', 'w') as f:
        f.write(",".join(headers) + "\n")
        for key, value in results.items():
            domain = redis_connection.hmget(f"test-texts:{key}", "article_domain")[0].decode()
            for article_id, article_domain in value:
                f.write("%s,%s,%s,%s\n" % (key, domain, article_id, article_domain))


def main():
    results = get_results()
    write_to_csv(results)


if __name__ == "__main__":
    main()
