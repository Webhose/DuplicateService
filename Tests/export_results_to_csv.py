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
            article_id = item.decode().split(":")[0]
            results[key.decode()].append(article_id)
    print(f"Total articles: {total_ids}")
    return results


def write_to_csv(results):
    with open('results.csv', 'w') as f:
        for key, value in results.items():
            f.write("%s,%s\n" % (key, value))


def main():
    results = get_results()
    write_to_csv(results)


if __name__ == "__main__":
    main()
