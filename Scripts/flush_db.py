import redis

r = redis.Redis(host='tbcrawler21', port=6379, db=4)


def delete_all_keys():
    r.flushdb()


if __name__ == '__main__':
    delete_all_keys()
