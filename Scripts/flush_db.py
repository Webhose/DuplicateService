import redis
from consts import Consts

r = redis.Redis(host=Consts.REDIS_HOST, port=6379, db=4)


def delete_all_keys():
    r.flushdb()


if __name__ == '__main__':
    delete_all_keys()
