import json
import time

import requests
import tldextract
from hashlib import sha256
from utils import logger, Consts, store_article_in_redis
from metrics3 import metrics
from rabbit_utils import get_rabbit_connection
from rediscluster import RedisCluster, ClusterConnectionPool
from redis import Redis, ConnectionPool



def over_rpop(self, name, count=None):
    """
    overwriting original RPOP
    :param name:
    :param count:
    :return:
    """
    if count:
        try:
            return self.execute_command("RPOP", name, count)
        except Exception as err:
            pass
    return [self.execute_command("RPOP", name)]


def connect_to_redis(connect_details):
    """
    retrieves redis connection based on its configuration (Redis Connection or RedisCluster)
    :return: Redis or RedisCluster connections
    """
    # TODO: ADD VALIDATION
    _redis_connection = None
    _host = connect_details
    try:
        _hosts_list = _host
        if isinstance(_host, str):
            _hosts_list = json.loads(_host)

        if not _hosts_list:
            raise Exception("no hosts specified")

        # if configured host is a list of connections - connect to RedisCluster
        _redis_connection_pool = ClusterConnectionPool(startup_nodes=_hosts_list, max_connections=200)
        _redis_connection = RedisCluster(connection_pool=_redis_connection_pool, health_check_interval=10)

        logger.info(
            "successful connection pool has been established to Redis Cluster {hosts}".format(hosts=_hosts_list))
    except Exception as connect_error:
        metrics.count(Consts.TOTAL_FAILED_REDIS_CONNECTION)
        logger.error(
            "failed to create connection pool for Redis Cluster with error: {error}".format(error=connect_error))
        host = connect_details[0].get('host')
        port = connect_details[0].get('port')
        db = connect_details[0].get('db')
        _redis_connection_pool = ConnectionPool(host=host, port=port, db=db)
        _redis_connection = Redis(connection_pool=_redis_connection_pool, health_check_interval=10)
        return _redis_connection

    # overwriting rpop functionality to support rpop with count (Redis >= 6.2.0)
    setattr(_redis_connection, "rpop", over_rpop.__get__(_redis_connection, _redis_connection.__class__))
    # return which redis connection created
    return _redis_connection


def get_reddis_connection(site_type):
    reddis_config = {
        # "mainstream": [{"host": "localhost", "port": "6379", "db": 3}],
        "mainstream": [{"host": "redis-news-002", "port": "6379"}, {"host": "redis-news-004", "port": "6379"}, {"host": "redis-news-005", "port": "6379"}],
        # "blogs": [{"host": "redis-blogs-001", "port": "6379"}, {"host": "redis-blogs-002", "port": "6379"}, {"host": "redis-blogs-003", "port": "6379"}],
        # "discussion": [{"host": "redis-discussions-001", "port": "6379"}, {"host": "redis-discussions-002", "port": "6379"}, {"host": "redis-discussions-003", "port": "6379"}],
    }
    return connect_to_redis(reddis_config.get(site_type))


def get_tld_from_url(url):
    ext = tldextract.extract(url)
    return ext.registered_domain or ext.domain


def push_to_distribution_queue(document, method="NBDR"):
    massage = "{method} {message}".format(
        method=method, message=json.dumps(document, default=lambda obj: getattr(obj, '__dict__', str(obj)))
    )
    redis_connection = get_reddis_connection(document.get('index'))
    if massage and redis_connection:
        redis_connection.lpush("distribution", massage)
    else:
        logger.error("Failed to push document to distribution queue")
        metrics.count(Consts.TOTAL_DOCUMENTS_FAILED_DISTRIBUTION)


def validate_document(body):
    # TODO consider to add a retry mechanism
    """
    Validate the document by sending it to the DuplicateService
    :param body: The document to validate
    :return: return updated doc with the new field.
    """
    try:
        url = body.get('topicRecord').get('url')
        article_id = sha256(url.encode()).hexdigest()
        data = {
            "content": body.get('topicRecord').get('topic'),
            "language": body.get('language'),
            "domain": get_tld_from_url(url),
            "article_id": article_id
        }
        response = requests.post(f'http://{Consts.HOST}:9039/is_duplicate', json=data)
        if response.ok:
            message = response.json().get('status')
            if message == Consts.SIMILARITY:
                metrics.count(Consts.TOTAL_SIMILARITY)
                url = body.get('topicRecord').get('url')
                store_article_in_redis(url)
                body['syndicated'] = True
            elif message == Consts.DUPLICATE:
                metrics.count(Consts.TOTAL_DUPLICATE)
                url = body.get('topicRecord').get('url')
                store_article_in_redis(url, queue_name="duplicate")
            elif message == Consts.DUPLICATE_KEYS:
                metrics.count(Consts.TOTAL_DUPLICATE_KEYS)
            elif message == Consts.UNIQUE:
                metrics.count(Consts.TOTAL_UNQIUE)
                logger.info("document is not syndication and send to DSS")
            else:
                metrics.count(Consts.TOTAL_OTHER)

        else:
            metrics.count(Consts.TOTAL_DUPLICATE_REQUESTS_NOT_OK)
            logger.critical(f"Failed to get response from DuplicateService with the following error: {response.text}")
    except Exception as e:
        metrics.count(Consts.TOTAL_DUPLICATE_REQUESTS_ERROR)
        logger.critical(f"Failed to validate document with the following error: {e}")
        return


def callback(ch, method, properties, body):
    metrics.count(Consts.TOTAL_DOCUMENTS)
    body = json.loads(body)
    logger.info(f"Received document: {body}")
    validate_document(body)
    logger.info(f"Pushing document to distribution queue: {body}")
    push_to_distribution_queue(body)


def start_consumer(connection):
    logger.info("Starting consumer...")
    channel = connection.channel()
    channel.queue_declare(queue='SyndicationQueue', durable=True)
    channel.basic_consume(queue='SyndicationQueue', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


def main():
    connection = get_rabbit_connection()
    if not connection:
        logger.error("Failed to get RabbitMQ connection. Exiting.")
        metrics.count(Consts.TOTAL_FAILED_FAILED_RABBIT_CONNECTION)
        return
    while True:
        try:
            start_consumer(connection)
        except Exception as e:
            connection = get_rabbit_connection()
            if not connection:
                logger.error("Failed to get RabbitMQ connection. Exiting.")
                metrics.count(Consts.TOTAL_FAILED_FAILED_RABBIT_CONNECTION)
                logger.info("Retrying to start consumer in 30 seconds...")
                time.sleep(30)
                continue
            metrics.count(Consts.TOTAL_FAILED_CONSUME)
            logger.critical(f"Failed to start consumer with the following error: {e}")


if __name__ == '__main__':
    logger.info("Starting RabbitMQ consumer...")
    main()
