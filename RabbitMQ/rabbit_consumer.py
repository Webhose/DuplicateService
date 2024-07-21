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
    Overwriting original RPOP to support RPOP with count (Redis >= 6.2.0)
    """
    try:
        if count:
            return self.execute_command("RPOP", name, count)
        return self.execute_command("RPOP", name)
    except Exception as err:
        logger.error(f"RPOP failed with error: {err}")
        return None


def connect_to_redis(connect_details):
    """
    Retrieves Redis connection based on its configuration (Redis Connection or RedisCluster)
    :return: Redis or RedisCluster connections
    """
    try:
        _hosts_list = connect_details if not isinstance(connect_details, str) else json.loads(connect_details)
        if not _hosts_list:
            raise ValueError("No hosts specified")

        if len(_hosts_list) > 1:
            return connect_to_redis_cluster(_hosts_list)
        else:
            return connect_to_single_redis_instance(_hosts_list[0])
    except json.JSONDecodeError as json_err:
        logger.error(f"JSON decoding failed: {json_err}")
    except Exception as err:
        metrics.count(Consts.TOTAL_FAILED_REDIS_CONNECTION)
        logger.error(f"Failed to create Redis connection: {err}")
        return None


def connect_to_redis_cluster(hosts_list):
    try:
        pool = ClusterConnectionPool(startup_nodes=hosts_list, max_connections=200)
        redis_connection = RedisCluster(connection_pool=pool, health_check_interval=10)
        setattr(redis_connection, "rpop", over_rpop.__get__(redis_connection, redis_connection.__class__))
        logger.info(f"Successful connection pool established to Redis Cluster: {hosts_list}")
        return redis_connection
    except Exception as err:
        logger.error(f"Failed to connect to Redis Cluster: {err}")
        raise


def connect_to_single_redis_instance(host_details):
    try:
        pool = ConnectionPool(host=host_details['host'], port=host_details['port'], db=host_details.get('db', 0))
        redis_connection = Redis(connection_pool=pool, health_check_interval=10)
        setattr(redis_connection, "rpop", over_rpop.__get__(redis_connection, redis_connection.__class__))
        logger.info(f"Connected to single Redis instance at {host_details['host']}:{host_details['port']}")
        return redis_connection
    except Exception as err:
        logger.error(f"Failed to connect to single Redis instance: {err}")
        raise


def get_redis_connection(site_type):
    redis_config = {
        # "mainstream": [{"host": "localhost", "port": 6379, "db": 3}],
        "mainstream": [
            {"host": "redis-news-002", "port": "6379"}, {"host": "redis-news-004", "port": "6379"},
            {"host": "redis-news-005", "port": "6379"}
        ]
    }
    return connect_to_redis(redis_config.get(site_type))


def get_tld_from_url(url):
    ext = tldextract.extract(url)
    return ext.registered_domain or ext.domain


def push_to_distribution_queue(document, method="NBDR"):
    message = f"{method} {json.dumps(document, default=lambda obj: getattr(obj, '__dict__', str(obj)))}"
    redis_connection = get_redis_connection(document.get('index'))

    if message and redis_connection:
        redis_connection.lpush("distribution", message)
    else:
        logger.error("Failed to push document to distribution queue")
        metrics.count(Consts.TOTAL_DOCUMENTS_FAILED_DISTRIBUTION)


def validate_document(body):
    """
    Validate the document by sending it to the DuplicateService
    """
    try:
        logger.info("Validating document")
        url = body.get('topicRecord').get('url')
        article_id = sha256(url.encode()).hexdigest()
        data = {
            "content": body.get('topicRecord').get('topic'),
            "language": body.get('language'),
            "domain": get_tld_from_url(url),
            "article_id": article_id
        }
        response = requests.post(f'http://{Consts.HOST}:9039/is_duplicate', json=data)
        response.raise_for_status()

        message = response.json().get('status')
        if message == Consts.SIMILARITY:
            metrics.count(Consts.TOTAL_SIMILARITY)
            store_article_in_redis(url)
            body['syndicated'] = True
        elif message == Consts.DUPLICATE:
            metrics.count(Consts.TOTAL_DUPLICATE)
            store_article_in_redis(url, queue_name="duplicate")
        elif message == Consts.DUPLICATE_KEYS:
            metrics.count(Consts.TOTAL_DUPLICATE_KEYS)
        elif message == Consts.UNIQUE:
            metrics.count(Consts.TOTAL_UNIQUE)
            logger.info("Document is not a syndication, sending to DSS")
        else:
            metrics.count(Consts.TOTAL_OTHER)
    except requests.RequestException as e:
        metrics.count(Consts.TOTAL_DUPLICATE_REQUESTS_NOT_OK)
        logger.critical(f"Failed to get response from DuplicateService: {e.response.text}")
    except Exception as e:
        metrics.count(Consts.TOTAL_DUPLICATE_REQUESTS_ERROR)
        logger.critical(f"Failed to validate document: {e}")


def process_document(body):
    """
    Process the document by validating it and pushing it to the distribution queue
    """
    logger.info("Processing document")
    validate_document(body)
    logger.info("Pushing document to distribution queue")
    push_to_distribution_queue(body)


def callback(ch, method, properties, body):
    metrics.count(Consts.TOTAL_DOCUMENTS)
    body = json.loads(body)
    process_document(body)


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
            handle_consumer_exception(e)


def handle_consumer_exception(e):
    metrics.count(Consts.TOTAL_FAILED_CONSUME)
    logger.critical(f"Consumer failed: {e}")
    time.sleep(30)
    connection = get_rabbit_connection()
    if not connection:
        logger.error("Failed to reconnect to RabbitMQ. Exiting.")
        metrics.count(Consts.TOTAL_FAILED_FAILED_RABBIT_CONNECTION)
        return


if __name__ == '__main__':
    logger.info("Starting RabbitMQ consumer...")
    main()
