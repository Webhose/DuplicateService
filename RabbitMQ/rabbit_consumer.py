import json
import time
import requests
import tldextract
from hashlib import sha256
from utils import logger, Consts, store_article_in_redis
from metrics3 import metrics
from rabbit_utils import get_rabbit_connection
from redis_utils import RedisConnectionManager

# Instantiate RedisConnectionManager globally to manage connections
redis_manager = RedisConnectionManager()


def get_tld_from_url(url):
    ext = tldextract.extract(url)
    return ext.registered_domain or ext.domain


def push_to_distribution_queue(document, method="NBDR", queue_name="distribution"):
    try:
        message = f"{method} {json.dumps(document, default=lambda obj: getattr(obj, '__dict__', str(obj)))}"
        redis_connection = redis_manager.get_redis_connection(document.get('index'))

        if message and redis_connection:
            # Push the message to the Redis queue
            redis_connection.lpush(queue_name, message)
            metrics.count(Consts.TOTAL_DOCUMENTS_DISTRIBUTION)
        else:
            logger.error("Failed to push document to distribution queue")
            metrics.count(Consts.TOTAL_DOCUMENTS_FAILED_DISTRIBUTION)
    except Exception as e:
        logger.critical(f"Failed to push document to distribution queue: {e}")


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
    try:
        metrics.count(Consts.TOTAL_DOCUMENTS)
        body = json.loads(body)
        process_document(body)
        # Acknowledge message after processing
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        metrics.count(Consts.TOTAL_FAILED_PROCESS_DOCUMENT)
        logger.error(f"Failed to process document: {e}")
        # Negative acknowledgment
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consumer(connection):
    logger.info("Starting consumer...")
    channel = connection.channel()
    channel.queue_declare(queue='SyndicationQueue', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='SyndicationQueue', on_message_callback=callback)
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
            connection = handle_consumer_exception(e)


def handle_consumer_exception(e):
    metrics.count(Consts.TOTAL_FAILED_CONSUME)
    logger.critical(f"Consumer failed: {e}")

    while True:
        time.sleep(30)
        connection = get_rabbit_connection()
        if connection:
            logger.info("Successfully reconnected to RabbitMQ.")
            return connection
        else:
            logger.error("Failed to reconnect to RabbitMQ. Retrying...")
            metrics.count(Consts.TOTAL_FAILED_FAILED_RABBIT_CONNECTION)


if __name__ == '__main__':
    logger.info("Starting RabbitMQ consumer...")
    main()
