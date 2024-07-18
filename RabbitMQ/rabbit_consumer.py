import json
import requests
import tldextract
from hashlib import sha256
from utils import logger, Consts, store_article_in_redis
from metrics3 import metrics
from rabbit_utils import get_rabbit_connection


def get_tld_from_url(url):
    ext = tldextract.extract(url)
    return ext.registered_domain or ext.domain


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
    validate_document(body)
    # TODO need to send the document to DSS


def start_consumer(connection):
    channel = connection.channel()
    channel.queue_declare(queue='SyndicationQueue', durable=True)
    channel.basic_consume(queue='SyndicationQueue', on_message_callback=callback, auto_ack=True)
    logger.info("Starting consumer...")
    channel.start_consuming()


def main():
    connection = get_rabbit_connection()
    if not connection:
        return
    while True:
        try:
            start_consumer(connection)
        except Exception as e:
            metrics.count(Consts.TOTAL_FAILED_CONSUME)
            logger.critical(f"Failed to start consumer with the following error: {e}")


if __name__ == '__main__':
    logger.info("Starting RabbitMQ consumer...")
    main()
