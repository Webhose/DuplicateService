import pika
import json
import requests
import tldextract
from hashlib import sha256
from utils import logger, Consts, store_article_in_redis


def get_tld_from_url(url):
    ext = tldextract.extract(url)
    return ext.registered_domain or ext.domain


def callback(ch, method, properties, body):
    body = json.loads(body)

    data = {
        "content": body.get('topicRecord').get('topic'),
        "language": body.get('language'),
        "domain": get_tld_from_url(body.get('topicRecord').get('url')),
        "article_id": sha256(body.get('topicRecord').get('url').encode()).hexdigest()
    }

    try:
        response = requests.post(f'http://{Consts.HOST}:9039/is_duplicate', json=data)
        if response.ok:

            if "similarity" in response.text:
                url = body.get('topicRecord').get('url')
                logger.info(f"Article {body.get('topicRecord').get('url')} is similar")
                store_article_in_redis(url)
            else:
                logger.info("document is not syndication and send to DSS")
            # elif "duplicate" in response.text:
            #     redis_connection.sadd("duplicate", article_id)
        else:
            logger.critical(f"Failed to get response from DuplicateService with the following error: {response.text}")
    except Exception as e:
        logger.critical(f"Failed to get data from ES with the following error: {e}")
        return


def get_rabbit_connection():
    try:
        connection = pika.ConnectionParameters(
            host='webhose-data-077',
            credentials=pika.credentials.PlainCredentials(
                'buzzilla', 'buzzilla',
                erase_on_connect=False
            )
        )
        connection = pika.BlockingConnection(connection)
        return connection
    except Exception as e:
        logger.critical(f"Failed to get RabbitMQ connection with the following error: {e}")
        return None


def start_consumer(connection):
    try:
        channel = connection.channel()
        channel.queue_declare(queue='SyndicationQueue', durable=True)
        channel.basic_consume(queue='SyndicationQueue', on_message_callback=callback, auto_ack=True)
        logger.info("Starting consumer...")
        channel.start_consuming()
    except Exception as e:
        logger.critical(f"Failed to start consumer with the following error: {e}")


def main():
    connection = get_rabbit_connection()
    if not connection:
        return
    start_consumer(connection)


if __name__ == '__main__':
    logger.info("Starting RabbitMQ consumer...")
    main()
