import pika
import logging

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()

logging.getLogger('pika').setLevel(logging.WARNING)


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
