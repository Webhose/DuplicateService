import redis
from rabbit_utils import get_rabbit_connection
import time
from consts import Consts
import logging

REDIS_HOST = 'tbcrawler21'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY_SYNDICATE_ON = 'is_syndicate_on'
SLEEP_INTERVAL = 60 * 3  # 3 minutes

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()

# Initialize Redis connection
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
except redis.RedisError as e:
    logger.critical(f"Failed to connect to Redis: {e}")
    exit(1)


def get_message_count(channel, queue_name):
    try:
        queue = channel.queue_declare(queue=queue_name, durable=True, passive=True)
        return queue.method.message_count
    except Exception as e:
        logger.critical(f"Failed to get message count for queue '{queue_name}': {e}")
        return None


def main():
    connection = get_rabbit_connection()
    if not connection:
        logger.error("Failed to get RabbitMQ connection. Exiting.")
        return

    try:
        # check number of messages in queue
        channel = connection.channel()
        message_count = get_message_count(channel, Consts.QUEUE_NAME)
        logger.info(f"Initial message count: {message_count}")
        if message_count is None:
            logger.critical("Failed to retrieve initial message count. Exiting.")
            return

        if message_count > Consts.MAX_MESSAGES_IN_QUEUE:
            logger.info(f"Number of messages in queue: {message_count} should be under {Consts.MAX_MESSAGES_IN_QUEUE}."
                        f" Pausing syndication.")
            r.set(REDIS_KEY_SYNDICATE_ON, "False")

        # while message_count > Consts.MAX_MESSAGES_IN_QUEUE:
        #     logger.info(f"Number of messages in queue: {message_count} should be under {Consts.MAX_MESSAGES_IN_QUEUE}."
        #                 f" Pausing syndication.")
        #
        #     message_count = get_message_count(channel, Consts.QUEUE_NAME)
        #
        #     if message_count is None:
        #         logger.critical("Failed to retrieve initial message count. Exiting.")
        #         return
        #
        #     time.sleep(SLEEP_INTERVAL)
        # r.set(REDIS_KEY_SYNDICATE_ON, "True")
        # logger.info(f"Queue '{Consts.QUEUE_NAME}' is under the limit. Resuming syndication.")

    except Exception as e:
        logger.critical(f"Failed to monitor queue with the following error: {e}")

    finally:
        if connection and connection.is_open:
            connection.close()
            logger.info("RabbitMQ connection closed.")


if __name__ == '__main__':
    logger.info("Starting RabbitMQ monitor...")
    main()
