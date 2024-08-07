import redis
import time
from consts import Consts
import logging

REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY_SYNDICATE_ON = 'is_syndicate_on'
REDIS_QUEUE_NAME = "syndication"
SLEEP_INTERVAL = 60 * 3  # 3 minutes

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()

# Initialize Redis connection
try:
    r = redis.Redis(host=Consts.REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
except redis.RedisError as e:
    logger.critical(f"Failed to connect to Redis: {e}")
    exit(1)


def get_message_count(queue_name):
    try:
        return r.llen(queue_name)
    except redis.RedisError as e:
        logger.critical(f"Failed to get message count for queue '{queue_name}': {e}")
        return None
    except Exception as e:
        logger.critical(f"Failed to get message count for queue '{queue_name}': {e}")
        return None


def main():
    try:
        # Check number of messages in the Redis queue
        message_count = get_message_count(REDIS_QUEUE_NAME)
        logger.info(f"Monitoring queue '{REDIS_QUEUE_NAME}' current message count: {message_count}")
        if message_count is None:
            logger.error("Failed to retrieve message count.")
            return

        if message_count > Consts.MAX_MESSAGES_IN_QUEUE:
            logger.info(f"Number of messages in queue: {message_count} should be under {Consts.MAX_MESSAGES_IN_QUEUE}."
                        f" Pausing syndication.")
            r.set(REDIS_KEY_SYNDICATE_ON, "False")

        # while message_count > Consts.MAX_MESSAGES_IN_QUEUE:
        #     logger.info(f"Number of messages in queue: {message_count} should be under {Consts.MAX_MESSAGES_IN_QUEUE}."
        #                 f" Pausing syndication.")
        #
        #     message_count = get_message_count(REDIS_QUEUE_NAME)
        #
        #     if message_count is None:
        #         logger.critical("Failed to retrieve initial message count. Exiting.")
        #         return
        #
        #     time.sleep(SLEEP_INTERVAL)
        # r.set(REDIS_KEY_SYNDICATE_ON, "True")
        logger.info(f"Queue '{REDIS_QUEUE_NAME}' is under the limit. Resuming syndication.")

    except Exception as e:
        logger.critical(f"Failed to monitor queue with the following error: {e}")


if __name__ == '__main__':
    logger.info("Starting Redis queue monitor...")
    main()
