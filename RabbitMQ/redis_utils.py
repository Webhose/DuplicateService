from rediscluster import RedisCluster, ClusterConnectionPool
from redis import Redis, ConnectionPool
from utils import logger, Consts
import json
from metrics3 import metrics


class RedisConnectionManager:
    """
    Manages Redis and Redis Cluster connections with optimized pooling and error handling.
    """

    def __init__(self):
        self.pools = {}

    def connect_to_redis(self, connect_details):
        """
        Retrieves Redis connection based on its configuration (Redis Connection or RedisCluster).
        :return: Redis or RedisCluster connection.
        """
        try:
            # Parse host details
            _hosts_list = connect_details if not isinstance(connect_details, str) else json.loads(connect_details)
            if not _hosts_list:
                raise ValueError("No hosts specified")

            # Decide on Redis or Redis Cluster based on the number of hosts
            if len(_hosts_list) > 1:
                return self.connect_to_redis_cluster(_hosts_list)
            else:
                return self.connect_to_single_redis_instance(_hosts_list[0])
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON decoding failed: {json_err}")
        except Exception as err:
            metrics.count(Consts.TOTAL_FAILED_REDIS_CONNECTION)
            logger.error(f"Failed to create Redis connection: {err}")
            return None

    def connect_to_redis_cluster(self, hosts_list):
        """
        Establishes a connection to a Redis Cluster using a connection pool.
        """
        try:
            if "cluster" not in self.pools:
                pool = ClusterConnectionPool(startup_nodes=hosts_list, max_connections=200, health_check_interval=10)
                self.pools["cluster"] = pool
            else:
                pool = self.pools["cluster"]

            redis_connection = RedisCluster(connection_pool=pool)
            logger.info(f"Successful connection pool established to Redis Cluster: {hosts_list}")
            return redis_connection
        except Exception as err:
            # metrics.count(Consts.TOTAL_FAILED_REDIS_CLUSTER_CONNECTION)
            logger.error(f"Failed to connect to Redis Cluster: {err}")
            raise

    def connect_to_single_redis_instance(self, host_details):
        """
        Establishes a connection to a single Redis instance using a connection pool.
        """
        try:
            pool_key = f"{host_details['host']}:{host_details['port']}"
            if pool_key not in self.pools:
                pool = ConnectionPool(
                    host=host_details['host'],
                    port=host_details['port'],
                    db=host_details.get('db', 0),
                    max_connections=50,
                    health_check_interval=10
                )
                self.pools[pool_key] = pool
            else:
                pool = self.pools[pool_key]

            redis_connection = Redis(connection_pool=pool)
            logger.info(f"Connected to single Redis instance at {host_details['host']}:{host_details['port']}")
            return redis_connection
        except Exception as err:
            # metrics.count(Consts.TOTAL_FAILED_SINGLE_REDIS_CONNECTION)
            logger.error(f"Failed to connect to single Redis instance: {err}")
            raise

    def get_redis_connection(self, site_type):
        """
        Retrieves a Redis connection for a specified site type from the configuration.
        """
        redis_config = {
            # "mainstream": [{"host": "localhost", "port": 6379, "db": 3}],
            "mainstream": [
                {"host": "redis-news-002", "port": "6379"},
                {"host": "redis-news-004", "port": "6379"},
                {"host": "redis-news-005", "port": "6379"}
            ]
        }
        return self.connect_to_redis(redis_config.get(site_type))
