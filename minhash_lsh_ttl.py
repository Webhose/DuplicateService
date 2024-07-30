import metrics3_docker.metrics as metrics
from datasketch import MinHashLSH, MinHash
from datetime import datetime, timedelta
import heapq
import logging
from consts import Consts

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger()

logging.getLogger('redis').setLevel(logging.WARNING)
logging.getLogger('rediscluster').setLevel(logging.WARNING)


class MinHashLSHTTL:
    def __init__(self, threshold: float, num_perm: int, ttl: int = 24):
        """
        Initialize the MinHashLSH with TTL.

        :param threshold: The Jaccard index threshold for similarity.
        :param num_perm: Number of permutations for MinHash.
        :param ttl: Time-to-live for each entry in hours (default is 24 hours).
        """
        self.lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self.ttl = ttl
        self.expiration_heap = []

    def insert(self, key: str, minhash: MinHash):
        # Insert the MinHash into the LSH
        self.lsh.insert(key, minhash)
        # Set the expiration time for the key
        expire_time = datetime.now() + timedelta(hours=self.ttl)
        heapq.heappush(self.expiration_heap, (expire_time, key))

    def query(self, minhash: MinHash):
        # Clean up expired keys before querying
        self.cleanup_expired_keys()
        # Query
        return self.lsh.query(minhash)

    def remove(self, key: str):
        self.lsh.remove(key)

    def cleanup_expired_keys(self):
        """
        Clean up expired keys from the LSH.
        """
        metrics.count(Consts.GET_EXPIRED_KEYS_TOTAL)
        now = datetime.now()
        while self.expiration_heap and self.expiration_heap[0][0] < now:
            try:
                expire_time, key = heapq.heappop(self.expiration_heap)
                metrics.count(Consts.MINHASH_LSH_TTL_EXPIRED_KEYS_TOTAL)
                self.remove(key)
            except Exception as e:
                logger.error(f"Error cleaning up expired keys: {e}")
                break
