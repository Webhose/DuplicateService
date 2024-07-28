from metrics3 import metrics
from datasketch import MinHashLSH, MinHash
from datetime import datetime, timedelta
import logging
from consts import Consts

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.INFO)

logger = logging.getLogger()


class MinHashLSHTTL:
    def __init__(self, threshold: float, num_perm: int, ttl: int = 86400):
        """
        Initialize the MinHashLSH with TTL.

        :param threshold: The Jaccard index threshold for similarity.
        :param num_perm: Number of permutations for MinHash.
        :param ttl: Time-to-live for each entry in seconds (default is 24 hours).
        """
        self.lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self.ttl = ttl
        self.expiration_times = {}

    def insert(self, key: str, minhash: MinHash):
        # Insert the MinHash into the LSH
        self.lsh.insert(key, minhash)
        # Set the expiration time for the key
        self.expiration_times[key] = datetime.now() + timedelta(seconds=self.ttl)

    def query(self, minhash: MinHash):
        # self.cleanup_expired_keys()
        return self.lsh.query(minhash)

    def remove(self, key: str):
        """
        Remove a MinHash from the LSH and expiration dictionary.
        """
        self.lsh.remove(key)
        if key in self.expiration_times:
            del self.expiration_times[key]

    def cleanup_expired_keys(self):
        # TODO consider to optimize the logic
        expired_keys = self.get_expired_keys()
        for key in expired_keys:
            metrics.count(Consts.MINHASH_LSH_TTL_EXPIRED_KEYS_TOTAL)
            self.remove(key)
        logger.info(f"Expired keys removed: {expired_keys}")

    def get_expired_keys(self):
        logger.info("Getting expired keys")
        metrics.count(Consts.GET_EXPIRED_KEYS_TOTAL)
        now = datetime.now()
        return [key for key, expire_time in self.expiration_times.items() if expire_time < now]
