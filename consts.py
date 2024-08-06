class Consts:
    HOST = "tanya-032"
    REDIS_HOST = "tanya-032"
    REDIS_PORT = 6379
    REDIS_DB = 4
    DUPLICATE = "duplicate"
    SIMILARITY = "similarity"
    MAX_MESSAGES_IN_QUEUE = 100000
    QUEUE_NAME = 'SyndicationQueue'
    DUPLICATE_KEYS = "duplicate_keys"
    UNIQUE = "unique"
    MAX_HOURS_FOR_RECOVERY = 4

    # Metrics
    TOTAL_LSH_OBJECT_CREATED = "total_lsh_object_created"
    TOTAL_SIMILARITY = "total_similarity"
    TOTAL_DUPLICATE = "total_duplicate"
    TOTAL_UNIQUE = "total_unique"
    TOTAL_OTHER = "total_other"
    TOTAL_DOCUMENTS = "total_documents"
    TOTAL_DUPLICATE_REQUESTS_ERROR = "total_duplicate_requests_error"
    TOTAL_DUPLICATE_REQUESTS_NOT_OK = "duplicate_requests_bad_response_total"
    TOTAL_FAILED_CONSUME = "total_failed_consume"
    TOTAL_DUPLICATE_KEYS = "total_duplicate_keys"
    TOTAL_FAILED_REDIS_CONNECTION = "total_failed_redis_connection"
    TOTAL_DOCUMENTS_FAILED_DISTRIBUTION = "total_documents_failed_distribution"
    TOTAL_FAILED_FAILED_RABBIT_CONNECTION = "total_failed_failed_rabbit_connection"
    TOTAL_DOCUMENTS_DISTRIBUTION = "total_documents_distribution"
    TOTAL_FAILED_PROCESS_DOCUMENT = "total_failed_process_document"
    MINHASH_LSH_TTL_EXPIRED_KEYS_TOTAL = "minhash_lsh_ttl_expired_keys_total"
    GET_EXPIRED_KEYS_TOTAL = "get_expired_keys_total"
    BACKGROUND_CLEANUP_TASK_TOTAL = "background_cleanup_task_total"
