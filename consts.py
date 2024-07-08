class Consts:
    HOST = "tbcrawler21"
    REDIS_HOST = "tbcrawler21"
    REDIS_PORT = 6379
    REDIS_DB = 4
    DUPLICATE = "duplicate"
    SIMILARITY = "similarity"
    MAX_MESSAGES_IN_QUEUE = 100000
    QUEUE_NAME = 'SyndicationQueue'
    DUPLICATE_KEYS = "duplicate_keys"
    UNIQUE = "unique"

    # Metrics
    TOTAL_LSH_OBJECT_CREATED = "total_lsh_object_created"
    TOTAL_SIMILARITY = "total_similarity"
    TOTAL_DUPLICATE = "total_duplicate"
    TOTAL_UNQIUE = "total_unique"
    TOTAL_OTHER = "total_other"
    TOTAL_DOCUMENTS = "total_documents"
    TOTAL_DUPLICATE_REQUESTS_ERROR = "total_duplicate_requests_error"
    TOTAL_DUPLICATE_REQUESTS_NOT_OK = "duplicate_requests_bad_response_total"
    TOTAL_FAILED_CONSUME = "total_failed_consume"
    TOTAL_DUPLICATE_KEYS = "total_duplicate_keys"
