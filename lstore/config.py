# Metadata column indices
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# Storage configuration
PAGE_SIZE = 4096  # Size of each page in bytes
RECORD_SIZE = 8   # Size of each record in bytes (64-bit integers)
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE  # Number of records per page
BASE_PAGES_PER_RANGE = 16  # Number of base pages per range 