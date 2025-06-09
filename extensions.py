import os
from flask_caching import Cache

# FileSystem Cache (persists between app restarts)
cache_config = {
    'CACHE_TYPE': 'FileSystemCache',
    'CACHE_DIR': os.path.join(os.getcwd(), 'cache'),  # Cache directory
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_THRESHOLD': 500  # Maximum number of items in cache
}

# Initialize cache object
cache = Cache(config=cache_config)