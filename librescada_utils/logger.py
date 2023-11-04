import logging

log_format = '%(asctime)s - %(module)s: [%(levelname)s] %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)

# Set the log level for all loggers under the "asyncua" and "asyncio" package prefixes
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

