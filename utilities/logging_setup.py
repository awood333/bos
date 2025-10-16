# utilities/logging_setup.py

import logging
import functools

def setup_debug_logging(level=logging.WARNING):
    """Set up logging with configurable level"""
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    return logging.getLogger(__name__)

def debug_method(func):
    """Decorator to add debug logging to methods"""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        logger = logging.getLogger(self.__class__.__name__)
        logger.debug(f"Starting {func.__name__}")
        try:
            result = func(self, *args, **kwargs)
            logger.debug(f"Completed {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            raise
    return wrapper

def safe_dependency(dep_name, fallback=None):
    """Safely get dependency with fallback"""
    from container import get_dependency
    try:
        return get_dependency(dep_name)
    except Exception as e:
        logger = logging.getLogger("safe_dependency")
        logger.warning(f"Failed to get {dep_name}: {e}")
        if fallback:
            return fallback()
        raise