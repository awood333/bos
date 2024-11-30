# utilities/logging_setup.py

import logging

class LoggingSetup:
    def __init__(self, name=__name__):
        self.logger = self.setup_logging(name)
        
    def setup_logging(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger