import logging
import sys

def get_logger(name="statarb"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)
    return logger
