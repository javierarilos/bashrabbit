import logging

def hasHandlers(logger):
    """returns True if logger has handlers.
       py2 and py3 compatible implementation
    """
    if logger.__dict__.get('hasHandlers', None):
        return logger.hasHandlers()
    else:
        return len(logger.handlers) > 0

def get_logger(name=None):
    name = name or __name__
    logger = logging.getLogger(name)
    if hasHandlers(logger):
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s;%(name)s;%(threadName)s;%(levelname)s;%(message)s',
                                  datefmt='%Y-%m-%d;%H:%M:%S')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger
