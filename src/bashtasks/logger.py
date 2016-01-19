import logging


def get_logger(name=None):
    name = name or __name__
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('{asctime};{name};{threadName};{levelname};{message}',
                                  style='{', datefmt='%Y-%m-%d;%H:%M:%S')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger
