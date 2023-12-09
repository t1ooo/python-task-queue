import logging


logger = logging.getLogger(__name__)


def cls_logger(obj: object) -> logging.Logger:
    return logger.getChild(obj.__class__.__name__)
