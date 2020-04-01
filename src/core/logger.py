import logging
import logging.config
import os

import logging_gelf.handlers
import logging_gelf.formatters


def get_logger():
    """Return logger - Gelf if specified in environment, otherwise standard logging."""
    if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:
        logging.info('Using the Gelf logger to log extraction outputs')
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(host=os.getenv('KBC_LOGGER_ADDR'),
                                                                          port=int(os.getenv('KBC_LOGGER_PORT')))
        logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
        logger.addHandler(logging_gelf_handler)

        # remove default logging to stdout
        logger.removeHandler(logger.handlers[0])

        return logger

    this_dir, _ = os.path.split(__file__)
    path = os.path.join(this_dir, 'logging.conf')
    logging.config.fileConfig(path, disable_existing_loggers=False)

    return logging.getLogger()


def log_debug(msg, *args, **kwargs):
    get_logger().debug(msg, *args, **kwargs)


def log_info(msg, *args, **kwargs):
    get_logger().info(msg, *args, **kwargs)


def log_warning(msg, *args, **kwargs):
    get_logger().warning(msg, *args, **kwargs)


def log_error(msg, *args, **kwargs):
    get_logger().error(msg, *args, **kwargs)


def log_critical(msg, *args, **kwargs):
    get_logger().critical(msg, *args, **kwargs)


def log_fatal(msg, *args, **kwargs):
    get_logger().fatal(msg, *args, **kwargs)


def log_exception(msg, *args, **kwargs):
    get_logger().exception(msg, *args, **kwargs)
