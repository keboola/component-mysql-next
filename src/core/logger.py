import logging
import logging.config
import os

import logging_gelf.handlers
import logging_gelf.formatters


def get_logger():
    """Return logger - Gelf if specified in environment, otherwise standard logging."""
    if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:
        return _set_gelf_logger()

    this_dir, _ = os.path.split(__file__)
    path = os.path.join(this_dir, 'logging.conf')
    logging.config.fileConfig(path, disable_existing_loggers=False)

    return logging.getLogger()


def _set_gelf_logger(log_level=logging.INFO, transport_layer='TCP'):  # noqa: E301
    """
    Sets gelf console logger. Handler for console output is not included by default,
    for testing in non-gelf environments use stdout=True.
    Args:
        log_level: logging level, default: 'INFO'
        transport_layer: 'TCP' or 'UDP', default:'UDP
    Returns: logging object
    """
    # remove existing handlers
    for h in logging.getLogger().handlers:
        logging.getLogger().removeHandler(h)
    # gelf handler setup
    host = os.getenv('KBC_LOGGER_ADDR', 'localhost')
    port = os.getenv('KBC_LOGGER_PORT', 12201)
    if transport_layer == 'TCP':
        gelf = logging_gelf.handlers.GELFTCPSocketHandler(host=host, port=port)
    elif transport_layer == 'UDP':
        gelf = logging_gelf.handlers.GELFUDPSocketHandler(host=host, port=port)
    else:
        raise ValueError(F'Unsupported gelf transport layer: {transport_layer}. Choose TCP or UDP')
    logging.getLogger().setLevel(log_level)
    logging.getLogger().addHandler(gelf)
    logger = logging.getLogger()
    return logger


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
