"""
Utility functions to assist in data replication processes.
"""
import argparse
import collections
import datetime
import fnmatch
import functools
import json
import os
import time
from typing.io import IO
from warnings import warn

import backoff as backoff_module
import dateutil.parser
import pytz

from .catalog import Catalog

DATETIME_PARSE = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FMT = "%04Y-%m-%d %H:%M:%S.%f"
DATETIME_FMT_SAFE = "%Y-%m-%d %H:%M:%S.%f"


def now(format='dt'):
    if format == 'dt':
        return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    elif format == 'ts_1e6':
        return int(time.time() * 1e6)
    else:
        raise ValueError(f"Invalid format {format}.")


def strptime_with_tz(dtime):
    d_object = dateutil.parser.parse(dtime)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=pytz.UTC)

    return d_object


def strptime(dtime):
    """DEPRECATED Use strptime_to_utc instead.
    Parse DTIME according to DATETIME_PARSE without TZ safety.
    >>> strptime("2018-01-01T00:00:00Z")
    datetime.datetime(2018, 1, 1, 0, 0)
    Requires the Z TZ signifier
    >>> strptime("2018-01-01T00:00:00")
    Traceback (most recent call last):
      ...
    ValueError: time data '2018-01-01T00:00:00' does not match format '%Y-%m-%dT%H:%M:%SZ'
    Can't parse non-UTC DTs
    >>> strptime("2018-01-01T00:00:00-04:00")
    Traceback (most recent call last):
      ...
    ValueError: time data '2018-01-01T00:00:00-04:00' does not match format '%Y-%m-%dT%H:%M:%SZ'
    Does not support fractional seconds
    >>> strptime("2018-01-01T00:00:00.000000Z")
    Traceback (most recent call last):
      ...
    ValueError: time data '2018-01-01T00:00:00.000000Z' does not match format '%Y-%m-%dT%H:%M:%SZ'
    """

    warn("Use strptime_to_utc instead", DeprecationWarning, stacklevel=2)

    return datetime.datetime.strptime(dtime, DATETIME_PARSE)


def strptime_to_utc(dtimestr):
    d_object = dateutil.parser.parse(dtimestr)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=pytz.UTC)
    else:
        return d_object.astimezone(tz=pytz.UTC)


def strftime(dtime, format_str=DATETIME_FMT):
    if dtime.utcoffset() != datetime.timedelta(0):
        raise Exception("datetime must be pegged at UTC tzoneinfo")

    dt_str = None
    try:
        dt_str = dtime.strftime(format_str)
        if dt_str.startswith('4Y'):
            dt_str = dtime.strftime(DATETIME_FMT_SAFE)
    except ValueError:
        dt_str = dtime.strftime(DATETIME_FMT_SAFE)

    return dt_str


def ratelimit(limit, every):
    def limitdecorator(func):
        times = collections.deque()

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if len(times) >= limit:
                tim0 = times.pop()
                tim = time.time()
                sleep_time = every - (tim - tim0)
                if sleep_time > 0:
                    time.sleep(sleep_time)

            times.appendleft(time.time())
            return func(*args, **kwargs)

        return wrapper

    return limitdecorator


def chunk(array, num):
    for i in range(0, len(array), num):
        yield array[i:i + num]


def load_json(path):
    with open(path) as fil:
        return json.load(fil)


def update_state(state, entity, dtime):
    if dtime is None:
        return

    if isinstance(dtime, datetime.datetime):
        dtime = strftime(dtime)

    if entity not in state:
        state[entity] = dtime

    if dtime >= state[entity]:
        state[entity] = dtime


def parse_args(required_config_keys):
    """Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '-p', '--properties',
        help='Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)
    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        setattr(args, 'properties_path', args.properties)
        args.properties = load_json(args.properties)
    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)

    check_config(args.config, required_config_keys)

    return args


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))


def backoff(exceptions, giveup):
    """Decorates a function to retry up to 5 times using an exponential backoff
    function.
    exceptions is a tuple of exception classes that are retried
    giveup is a function that accepts the exception and returns True to retry
    """
    return backoff_module.on_exception(
        backoff_module.expo,
        exceptions,
        max_tries=5,
        giveup=giveup,
        factor=2)


def exception_is_4xx(exception):
    """Returns True if exception is in the 4xx range."""
    if not hasattr(exception, "response"):
        return False

    if exception.response is None:
        return False

    if not hasattr(exception.response, "status_code"):
        return False

    return 400 <= exception.response.status_code < 500


def handle_top_exception(logger):
    """A decorator that will catch exceptions and log the exception's message
    as a CRITICAL log."""

    def decorator(fnc):
        @functools.wraps(fnc)
        def wrapped(*args, **kwargs):
            try:
                return fnc(*args, **kwargs)
            except Exception as exc:
                logger.critical(exc)
                raise

        return wrapped

    return decorator


def find_files(base, pattern):
    """Return list of files matching pattern in base folder."""
    if not os.path.exists(base):
        return None
    else:
        return [n for n in fnmatch.filter(os.listdir(base), pattern) if
                os.path.isfile(os.path.join(base, n))]


def should_sync_field(inclusion, selected, default=False):
    """
    Returns True if a field should be synced.
    inclusion: automatic|available|unsupported
    selected: True|False|None
    default: (default: False) True|False
    "automatic" inclusion always returns True:
    >>> should_sync_field("automatic", None, False)
    True
    >>> should_sync_field("automatic", True, False)
    True
    >>> should_sync_field("automatic", False, False)
    True
    >>> should_sync_field("automatic", None, True)
    True
    >>> should_sync_field("automatic", True, True)
    True
    >>> should_sync_field("automatic", False, True)
    True
    "unsupported" inclusion always returns False
    >>> should_sync_field("unsupported", None, False)
    False
    >>> should_sync_field("unsupported", True, False)
    False
    >>> should_sync_field("unsupported", False, False)
    False
    >>> should_sync_field("unsupported", None, True)
    False
    >>> should_sync_field("unsupported", True, True)
    False
    >>> should_sync_field("unsupported", False, True)
    False
    "available" inclusion uses the selected value when set
    >>> should_sync_field("available", True, False)
    True
    >>> should_sync_field("available", False, False)
    False
    >>> should_sync_field("available", True, True)
    True
    >>> should_sync_field("available", False, True)
    False
    "available" inclusion uses the default value when selected is None
    >>> should_sync_field("available", None, False)
    False
    >>> should_sync_field("available", None, True)
    True
    """
    # always select automatic fields
    if inclusion == "automatic":
        return True

    # never select unsupported fields
    if inclusion == "unsupported":
        return False

    # at this point inclusion == "available"
    # selected could be None, otherwise use the value of selected
    if selected is not None:
        return selected

    # if there was no selected value, use the default
    return default


def _read_next_block_of_lines(file: IO, position: int, block_size: int, is_end: bool = False, encoding='UTF-8'):
    """
    Read the smallest next block of bytes representing characters that ends with new line.
    Retry if incomplete byte sequence is hit and adjust the position.
    Args:
        file:
        position:
        block_size:
        is_end: mark if the block is the last one in the file
        encoding:

    Returns: block:str, position:int - new adjusted position

    """

    new_block_size = block_size
    while True:
        file.seek(position, os.SEEK_END)
        block = file.read(new_block_size)
        try:
            decoded = block.decode(encoding)
            if not decoded.startswith('\n') and not is_end:
                new_block_size += 1
                position -= 1
                continue
            return decoded, position
        except UnicodeDecodeError as e:
            # in case we hit partial character representation (not full byte sequence)
            new_block_size += 1
            position -= 1


def _reversed_blocks(file, blocksize=4096):
    """
    Generate blocks of file's contents in reverse order.
    Args:
        file:
        blocksize:

    Returns:

    """
    file.seek(0, os.SEEK_END)
    max_pos = file.tell()
    position = 0
    delta = blocksize
    while abs(position) < max_pos:
        delta = blocksize if delta <= (max_pos + position) else (max_pos + position)
        position = (-delta) + position
        is_end = max_pos + position == 0

        decoded, position = _read_next_block_of_lines(file, position, delta, is_end)
        yield decoded


def reverse_readline(file, buf_size=8192):
    """
    Generate the lines of file in reverse order.
    Args:
        file:
        buf_size:

    Returns:

    """

    part = ''
    quoting = False
    for block in _reversed_blocks(file, buf_size):

        for c in reversed(block):
            if c == '"':
                quoting = not quoting
            elif c == '\n' and part and not quoting:
                yield part[::-1]
                part = ''
            part += c
    if part:
        yield part[::-1]
