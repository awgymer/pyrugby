import logging
import datetime
from urllib.parse import urlunsplit, urlencode

from .constants import CMSAPI_ROOT, CMSAPI_SCHEME, CMSAPI_PATHS

log = logging.getLogger(__name__)


def cms_timestamp_to_datetime(timestamp, gmt_offset=0):
    return datetime.datetime.utcfromtimestamp(
        timestamp + gmt_offset*60*60
    )


def get_api_url(endpoint, path_args, query_args=None):
    return urlunsplit((
        CMSAPI_SCHEME,
        CMSAPI_ROOT,
        CMSAPI_PATHS[endpoint].format(**path_args),
        urlencode(query_args or {}),
        ''
    ))
