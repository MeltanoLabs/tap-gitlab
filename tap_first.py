import singer
import requests
import urllib.request
from datetime import datetime, timezone
from singer import utils

now = datetime.now(timezone.utc).isoformat()
schema = {
  'properties': {
    'id': {
      'type': 'integer',
    },
    'description': {
      'type': ['string', 'null'],
    },
    'ip_address': {
      'type': ['string', 'null'],
    },
    'active': {
      'type': 'boolean',
    },
    'is_shared': {
      'type': 'boolean',
    },
    'name': {
      'type': 'string',
    },
    'online': {
      'type': 'boolean',
    },
    'status': {
      'type': 'string',
    },
  }
}

REQUIRED_CONFIG_KEYS = [
  'api_key',
]


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    api_key = args.config.get('api_key')
    gitlab_runners_url = f'https://gitlab.com/api/v4/runners/?private_token={api_key}'
    resp = requests.get(gitlab_runners_url)
    status_code = resp.status_code
    resp = resp.json()
    if status_code != 200:
        raise requests.HTTPError(resp)

    singer.write_schema('runners', schema, 'id')
    singer.write_records('runners', resp)

