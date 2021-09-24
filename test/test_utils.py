import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../test"))

import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.constants import *


def load_client_info_from_file(from_path: str):
    if from_path is None:
        raise Exception("Unable to load Client Connection information from None file")
    _creds = None
    with open(from_path, 'r') as w:
        _creds = json.load(w)
        w.close()

    _clients = {}
    _account_ids = {}
    _credentials_dict = {}
    _region = _creds.get('AWS_REGION')

    for token in [MESH, PRODUCER, CONSUMER, PRODUCER_ADMIN, CONSUMER_ADMIN]:
        _clients[token] = utils.generate_client('sts', region=_region, credentials=_creds.get(token))
        _account_ids[token] = _creds.get(token).get('AccountId')
        _credentials_dict = _creds

    return _region, _clients, _account_ids, _credentials_dict
