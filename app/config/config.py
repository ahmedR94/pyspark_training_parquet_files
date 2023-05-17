import os
import json


def set_env_vars():
    for k, v in json.load(open("app/config/config.json")).items():
        os.environ[k] = v
