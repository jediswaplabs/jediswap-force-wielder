import os, json
from dotenv import load_dotenv

def write_to_json(_dict, path) -> None:
    with open(path, 'w') as jfile:
        json_object = json.dump(_dict, jfile, indent=1, default=str)

def read_from_json(json_path) -> dict:
    with open(json_path, 'r') as jfile:
        data = json.load(jfile)
        return data

def write_list_to_json(_list, path) -> None:
    json_str = json.dumps(_list)
    with open(path, 'w') as jfile:
        json.dump(json_str, jfile)

def read_list_from_json(json_path) -> dict:
    """Reads json, returns list with contents of json file."""
    with open(json_path, 'r') as jfile:
        return json.loads(json.loads(jfile.read()))
