import json

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

def merge_unique(list_of_lists, unique_att="id"):
    """Merge multiple lists of dicts. Keep only one element with {unique_att}"""
    unique_tracker = set()
    out_l = []

    for l in list_of_lists:
        for ele in l:
            if ele[unique_att] not in unique_tracker:
                out_l.append(ele)
                unique_tracker.add(ele[unique_att])
    return out_l

def obvious_print(msg) -> None:
    out_str = '\n' + '='*75 + '\n\t' + msg + '\n' + '='*75 + '\n'
    print(out_str)
