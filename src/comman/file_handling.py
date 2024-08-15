import json
from traceback import print_tb


def write_json(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f)
        return file_path

def read_json(file_path):
    with open(file_path) as f:
        return json.load(f)



