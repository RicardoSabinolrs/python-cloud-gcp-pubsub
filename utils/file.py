import json
from os import listdir
from os.path import join


def find_all_json_files_in_local_disk(local_folder='data'):
    return [join(local_folder, file) for file in listdir(local_folder)]


def read_file_content(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        return json.loads(content)
