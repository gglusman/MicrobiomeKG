# v0.4 08/13/24 Skye Goetz (ISB)

import os
import re
import gzip
import glob
import dill
import pandas as pd

pd.set_option('display.max_columns', None)

cwd = os.getcwd()
BABEL_directory_list = [
    # [ LIST OF PATHS TO DIRECTORIES CONTAINING MAPPING DATA ]
]
shopping_list = set([
    # [ LIST OF FILES IN THOSE TO DIRECTORIES YOU WISH TO USE ]
    # [ NAMEING DATA MUST END IN '.names.gz' ]
    # [ MAPPING DATA MUST END IN '.map.gz' ]
])

category_regex = re.compile(r'\.names\.gz|\.trim|\.suppl') 
    # CHANGE THIS TO FIT YOUR DESIRED CATEGORY NAMING STRUCTURE (FILENAMES DERIVE CATEGORY MAPPING)

def load_files(dir_list, pattern):
    files = []
    for dir in dir_list:
        files.extend(glob.glob(os.path.join(dir, pattern)))
    return files

def process_name_file(field):
    name_resolver = {}
    term_mapper = {}
    semantic_types = {}
    with gzip.open(field, 'rt') as names:
        base_name = os.path.basename(names.name)
        if base_name in shopping_list:
            df = pd.read_csv(names, sep='\t')
            df.insert(0, 'category', base_name)
            df['category'] = df['category'].str.replace(category_regex, '', regex=True)
            df['category'] = 'biolink:' + df['category']
            df['synonyms'] = df['synonyms'].str.split('|')
            df = df.explode('synonyms').reset_index(drop=True)
            df = df.apply(lambda row: row.str.strip())
            df = df[~df.apply(lambda row: row.astype(str).eq('nan')).any(axis=1)]
            name_resolver.update(dict(zip(df['id'], df['name'])))
            term_mapper.update(dict(zip(df['name'].str.lower(), df['id'])))
            semantic_types.update(dict(zip(df['id'], df['category'])))
            term_mapper.update(dict(zip(df['synonyms'].str.lower(), df['id'])))
    return name_resolver, term_mapper, semantic_types

def process_map_file(field):
    node_normalizer = {}
    with gzip.open(field, 'rt') as maps:
        base_name = os.path.basename(maps.name)
        if base_name in shopping_list:
            df = pd.read_csv(maps, sep='\t')
            node_normalizer.update(dict(zip(df['alias'], df['preferred'])))
    return node_normalizer

def BABEL2pickle():
    name_list = load_files(BABEL_directory_list, '*.names.gz')
    map_list = load_files(BABEL_directory_list, '*.map.gz')
    name_resolver = {}
    term_mapper = {}
    semantic_types = {}
    node_normalizer = {}
    for name_file in name_list:
        name_res, term_map, sem_types = process_name_file(name_file)
        name_resolver.update(name_res)
        term_mapper.update(term_map)
        semantic_types.update(sem_types)
    for map_file in map_list:
        norm = process_map_file(map_file)
        node_normalizer.update(norm)
    pickle_path = os.path.join(cwd, 'pickle')
    os.makedirs(pickle_path, exist_ok=True)
    for file_name, data in zip(['name_resolver', 'term_mapper', 'semantic_types', 'node_normalizer'],
                               [name_resolver, term_mapper, semantic_types, node_normalizer]):
        file_path = os.path.join(pickle_path, f"{file_name}.pkl.gz")
        with gzip.open(file_path, 'wb') as file:
            dill.dump(data, file)

if __name__ == '__main__':
    BABEL2pickle()