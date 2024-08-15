# v0.3 08/13/24 Skye Goetz (ISB)

import os
import sys
import glob
import subprocess
import pandas as pd
from tqdm import tqdm

cwd = os.getcwd()
try:
    dir_path = sys.argv[1]
except IndexError:
    print(f'dir2assembly : Must Input a Dictionary to Field [1]')
    sys.exit(1)
KG_name = 'Test' # NAME YOUR FINAL KG HERE
edges_path = os.path.join(cwd, f'{KG_name}_edges.tsv')
nodes_path = os.path.join(cwd, f'{KG_name}_nodes.tsv')

def values(value):
    try:
        value = float(value)
        return format(value, '.2e') if value >= 1e5 else value
    except (ValueError, TypeError):
        return value

def sci_notation(df, column_name):
    if column_name in df.columns: 
        df[column_name] = df[column_name].apply(lambda x: values(x))
    return df

def cutoff(df):
    try:
        df = df[df['p'].astype(float) <= 0.05]
    except (ValueError, TypeError):
        pass
    return df

def dir2assembly(config_path):
    result_file_name = os.path.splitext(os.path.basename(config_path))[0] + '.tsv'
    results_path = os.path.join(cwd, 'results', result_file_name)
    subprocess.run(['python3', os.path.join(cwd, 'bin', 'config2KG.py'), config_path], check=True)
    df = pd.read_csv(results_path, sep='\t')
    df = df.pipe(sci_notation, 'relationship_strength') \
        .pipe(sci_notation, 'p') \
        .pipe(cutoff) \
        .dropna() \
        .drop_duplicates(subset=['subject', 'predicate', 'object', 'publication', 'relationship_type'])
    edges = df[['subject', 'predicate', 'object', 'subject_name', 'object_name', 'n', 'relationship_strength', 'p', 'relationship_type', 
        'p_correction_method', 'knowledge_level', 'agent_type', 'publication', 'publication_name', 'authors', 'url', 'sheet_name', 'curator_name']] \
        .copy()
    if not os.path.isfile(edges_path):
        edges.to_csv(edges_path, sep='\t', index=False)
    else:
        existing_edges = pd.read_csv(edges_path, sep='\t')
        edges = pd.concat([existing_edges, edges], ignore_index=True)
        edges.to_csv(edges_path, sep='\t', index=False)
    nodes = pd.concat([
        df[['subject', 'subject_name', 'subject_category']].copy().rename(columns={'subject': 'id', 'subject_name': 'name', 'subject_category': 'category'}),
        df[['object', 'object_name', 'object_category']].copy().rename(columns={'object': 'id', 'object_name': 'name', 'object_category': 'category'})
    ], ignore_index=True)
    nodes = nodes[['id', 'name', 'category']].drop_duplicates()
    if not os.path.isfile(nodes_path):
        nodes.to_csv(nodes_path, sep='\t', index=False)
    else:
        existing_nodes = pd.read_csv(nodes_path, sep='\t')
        nodes = pd.concat([existing_nodes, nodes], ignore_index=True).drop_duplicates()
        nodes.to_csv(nodes_path, sep='\t', index=False)

for file in [edges_path, nodes_path]:
    if os.path.isfile(file):
        os.remove(file)
config_files = glob.glob(os.path.join(dir_path, '*.yaml'))
for config_path in tqdm(config_files, desc=f'dir2assembly : Processing Config Files '):
    dir2assembly(config_path)