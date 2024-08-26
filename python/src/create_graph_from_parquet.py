import pandas as pd
import igraph as ig
import numpy as np
import ast

def create_graph_from_parquet(filepath):
    df = pd.read_parquet(filepath)
    total_rows = len(df)
    edges = []

    for i, row in enumerate(df.iterrows()):
        _, row = row
        source = row['Page Title']
        targets = row['Page References']

        if targets is None:
            targets = []
        elif isinstance(targets, np.ndarray):
            targets = targets.tolist()
        elif isinstance(targets, str):
            try:
                targets = ast.literal_eval(targets)
            except (SyntaxError, ValueError):
                print(f"Warning: Failed to convert string to list for Page References: {targets}")
                targets = []
        elif not isinstance(targets, list):
            print(f"Warning: Expected list for Page References, got {type(targets)} instead.")
            targets = []

        for target in targets:
            if target:
                edges.append((source, target))

        percent_complete = (i / total_rows) * 100
        print(f"Processing: {percent_complete:.2f}% complete.")    
    return ig.Graph.TupleList(edges, directed=True)