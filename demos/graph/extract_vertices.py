#!/usr/bin/env python3
"""
Extract unique vertex IDs from edges.csv for each graph demo.
Writes a bare vertex_id list to vertices_ids.csv (one column, pipe-delimited with header).
The actual name/category columns are populated separately with context-aware names.
"""
import csv
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DEMOS = [
    "graph-dolphins",
    "graph-karate-club",
    "graph-polbooks",
    "graph-netscience",
    "graph-email-eu-core",
]

for demo in DEMOS:
    edges_path = os.path.join(SCRIPT_DIR, demo, "data", "edges.csv")
    out_path = os.path.join(SCRIPT_DIR, demo, "data", "vertex_ids.csv")

    nodes = set()
    with open(edges_path, "r") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            nodes.add(int(row["src"]))
            nodes.add(int(row["dst"]))

    sorted_nodes = sorted(nodes)
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="|")
        writer.writerow(["vertex_id"])
        for nid in sorted_nodes:
            writer.writerow([nid])

    print(f"{demo}: {len(sorted_nodes)} unique vertices extracted → {out_path}")
