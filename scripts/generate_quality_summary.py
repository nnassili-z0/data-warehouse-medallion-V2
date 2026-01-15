#!/usr/bin/env python3
import os
import json
import csv

artifacts_dir = '../artifacts'
summary = {}

for file in os.listdir(artifacts_dir):
    if file.endswith('.csv'):
        path = os.path.join(artifacts_dir, file)
        with open(path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            count = len(rows) - 1 if rows else 0  # subtract header
        summary[file] = count

with open(os.path.join(artifacts_dir, 'quality_summary.json'), 'w') as f:
    json.dump(summary, f, indent=2)

print("Quality summary generated.")