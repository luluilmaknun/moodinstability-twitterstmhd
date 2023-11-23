#!/bin/bash

dirs=("dataset/ptsd.zip")
source venv/bin/activate

for dir in "${dirs[@]}"; do
  echo "Running for ${dir}..."
  python3 transform.py "${dir}" 9
done
