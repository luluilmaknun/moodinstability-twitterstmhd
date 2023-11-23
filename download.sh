#!/bin/bash

urls=(
  "https://zenodo.org/record/5854911/files/adhd.zip"
  "https://zenodo.org/record/5854911/files/anxiety.zip"
  "https://zenodo.org/record/5854911/files/bipolar.zip"
  "https://zenodo.org/record/5854911/files/depression.zip"
  "https://zenodo.org/record/5854911/files/mdd.zip"
  "https://zenodo.org/record/5854911/files/neg.zip"
  "https://zenodo.org/record/5854911/files/ocd.zip"
  "https://zenodo.org/record/5854911/files/ppd.zip"
  "https://zenodo.org/record/5854911/files/ptsd.zip"
)

for url in "${urls[@]}"; do
  filename=$(basename "${url}")
  echo "Downloading ${filename}..."
  curl -OJL "${url}"
done
