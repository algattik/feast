#!/usr/bin/env bash

set -ex

# Clone Feast repository into Jupyter container
git clone -b ${FEAST_REPOSITORY_VERSION} --single-branch https://github.com/feast-dev/feast.git || true

# Compile Feast Protobuf (only needed for running tests)
make -C feast compile-protos-python

# Install Feast SDK
pip install -e feast/sdk/python -U

# Start Jupyter Notebook
start-notebook.sh --NotebookApp.token=''