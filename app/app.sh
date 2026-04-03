#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Install build dependencies
apt-get update && apt-get install -y python3-dev build-essential

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# Package the virtual env.
venv-pack -o .venv.tar.gz --force

# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh

# Run the ranker
bash search.sh "this is a query!"

echo "Pipeline complete. Container is ready for interactive queries."
echo "Usage: bash search.sh '<your query>'"

# Keep container alive for interactive queries
tail -f /dev/null
