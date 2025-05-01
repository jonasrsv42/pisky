#!/bin/bash

# Activate the virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Set the environment variable for Cargo to use the git CLI
export CARGO_NET_GIT_FETCH_WITH_CLI=true

# Run maturin with the desired command
if [ "$1" == "release" ]; then
    echo "Building release version..."
    maturin build --release
else
    echo "Building development version..."
    maturin develop
fi

# Print a success message
echo "Build completed successfully!"