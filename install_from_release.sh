#!/bin/bash
# Script to install pisky from GitHub release and verify it works

set -e  # Exit on any error

# Variables
RELEASE_VERSION="v0.4.0"

echo "🚀 Creating a new virtual environment for testing..."
python3 -m venv test_venv
source test_venv/bin/activate

echo "📦 Installing pisky from GitHub release using SSH..."
# Install via git+ssh using SSH protocol
pip install "pisky @ git+ssh://git@github.com/jonasrsv42/pisky.git@${RELEASE_VERSION}"

echo "🧪 Running test script..."
python3 test_install.py

echo "🧹 Cleaning up..."
deactivate
rm -rf test_venv

echo "✨ Done! If all tests passed, your release is working correctly."
