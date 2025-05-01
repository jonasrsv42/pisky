# Building and Releasing Pisky

This guide explains how to build Python wheels and manually publish a release to the private GitHub repository.

## Prerequisites

- GitHub account with write access to `git@github.com:jonasrsv42/pisky.git`
- Python 3.8+ with pip and venv
- Rust toolchain with cargo installed
- [Maturin](https://github.com/PyO3/maturin) for building Python wheels

## Building Wheels

Pisky uses Maturin to build wheels for different Python versions and platforms.

### Building Release Wheels

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install maturin
pip install maturin

# Build optimized wheels
cd pisky
maturin build --release

# Wheels will be available in target/wheels/
```

You can build wheels for specific Python versions if you have them installed:

```bash
# For a specific Python version
maturin build --release --interpreter python3.10

# For multiple Python versions
maturin build --release --interpreter python3.8 python3.9 python3.10
```

## Creating a GitHub Release Manually

GitHub Releases is the recommended way to distribute binary wheels without committing them to the repository.

1. **Update version number**:

   - Update the version in `Cargo.toml` in the `[package]` section
   - Make sure all changes are committed

2. **Create and push a version tag**:

   ```bash
   # Create a new tag with semantic versioning
   git tag -a v0.1.0 -m "Release v0.1.0"
   
   # Push the tag to GitHub
   git push origin v0.1.0
   ```

3. **Create a GitHub release**:

   - Go to the repository on GitHub (`https://github.com/jonasrsv42/pisky`)
   - Click "Releases" on the right side
   - Click "Draft a new release"
   - Select the tag you just pushed (v0.1.0)
   - Set the release title (e.g., "Pisky v0.1.0")
   - Add release notes describing the changes
   - Upload the wheel files from `target/wheels/`
   - Click "Publish release"

## Installing from GitHub Release

To use the package from a GitHub release in another project, you can reference it in your `pyproject.toml` or `requirements.txt`:

### In a pyproject.toml file:

```toml
[project]
dependencies = [
    "pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.1.0/pisky-0.1.0-cp310-cp310-manylinux_2_17_x86_64.whl"
]
```

### In a requirements.txt file:

```
pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.1.0/pisky-0.1.0-cp310-cp310-manylinux_2_17_x86_64.whl
```

### With pip:

```bash
pip install "pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.1.0/pisky-0.1.0-cp310-cp310-manylinux_2_17_x86_64.whl"
```

Make sure to replace:
- The version number in the URL (v0.1.0)
- The wheel filename to match the Python version and platform you need

## Release Checklist

1. Update version in Cargo.toml
2. Build wheels: `maturin build --release`
3. Create and push a tag: `git tag -a v0.1.0 -m "Release v0.1.0" && git push origin v0.1.0`
4. Create a GitHub release and upload the wheels from `target/wheels/`
5. Verify installation works: `pip install "pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.1.0/[wheel-filename].whl"`