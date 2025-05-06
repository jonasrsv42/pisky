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

## Creating a GitHub Release

GitHub Releases is the recommended way to distribute binary wheels without committing them to the repository. You can create releases either manually or using the GitHub CLI.

### Option 1: Using GitHub CLI (Recommended)

The GitHub CLI (`gh`) makes it easier to automate the release process:

1. **Update version number**:

   - Update the version in `Cargo.toml` in the `[package]` section (e.g., to 0.2.0)
   - Make sure all changes are committed

2. **Build the wheels**:

   ```bash
   maturin build --release
   ```

3. **Create the release and upload assets in one command**:

   ```bash
   # Create a new tag and release with the GitHub CLI
   VERSION=0.2.0
   gh release create v$VERSION \
     --title "Pisky v$VERSION" \
     --notes "Release notes for version $VERSION:
     
   - Added corruption recovery support
   - Added static type checking support with mypy
   - <other changes>" \
     --target main 
   
   # Upload only the latest wheel for each Python version
   # This uses a pattern like 'pisky-0.2.0-cp310-cp310-*.whl'
   find target/wheels -name "pisky-$VERSION-*.whl" | sort | uniq -f 2 | xargs -I{} gh release upload v$VERSION {}
   ```

   You can also create the tag separately if preferred:

   ```bash
   git tag -a v$VERSION -m "Release v$VERSION"
   git push origin v$VERSION
   ```

### Option 2: Manual Release Process

If you prefer the manual approach:

1. **Update version number**:

   - Update the version in `Cargo.toml` in the `[package]` section
   - Make sure all changes are committed

2. **Create and push a version tag**:

   ```bash
   # Create a new tag with semantic versioning
   git tag -a v0.2.0 -m "Release v0.2.0"
   
   # Push the tag to GitHub
   git push origin v0.2.0
   ```

3. **Create a GitHub release**:

   - Go to the repository on GitHub (`https://github.com/jonasrsv42/pisky`)
   - Click "Releases" on the right side
   - Click "Draft a new release"
   - Select the tag you just pushed (v0.2.0)
   - Set the release title (e.g., "Pisky v0.2.0")
   - Add release notes describing the changes
   - Upload the wheel files from `target/wheels/` (typically one per Python version)
   - Click "Publish release"

## Installing from GitHub Release

There are two primary methods to install pisky from a private GitHub repository:

### Method 1: Using SSH (Recommended for Private Repositories)

This method uses git+ssh to access the private repository, which relies on your SSH keys for authentication:

#### In a pyproject.toml file:

```toml
[project]
dependencies = [
    "pisky @ git+ssh://git@github.com/jonasrsv42/pisky.git@v0.4.0"
]
```

#### In a requirements.txt file:

```
pisky @ git+ssh://git@github.com/jonasrsv42/pisky.git@v0.4.0
```

#### With pip:

```bash
pip install "pisky @ git+ssh://git@github.com/jonasrsv42/pisky.git@v0.4.0"
```

Note: This method will clone the repository and build the package from source, which requires Rust to be installed on the system.

### Method 2: Direct Wheel Download (For HTTP-Accessible Repositories)

If your repository allows HTTPS access to release assets, you can directly reference the wheel:

#### In a pyproject.toml file:

```toml
[project]
dependencies = [
    "pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.4.0/pisky-0.4.0-cp310-cp310-manylinux_2_17_x86_64.whl"
]
```

#### In a requirements.txt file:

```
pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.4.0/pisky-0.4.0-cp310-cp310-manylinux_2_17_x86_64.whl
```

#### With pip:

```bash
pip install "pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.4.0/pisky-0.4.0-cp310-cp310-manylinux_2_17_x86_64.whl"
```

Make sure to replace:
- The version number in the URL (v0.4.0)
- The wheel filename to match the Python version and platform you need

## Release Checklist

### Using GitHub CLI (Recommended)

1. Update version in Cargo.toml to 0.4.0
2. Build wheels: `maturin build --release`
3. Create a release with GitHub CLI:
   ```bash
   VERSION=0.4.0
   gh release create v$VERSION \
     --title "Pisky v$VERSION" \
     --notes "Release notes for version $VERSION:
     
   - Update release notes here" \
     --target main

   # Upload wheels
   find target/wheels -name "pisky-$VERSION-*.whl" | sort | uniq -f 2 | xargs -I{} gh release upload v$VERSION {}
   ```
4. Verify installation works: `pip install "pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.4.0/[wheel-filename].whl"`

### Manual Process

1. Update version in Cargo.toml to 0.4.0
2. Build wheels: `maturin build --release`
3. Create and push a tag: `git tag -a v0.4.0 -m "Release v0.4.0" && git push origin v0.4.0`
4. Create a GitHub release through the web interface and upload the wheels from `target/wheels/`
5. Verify installation works: `pip install "pisky @ https://github.com/jonasrsv42/pisky/releases/download/v0.4.0/[wheel-filename].whl"`