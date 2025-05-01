# Pisky Consumer Example

This is an example project that demonstrates how to consume the pisky package from a private GitHub repository using SSH authentication.

## Setup

1. Ensure you have SSH access to the private repository (`git@github.com:jonasrsv42/pisky.git`)

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install the project and its dependencies:
   ```bash
   pip install -e .
   ```
   
   This will install pisky directly from the GitHub repository using the SSH URL specified in pyproject.toml.

## Running the Example

```bash
python example.py
```

This will demonstrate both the single-threaded and multi-threaded APIs of pisky.

## Understanding pyproject.toml

The key part of the configuration is in the `pyproject.toml` file:

```toml
dependencies = [
    "pisky @ git+ssh://git@github.com/jonasrsv42/pisky.git@v0.1.0"
]
```

This tells pip to install pisky directly from the GitHub repository at the specific tag (v0.1.0) using SSH authentication.

## Notes

- This method requires Rust to be installed on the system, as it will build the package from source
- If you prefer to use pre-built wheels, you can reference them directly if HTTPS access is available
- For production use, consider using a private package index or artifact repository