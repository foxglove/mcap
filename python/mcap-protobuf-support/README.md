# Python MCAP protobuf support

This package provides protobuf support for the Python MCAP file format reader.

## Installation

Install directly via pip:

```bash
pip install mcap-protobuf-support
```

## Examples

Examples of use of this package can be found in the `/examples` directory.
If you are developing in this monorepo, sync the Python tooling environment with uv:

```bash
cd /path/to/mcap/python
uv sync --frozen
```

Then switch to the examples directory and run the setup script there:

```bash
cd examples
./setup.sh
```

You should now be able to run the examples:

```bash
uv run python point_cloud_example.py output.mcap
```

## Stay in touch

Join our [Discord community](https://foxglove.dev/chat) to ask questions, share feedback, and stay up to date on what our team is working on.
