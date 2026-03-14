# Installation

## Requirements

- Python 3.11 or later
- No compiled extensions — pure Python

## pip

```bash
pip install recua
```

## uv

```bash
uv add recua
```

## With progress display (optional)

The live terminal progress bars require [rich](https://rich.readthedocs.io/):

```bash
pip install "recua[progress]"
# or
uv add "recua[progress]"
```

Without `rich`, recua falls back to plain log output automatically.

## Development install

```bash
git clone https://github.com/youruser/recua
cd recua
uv sync          # installs all dev dependencies including rich
pre-commit install
```

See [CONTRIBUTING](https://github.com/youruser/recua/blob/main/CONTRIBUTING.md)
for the full development workflow.

## Verify installation

```python
import recua
print(recua.__version__)
```
