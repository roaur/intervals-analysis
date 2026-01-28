# Agent Instructions

This project uses `uv` for Python dependency management.

## Python Execution
- ALWAYS use `uv run python <script.py>` or `.venv/bin/python <script.py>` to execute Python scripts.
- NEVER try to run `python <script.py>` directly with the system Python.

## Dependency Management
- To add packages: `uv add <package>`
- To sync dependencies: `uv sync`
- Do NOT use `pip install`.
