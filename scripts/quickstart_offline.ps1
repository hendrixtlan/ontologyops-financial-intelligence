$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
if (-not (Test-Path ".venv")) { python -m venv .venv }
. .\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m ontologyops stream-demo
python -m ontologyops investigate --engine reference
python -m pytest -q
