# Vault Network QuickSight Scraper

This repository contains my submission for the Vault Network Full Stack Engineer technical assessment.

The goal of the exercise is to authenticate to the provided AWS QuickSight dashboard, extract the dashboard data, and write the result to `output.json` in the repository root using the exact field names required by the prompt:

- `date`
- `code`
- `registrations`
- `ftds`
- `state`

`registrations` and `ftds` are written as integers, and the output is validated before the file is saved.

## Overview

The scraper is implemented in Python using Playwright.

QuickSight is a JavaScript-heavy application with an interactive sign-in flow and a virtualized table UI, so using a real browser is the most reliable approach. The script handles the current AWS sign-in flow, waits for the dashboard to fully load, and then extracts data using the following strategy:

1. Try to export the data directly if a CSV-style export is available.
2. Fall back to scraping the rendered QuickSight grid when export is not available.


## Project Structure

- `scraper.py` - main scraper entrypoint
- `validate_output.py` - validator for the generated JSON file
- `requirements.txt` - Python dependencies
- `output.json` - generated submission file

## Requirements

- Python 3.11 or newer
- Playwright Chromium browser

## Setup

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
```

## Running the Scraper

You can provide credentials either through environment variables or CLI arguments.

### Option 1: Environment variables

#### macOS / Linux

```bash
export VAULT_USERNAME="candidate@vaultsportshq.com"
export VAULT_PASSWORD='Vault!nterview1'
python scraper.py
```

#### Windows PowerShell

```powershell
$env:VAULT_USERNAME="candidate@vaultsportshq.com"
$env:VAULT_PASSWORD="Vault!nterview1"
python scraper.py
```

### Option 2: CLI arguments

```bash
python scraper.py --username "candidate@vaultsportshq.com" --password "Vault!nterview1"
```

### Useful options

- `--headful` runs Chromium with a visible window for debugging
- `--output` writes the result to a custom file path
- `--downloads-dir` sets the directory used for temporary downloads
- `--dashboard-url` overrides the default dashboard URL

Example:

```bash
python scraper.py --headful --username "candidate@vaultsportshq.com" --password "Vault!nterview1"
```

## Logging

The script prints step-by-step progress while it runs, including:

- browser startup
- sign-in
- dashboard loading
- export attempt
- fallback scraping
- validation
- file write

In interactive terminals the progress display uses colored status output and a spinner. In non-interactive environments it falls back to plain text logs.

## Output

By default the scraper writes `output.json` to the repository root.

Records are deduplicated and sorted to keep the output deterministic.

## Validation

Validate the generated file with:

```bash
python validate_output.py
```

Or validate a different file:

```bash
python validate_output.py path/to/output.json
```

## Notes

- The current QuickSight sign-in flow is multi-step, so the scraper handles username and password as separate phases.
- The dashboard table is virtualized, so the fallback parser scrolls the grid and collects visible rows until no new rows appear.
- The script normalizes displayed dashboard dates into the required `YYYY-MM-DD` format before writing output.
- Validation happens before the final file is written so malformed output fails fast.