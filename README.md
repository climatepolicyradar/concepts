# Concepts

This repo uses DuckDB to fetch concepts data from S3 and store it in a local
database so we have fast access to the data. This allows us to fetch all concept
data in a single query, and also allows us to query the data in parallel.

## Dependency install

```bash
brew install uv
uv sync
```

## Run the API

```bash
source entrypoint.sh
```
