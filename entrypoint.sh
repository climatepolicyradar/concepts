#!/bin/sh
# TODO: This is probably not the place to do this as it increases startup time
# significantly, but I can't seem to work out where else we can do it with the right permissions
mkdir s3-concepts
aws s3 sync s3://cpr-production-document-cache/concepts ./s3-concepts
python3 ./create_duckdb.py
fastapi run server.py --port 8080