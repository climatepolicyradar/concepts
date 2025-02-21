#!/bin/sh
mkdir s3-concepts
aws s3 sync s3://cpr-production-document-cache/concepts ./s3-concepts
fastapi run server.py --port 8080