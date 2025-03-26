import os

import boto3

os.makedirs("./s3-concepts", exist_ok=True)

s3 = boto3.client("s3")

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket="cpr-production-document-cache", Prefix="concepts")

for page in pages:
    for obj in page.get("Contents", []):
        file_key = obj["Key"]
        local_path = os.path.join("./s3-concepts", file_key)

        # Ensure directories exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        print(f"Downloading {file_key} to {local_path}...")
        s3.download_file(
            Bucket="cpr-production-document-cache", Key=file_key, Filename=local_path
        )


print("Done")
