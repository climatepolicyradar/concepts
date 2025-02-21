import boto3

session = boto3.Session()
creds = session.get_credentials()
if creds is None:
    print("No credentials found.")
else:
    print("Credentials loaded:", creds)