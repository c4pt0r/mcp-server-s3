import os
from typing import Any
from dotenv import load_dotenv
import boto3
from mcp.server.fastmcp import FastMCP, Context

# Load environment variables
load_dotenv()

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-west-1')
)
# Initialize FastMCP server
app = FastMCP("s3")

@app.tool(name="list_buckets", description="List all buckets")
async def list_buckets(context: Context) -> Any:
    try:
        # List all buckets
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        return {"buckets": buckets}
    except Exception as e:
        return {"error": str(e)}

@app.tool(name="list_bucket", description="List objects in a bucket")
async def list_bucket(context: Context, bucket_name: str, key_prefix: str = ""):
    try:
        if not bucket_name:
            return {"error": "S3_BUCKET_NAME not set"}

        # List objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key_prefix)
        files = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })

        return {
            'bucket': bucket_name,
            'files': files
        }
    except Exception as e:
        return {"error": str(e)}

@app.tool(name="get_object", description="Get an object from a bucket")
async def get_object(context: Context, bucket_name: str, key: str):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].read()
    except Exception as e:
        return {"error": str(e)}

@app.tool(name="put_object", description="Put an object into a bucket")
async def put_object(context: Context, bucket_name: str, key: str, body: str):
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.tool(name="upload_local_file", description="Upload a local file to a bucket")
async def upload_local_file(context: Context, bucket_name: str, local_path: str, key: str):
    try:
        s3_client.upload_file(local_path, bucket_name, key)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.tool(name="download_file_to_local", description="Download a file from a bucket to a local path")
async def download_file_to_local(context: Context, bucket_name: str, key: str, local_path: str):
    try:
        s3_client.download_file(bucket_name, key, local_path)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

@app.tool(name="delete_object", description="Delete an object from a bucket")
async def delete_object(context: Context, bucket_name: str, key: str):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    app.run(transport='stdio')
