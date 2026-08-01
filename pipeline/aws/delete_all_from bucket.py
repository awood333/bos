'''pipeline/aws/delete_all_from bucket.py'''
import boto3


#NOTE run this to delete EVERYTHING from 'plots' in the AWS bucket

s3 = boto3.client('s3')
bucket = 'cow-bucket-613211402323-ap-southeast-7-an'
prefix = 'plots/Net_Revenue/'

# 1. List what's currently in S3
existing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
existing_keys = {obj['Key'] for obj in existing.get('Contents', [])}

# 2. Build the set of keys that SHOULD exist (from alive_ids)
live_keys = {f"{prefix}cow_{wy_id}_net_revenue.png" for wy_id in alive_ids}

# 3. Delete anything in S3 that's not in the live set
stale_keys = existing_keys - live_keys
if stale_keys:
    s3.delete_objects(
        Bucket=bucket,
        Delete={'Objects': [{'Key': k} for k in stale_keys]}
    )
    print(f"Deleted {len(stale_keys)} stale plot(s): {stale_keys}")