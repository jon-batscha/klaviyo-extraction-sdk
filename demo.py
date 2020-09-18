import extractor
import boto3
import json
import decimal
import time
import utils

# account
private_key = ''



# events
metric_id = ''
latest_timestamp = int(time.time())
earliest_timestamp = latest_timestamp-60*5


extractor = extractor.Extractor(private_key)

events = extractor.get_metric_events_parallel(metric_id,earliest_timestamp,latest_timestamp)

# profiles
segment_id = ''
max_profiles = 50


ids = extractor.get_profile_ids(segment_id)[:max_profiles]

profiles = [extractor.get_profile(id) for id in ids]

profiles = [p for p in profiles if p]


# write data
events_local_path = 'events.json'
profiles_local_path = 'profiles.json'


utils.write_data(events,events_local_path)
utils.write_data(profiles,profiles_local_path)



#s3
s3_bucket_name = 'klaviyo-extract'
events_s3_key = 'events.json'
profiles_s3_key = 'profiles.json'


s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket_name)

bucket.upload_file(events_local_path,events_s3_key)
bucket.upload_file(profiles_local_path,profiles_s3_key)



#dynamodb
region = 'us-east-2'
events_table_name = 'events'
profiles_table_name = 'profiles'


dynamodb = boto3.resource('dynamodb', region_name=region)

events_table = dynamodb.Table(events_table_name)
profiles_table = dynamodb.Table(profiles_table_name)

with events_table.batch_writer() as batch:
    for event in events:
        batch.put_item(Item=utils.float_to_decimal(event))

with profiles_table.batch_writer() as batch:
    for profile in profiles:
        batch.put_item(Item=utils.float_to_decimal(profile))
