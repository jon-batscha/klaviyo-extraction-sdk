## NOTE: this example script makes use of the event_extractor api to extract events from a specified metric for past n minutes using the events_extractor module
## this script extracts the same events using 3 separate methods (single_thread, parallel, serial) for illustrative purposes
import extractor
from utils import *
import sys
import json
import decimal

# parse command line args
# metric_id = sys.argv[1]
# earliest_timestamp = int(sys.argv[2])
# latest_timestamp = int(sys.argv[3])
latest_timestamp = int(time.time())
earliest_timestamp = 0
output_local_path = 'output.json'
output_s3_filename = 'output.json'
s3_bucket_name = 'klaviyo-extract-events'

extractor = extractor.Extractor('config.ini')


timer_start = time.time()
metrics_dict = extractor.get_metrics_dict()
events = []
for index,metric_id in enumerate(metrics_dict.keys()):
    print(f'{index}/{len(metrics_dict.keys())}')
    events.append(extractor.get_metric_events_parallel(metric_id,earliest_timestamp,latest_timestamp))
timer_end = time.time()

# print(f'\n\n# Events : {len(events)}')

# print(f'Runtime : {timer_end-timer_start}')

# # write events to localfile
# write_events(events,output_local_path)

# # send file to s3 bucket
# s3 = boto3.resource('s3')

# bucket = s3.Bucket(s3_bucket_name)

# bucket.upload_file(output_local_path,output_s3_filename)

# # send events to dynamodb
# table_name = 'extract-events-demo'
# dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
# table = dynamodb.Table(table_name)

# with table.batch_writer() as batch:
#     for event in events:
#         batch.put_item(Item=float_to_decimal(event))




