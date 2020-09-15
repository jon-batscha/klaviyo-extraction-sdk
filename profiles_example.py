import extractor
import time

ee = extractor.Extractor('config.ini')

segment_id = ''

start = time.time()
ids = ee.get_profile_ids(segment_id)
end = time.time()

print('\ncompleted extracting IDs')
print('time',end-start)
print('count',len(ids))

ids = ids[:10]

profiles = [ee.get_profile(id) for id in ids]

profiles = [p for p in profiles if p]