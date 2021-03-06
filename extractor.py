from utils import *

class Extractor:

    def __init__(self, private_key):

        self.private_key = private_key


    def get_metric_events(self, metric_id, earliest_timestamp, last_timestamp):
        '''
        args: metric id and start/end timestamps (10-digit unix int, inclusive of start/end)
        return: list of event objects
        '''

        earliest_timestamp -= 1 # to make inclusive, as since is exclusive, and we used desc sort

        print(f'Retrieving Events\nMetric ID : {metric_id}\ntimerange : {earliest_timestamp} - {last_timestamp}')

        events = []

        since = earliest_timestamp

        while True:

            timeline_call = f'https://a.klaviyo.com/api/v1/metric/{metric_id}/timeline?api_key={self.private_key}&since={since}&sort=asc&count=100'

            response = requests.get(timeline_call)

            if response.status_code == 404:

                print('Error : 404 code')
                return None

            elif response.status_code == 429:

                sleep = eval(response.json()['detail'].split()[-2])

                print(f'rate limit exceeded, # seconds to sleep: {sleep}')
                time.sleep(sleep)

            else:

                content = response.json()

                for event in content['data']:

                    if event['timestamp'] > last_timestamp:
                        
                        print(f'Done Retrieving Events for Thread : {len(events)}')
                        return events

                    else:

                        events.append(event)

                since = content['next']

                if not since:
                    print(f'Done Retrieving Events for Thread : {len(events)}')
                    return events

            print(f'# Events Retrieved by Thread : {len(events)}')

    
    def get_metric_events_serial(self,metric_id,earliest_timestamp,latest_timestamp):

        cores = multiprocessing.cpu_count()

        print(f'# CORES : {cores}')

        pool = multiprocessing.Pool(processes=cores)

        result = pool.starmap(self.get_metric_events,zip(repeat(metric_id),range(earliest_timestamp,latest_timestamp+1),range(earliest_timestamp,latest_timestamp+1)))
        pool.close()
        pool.join()

        out = []

        [out.extend(r) for r in result]

        return out 

    def get_metric_events_parallel(self,metric_id,earliest_timestamp,latest_timestamp):

        cores = multiprocessing.cpu_count()

        print(f'# CORES : {cores}')

        pool = multiprocessing.Pool(processes=cores)

        ranges = timestamp_ranges(earliest_timestamp,latest_timestamp,cores)

        result = pool.starmap(self.get_metric_events,zip(repeat(metric_id),[r[0] for r in ranges],[r[1] for r in ranges]))
        pool.close()
        pool.join()

        out = []

        [out.extend(r) for r in result]

        return out 

    def get_metrics_dict(self):
        '''
        returns dictionary mapping metric id to a dictionary containing its properties
        '''

        # get number of pages
        while True:

            metrics_call = f'https://a.klaviyo.com/api/v1/metrics?api_key={self.private_key}&count=1'

            response = requests.get(metrics_call)

            if response.status_code == 404:

                print('Error: 404')
                return None

            elif response.status_code == 429:

                sleep = eval(response.json()['detail'].split()[-2])

                print(f'rate limit exceeded, # seconds to sleep: {sleep}')
                time.sleep(sleep)

            else:

                content = response.json()

                total = content['total']

                pages = math.ceil(total/100)

                break

        metric_dict = dict()

        current_page = 0

        # get metrics from each page
        while current_page < pages:

            page_call = f'https://a.klaviyo.com/api/v1/metrics?api_key={self.private_key}&page={current_page}&count=100'

            response = requests.get(page_call)

            if response.status_code == 404:

                print('Error: 404')
                return None

            elif response.status_code == 429:

                sleep = eval(response.json()['detail'].split()[-2])

                print(f'rate limit exceeded, # seconds to sleep: {sleep}')
                time.sleep(sleep)

            else:

                content = response.json()

                for metric in content['data']:

                    metric_dict[metric['id']] = metric

                current_page += 1

        return metric_dict


    def get_profile_ids(self,segment_id):
        '''
        get list of ids from a given list/segment
        '''

        marker = None

        ids = []

        while True:

            if not marker:

                members_call = f'https://a.klaviyo.com/api/v2/group/{segment_id}/members/all?api_key={self.private_key}'

            else:

                members_call = f'https://a.klaviyo.com/api/v2/group/{segment_id}/members/all?api_key={self.private_key}&marker={marker}'

            response = requests.get(members_call)

            if response.status_code == 200:

                members_content = response.json()

                ids.extend([record['id'] for record in members_content['records']])

                if 'marker' in members_content:

                    marker = members_content['marker']

                else:

                    break

            elif response.status_code == 429:

                sleep = eval(response.json()['detail'].split()[-2])

                print(f'rate limit exceeded, # seconds to sleep: {sleep}')
                time.sleep(sleep)

            else:

                print('ERROR 404')
                return None

            print('IDs SAVED:',len(ids))


        print('IDs SAVED:',len(ids))
        return ids

    def get_profile(self, profile_id):
        '''
        extract profile data of a given ID; if no such id, returns None
        '''

        while True:

            profile_call = f'https://a.klaviyo.com/api/v1/person/{profile_id}?api_key={self.private_key}'

            response = requests.get(profile_call)

            if response.status_code == 429:

                sleep = eval(response.json()['detail'].split()[-2])

                print(f'rate limit exceeded, # seconds to sleep: {sleep}')
                time.sleep(sleep)

            else:

                if response.status_code == 200:

                    return response.json()

                else:

                    if response.status_code == 404:

                        print('ERROR: 404')

                    else:

                        print(f'UNKNOWN RESPONSE: {response.status_code}')

                    return None
                
    def get_profiles_parallel(self, profile_ids):
        '''
        given a list of profile IDs, return a list of profiles, using parallel API calls
        '''

        cores = multiprocessing.cpu_count()

        pool = multiprocessing.Pool(processes=cores)

        result = pool.map(self.get_profile, profile_ids)
        pool.close()
        pool.join()

        out = [profile for profile in result if profile]

        return out 

    def get_lists(self):
        '''
        return dictionary containing id -> name mapping for all lists in account
        '''

        lists = dict()

        
        while True:

            list_call = f'https://a.klaviyo.com/api/v2/lists?api_key={self.private_key}'

            response = requests.get(list_call)

            if response.status_code == 429:

                sleep = eval(response.json()['detail'].split()[-2])

                print(f'rate limit exceeded, # seconds to sleep: {sleep}')
                time.sleep(sleep)

            else:

                if response.status_code == 200:

                    lists = {_["list_id"]:_["list_name"] for _ in response.json()}

                    return lists

                else:

                    if response.status_code == 404:

                        print('ERROR: 404')

                    else:

                        print(f'UNKNOWN RESPONSE: {response.status_code}')

                    return None

    def get_memberships_parallel(self, list_ids):
        '''
        given a list of list/segment ids, return a dictionary mapping list_id to its list of members
        '''

        cores = multiprocessing.cpu_count()

        print(f'# CORES : {cores}')

        pool = multiprocessing.Pool(processes=cores)

        result = pool.map(self.get_profile_ids,list_ids)
        pool.close()
        pool.join()

        return {list_ids[i]: result[i] for i in range(len(list_ids))} 





