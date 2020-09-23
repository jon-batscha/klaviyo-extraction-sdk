# Klaviyo Extraction SDK

This SDK is a wrapper around Klaviyo's API, that makes it easier to extract data via the API.

Currently, the SDK supports extracting Events, Profiles, and Metrics, but in the future will also support extracting more complex funnel metrics.

## Component Files

#### `extractor.py`
- defines an `Extractor` class containing methods for extracting data

#### `utils.py`
- contains a few helper functions used the `Extractor` class

#### `demo.py`
- a sample script that demonstrates how to use some features in the SDK


## Optimization/Threading for Events

There are 2 parallelized methods for extracting events, each optimized for a different case:

#### `get_metric_events_parallel(...)`
- spins up as many processes as there are cores (we'll call this C), splits the given time-range into C subsets, extracts the data from each subset, and returns a list containing the data from the entire time range
- this is optimal for user driven events, such as `PlacedOrder`, where the distribution of events across time is relatively uniform, but suboptimal for events that are not uniformly distributed across time, particularly `ReceivedEmail` events

#### `get_metric_events_serial(...)`
- creates a thread for each individual timestamp in the range, each time processing C timestamps in parallel, before moving onto the next 'block' of C timestamps
- this is optimal for events such as `ReceivedEmail`, where all events in a given day can be clustered in a small segment of the day (due to campaigns being sent all at once), but suboptimal for events that are relatively uniformly distributed across time, such as `PlacedOrder`, as it requires sending unnecessary requests.

## Extracting Profiles

Extracting profiles is a 2-step process:
1. Extracting list of IDs from a given segment
  - this is not parallezable for a given segment, though users can create segments in Klaviyo strictly to split the work of extracting profiles
2. Extracting user profiles given a list of IDs
  - This can be parallelized
  
A recommended workflow for scaling extracting profile IDs (step 1 above) to tens of millions of users is:
1. Create initial pull of all IDs, and write to persistent storage
2. Create a segment of new profiles created over the past day
3. every day, only extract profile IDs from that segment, and add them to the store of IDs


## Dependencies

Python 3.8.3

packages:
- requests : https://requests.readthedocs.io/en/master/
  - `pip install requests`
