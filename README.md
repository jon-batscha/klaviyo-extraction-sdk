# Klaviyo-Extract SDK

This SDK is a wrapper around Klaviyo's API, that makes it easier to extract data via the API.

Currently, the SDK supports extracting Events, Profiles, and Metrics, but in the future will also support extracting more complex funnel metrics.

# Component Files

#### `extractor.py`
- defines an `Extractor` class containing methods for extracting data

#### `utils.py`
- this file contains a few helper functions used the `Extractor` class

# Dependencies

Python 3.8.3

packages:
- requests : https://requests.readthedocs.io/en/master/
  - `pip install requests`
