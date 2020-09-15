import boto3
import json
import math
import time
import multiprocessing
import csv
import os
import sys
import requests
import argparse
from operator import itemgetter
import datetime
import configparser
import decimal
from itertools import repeat


# events
def write_events(events,filename):

    with open(filename,'w') as outfile:

        for event in events:

            outfile.write(json.dumps(event)+'\n')


def timestamp_ranges(low,high,threads):
    '''
    input: start/end timestamps (inclusive), and # of threads
    return: n ranges (# threads), ascending
    '''

    low = max(int(low),0)

    step = (high-low)//threads

    extra_step = (high-low)%threads

    steps = [step]*(threads-1) + [step+extra_step]

    ranges = []

    current_start = low

    for current_step in steps:

        current_end = current_start + current_step - 1

        current_range = (current_start,current_end)

        ranges.append(current_range)

        current_start = current_end + 1

    return ranges

# for dynamodb
def float_to_decimal(dictionary):
    '''
    given a dictionary, replace all float with Decimal (for dynamoDB)
    '''

    return json.JSONDecoder(parse_float=decimal.Decimal).decode(json.dumps(dictionary))





