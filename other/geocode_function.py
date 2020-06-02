# Purpose: Output a csv of geocoded location data given an input csv of location names and addresses
# Authors:  Carl Higgs
# Date:    20181116

import argparse
import os
import sys
import time
import geocoder
import pandas as pd
     
  

def valid_path(arg):
    if not os.path.exists(arg):
        msg = "The path %s does not exist!" % arg
        raise argparse.ArgumentTypeError(msg)
    else:
        return arg
        
# Parse input arguments
parser = argparse.ArgumentParser(description='Output a csv of geocoded location data given an input csv of location names and addresses')                   
parser.add_argument('--csv',
                    help='path to the input csv file, which contains columns of name and address data for rows to be geocoded',
                    required=True,
                    type=valid_path)
parser.add_argument('--output',
                    help='file name for output csv with x and y coordinates appended.',
                    required=True)
parser.add_argument('--address',
                    help='addresses to be geocoded',
                    required=True)
args = parser.parse_args()

# Get the project name from the supplied project directory
locations = pd.read_csv(args.csv)
address = args.address
output  = os.path.join(os.path.dirname(args.csv),args.output)

address_list = locations[address].dropna().tolist()
print(address_list)
locations['coords'] = locations[address].apply(geocoder.google).apply(lambda x: (x.latlng))

# list(geocoder.google(locations[address]))[0].latlng

print(locations[['Name'],[address]])

#g = geocoder.google(address_list, method='batch')
#for result in g:
#  print(result.address, result.latlng)