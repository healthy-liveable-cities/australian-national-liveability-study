# Purpose: Output a csv of geocoded location data given an input csv containing Google map urls
# Authors:  Carl Higgs using responses from StackExchange, as below
# Date:    20181116

import argparse
import os
import httplib
import urlparse
import re
import pandas as pd

def unshorten_url(url):
    # gets long form of a shortened url
    # copied from 
    # Adam Rosenfield
    # https://stackoverflow.com/questions/4201062/how-can-i-unshorten-a-url
    parsed = urlparse.urlparse(url)
    h = httplib.HTTPConnection(parsed.netloc)
    h.request('HEAD', parsed.path)
    response = h.getresponse()
    if response.status/100 == 3 and response.getheader('Location'):
        return response.getheader('Location')
    else:
        return url

def coords_from_url(url,short_url = False):
    # returns coordinates from a supplied url, assuming google maps format
    # Drawing on 
    # GEK
    # https://stackoverflow.com/questions/32546135/convert-google-maps-link-to-coordinates?noredirect=1&lq=1
    # Hoda Raisi 
    # https://help.parsehub.com/hc/en-us/articles/226061627-Scrape-latitude-and-longitude-data-from-a-Google-Maps-link
    if short_url == True:
      url = unshorten_url(url)
    temp = re.search('\@([-?\d\.]*)\,([-?\d\.]*)', url, re.DOTALL)
    try:
      latitude  = temp.groups()[0]
      longitude = temp.groups()[1]
      return([latitude,longitude])
    except AttributeError:
      print(unshorten_url(url))
      return None

# check that a supplied path is valid      
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
                    help='addresses to be geocoded (a google maps URL)',
                    required=True)
parser.add_argument('--short_url',
                    help='a flag to indicate whether the supplied google maps URL is shortened (set True if so)',
                    default=False)
args = parser.parse_args()

# Get the project name from the supplied project directory
locations = pd.read_csv(args.csv)
address = args.address
output  = os.path.join(os.path.dirname(args.csv),args.output)

# retrieve coordinates from urls
locations['coords']    = locations[address].apply(coords_from_url,short_url = True)

# split coordinates into seperate lat and lon
locations['latitude']  = locations['coords'].apply(lambda x: x[0])
locations['longitude'] = locations['coords'].apply(lambda x: x[1])

# summarise apparent success at retrieving data
successes = int(locations['coords'].dropna().shape[0])
attempts = int(locations[address].dropna().shape[0])
percent = 100*successes/float(attempts)
print("Extracted coordinates for {} / {} ({:03.1f}%) supplied urls.".format(successes,attempts,percent))

# output results
locations.to_csv(output, index=False)
print("Result saved to: {}".format(output))
