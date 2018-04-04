import csv
import sys
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import get_key, find_dotenv
from requests.auth import HTTPBasicAuth


CENSUS_KEY = get_key(find_dotenv(), 'CENSUS_KEY')
request_url = 'http://citysdk.commerce.gov'

SUCCESS_CODE = 200

# REQUEST_FORMAT: 'https://geo.fcc.gov/api/census/block/find?latitude={}}&longitude={}&showall=false&format=json'.format(lat, lon)

def convert_dates(date_series):
    '''
    Faster approach to datetime parsing for large datasets leveraging repated dates.

    Attribution: https://github.com/sanand0/benchmarks/commit/0baf65b290b10016e6c5118f6c4055b0c45be2b0
    '''
    dates = {date:pd.to_datetime(date) for date in date_series.unique()}
    return date_series.map(dates)



def replace_year(row):
    if row['creation_date'] and row['completion_date']:
        

    # ordered_df['creation_date'] = ordered_df['creation_date'].apply(lambda t, y = ordered_df['completion_date'].dt.year: t.replace(year = y))



def compile_requests(params_file):
    '''
    Create dataframe for each of the given 311 services stored as keys in a given
    dictionary, each of which is a dictionary indicating a related CSV URL.

    Inputs:
        - params (list of dictionaries): service request type details including CSV 
            URL and column renaming details.
    Outputs:
        - intial_records (dataframe): pandas dataframe of historical 311 data
    '''
    service_types = ['abandoned_building', 'alley_light', 'graffiti']
    params = json.load(open(params_file))

    initial_records = []
    
    try:
        for service in service_types:
            print("Starting: {}".format(service))
      
            r = requests.get(params[service]['url'])

            if r.status_code == SUCCESS_CODE:
                decoded_dl = r.content.decode('utf-8')
                req_reader = csv.reader(decoded_dl.splitlines(), delimiter = ',')
                read_info = list(req_reader)
                
                historicals_df = pd.DataFrame(read_info[1:], columns = read_info[0])
                historicals_df.rename(columns = params[service]['clean_cols'], inplace=True)
                
                print(historicals_df.columns)
                print()
                ordered_df = historicals_df.reindex(columns = params[service]['order'])

                if service == 'abandoned_building':
                    ordered_df['street_address'] = ordered_df[['street_num', 'street_dir', 'street_name', 'street_suff']].astype(str).apply(lambda x: ' '.join(x), axis=1)

                else:
                    ordered_df['creation_date'] = convert_dates(ordered_df['creation_date'])
                    ordered_df['completion_date'] = convert_dates(ordered_df['completion_date'])

                    # new_year = ordered_df['completion_date'].year
                    df.loc[df['A'] > 2, 'B'] = new_val
                    ordered_df = ordered_df.loc[ordered_df['creation_date'] < 2011, 'creation_date'] = ordered_df['creation_date'].replace(year = ordered_df['completion_date'].year)
                        
                    ordered_df['response_time'] = (ordered_df['completion_date'] - ordered_df['creation_date']).astype('timedelta64[D]')                
                 
                print(ordered_df.columns)
                initial_records.append(ordered_df)

        full_df = pd.concat(initial_records)

        return full_df



    except Exception as e:
        print("Unexpected error: {}".format(e), file=sys.stderr)


def retrieive_block_data(lat, lon):
    request_obj = '''
    {
      'lat': {},
      'lng': {},
      'level': 'tract',
      'sublevel': True,
      'api': 'acs5',
      'year': 2010,
      'variables': ['NAME', 
                    'B01003_001E', 
                    'B02001_003E', 
                    'B19001_004E', 
                    'B05001_006E']
    }
    '''

    response = requests.post(request_url, 
                             auth=HTTPBasicAuth(CENSUS_KEY, None), 
                             json = request_obj)

    block_data = response.json()



