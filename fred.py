import urllib

import numpy as np
import pandas as pd
import requests

FRED_BASE_URL = "https://api.stlouisfed.org"
FRED_SERIES_URL = "fred/series/observations"
FRED_SERIES_FULL_URL = urllib.parse.urljoin(FRED_BASE_URL, FRED_SERIES_URL)

with open("fred_api_key.txt") as f:
    FRED_API_KEY = f.read().strip()


def download_fred(
    series_id,
    base_url=FRED_BASE_URL,
    data_url=FRED_SERIES_URL,
    api_key=FRED_API_KEY,
):
    """
    Download data from a FRED series.
    
    :param str series_id: ID of the FRED data series
    :param str base_url: URL of the FRED website
    :param str series_url: URL for FRED data API
    :param str api_key: FRED API key
    :returns (dict): response from the FRED API request
    """
    full_url = urllib.parse.urljoin(base_url, data_url)
    payload = {
    "series_id": series_id,
    "api_key": api_key,
    "file_type": "json"
    }
    response = requests.get(full_url, params=payload)
    response.raise_for_status
    return response.json()


def parse_fred(
    data,
    columns=None,
    dtypes=None
):
    """
    Convert the response from the FRED API into a pandas dataframe.
    
    :param dict data: response from the FRED API, returned by the .json() method
    :param list columns: list of column names
    :param dict dtypes: data types of the dataframe, use "date" for timestamps
    """
    df = pd.DataFrame(columns=columns)
    for obs in data["observations"]:
        obs.pop("realtime_start")
        obs.pop("realtime_end")
        df = df.append(obs, ignore_index=True)
    # FRED seems to put a dot to represent no value
    df.replace(".", np.nan, inplace=True)
    if dtypes:
        # copy dtypes to keep it immutable
        types = {**dtypes}
        topop = []
        for column, _type in types.items():
            if _type.lower() == "date":
                topop.append(column)
                df.loc[:, column] = pd.to_datetime(df[column])
        for column in topop:
            types.pop(column)
        return df.astype(types)
    return df