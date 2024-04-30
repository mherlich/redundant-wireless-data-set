"""
Utility functions to load dedicated trip raw data
"""

import datetime

import pandas as pd

def load_dedicated():
    """Load specification of dedicated measurement trips"""
    dedicated = pd.read_csv("prepare/dedicated.csv")
    # If last End is open set to current time
    if dedicated['End'].isna().iloc[-1]:
        dedicated.iloc[-1, 1] = str(datetime.datetime.now())
    for c in dedicated:
        dedicated[c] = pd.to_datetime(dedicated[c], utc=False).dt.tz_localize("Europe/Vienna").dt.tz_convert("UTC")
    return dedicated
