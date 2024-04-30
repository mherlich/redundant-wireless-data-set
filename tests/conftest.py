"""
Fixtures for tests
"""

import os
import re
import pytest

import pandas as pd

import prepare.dedicated

@pytest.fixture(name="data_folder") # Setting name this way, prevents redefinition on use of fixture
def fixture_data_folder():
    """Folder that contains data set and raw data (in subfolder)"""
    return "data/"

@pytest.fixture(name="newest_dataset_name")
def fixture_newest_dataset_name(data_folder):
    """Provide name of newest data set"""
    datasets = [data_folder + f for f in os.listdir(data_folder)
        if re.match("SRFG-Redun-[0-9]*.fth", f)]
    datasets.sort(key=os.path.getmtime)
    return datasets[-1]

@pytest.fixture(name="df")
def fixture_df(newest_dataset_name):
    """Load newest data set as df"""
    return pd.read_feather(newest_dataset_name)

@pytest.fixture()
def dfa(df):
    """Select only data for provider A"""
    return df[df['FullName'].str.startswith('A1')]

@pytest.fixture()
def dfb(df):
    """Select only data for provider B"""
    return df[df['FullName']=='3 AT']

@pytest.fixture()
def samefile(df):
    """True if row is from same file as preceding row"""
    return df['file'] == df['file'].shift(1)

# Fixtures for internal tests

@pytest.fixture()
def full(newest_dataset_name):
    """Full data set that is not reduced to rectangle under consideration"""
    return pd.read_feather(newest_dataset_name.replace('_ready', ''))

@pytest.fixture()
def raw_folder(data_folder):
    """Folder of the raw data files before preprocessing"""
    return data_folder + "all/"

@pytest.fixture()
def dedicated():
    """Data frame that specifies the dedicated measurements trips"""
    return prepare.dedicated.load_dedicated()
