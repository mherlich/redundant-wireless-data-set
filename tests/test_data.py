"""
General tests that should hold for all data sets
"""

import re
import datetime
import itertools

import numpy as np
import pandas as pd

import geopy.distance

import utils

def test_columns(df):
    """Columns in data match columns in documentation"""
    descriptions = []
    for line in open('README.md', 'r', encoding="utf8"):
        description = re.match('- ([\\w-]+):.+', line)
        if description:
            descriptions.append(description.group(1))
    assert set(descriptions) == set(df.columns)

def test_na_occurances(df):
    """Limited accurances of Not-a-number values in the indivdual columns"""
    assert df['time'].notna().all()
    assert df['timestamp'].notna().all()
    assert df['gpstime'].isna().mean() < 0.06

    assert df['lat'].notna().all()
    assert df['long'].notna().all()
    assert df['alt'].isna().mean() < 0.00001
    assert df['alt'].isna().groupby(df['file']).mean().max() < 0.002
    assert df['speed'].isna().mean() < 0.00001
    assert df['speed'].isna().groupby(df['file']).mean().max() < 0.002
    assert df['track'].isna().mean() < 0.00001
    assert df['track'].isna().groupby(df['file']).mean().max() < 0.004
    # alt, speed and track values differ, because track-fix occurs a second after alt and speed once

    assert df['file'].notna().all() # File is set for interpolated positions
    assert df['line'].isna().mean() < 0.06 # Line is not set for interpolated positions

    assert df['device'].notna().all()
    assert df['Rat'].notna().all()
    assert df['Numeric'].notna().all()
    assert df['State'].notna().all()
    assert df['FullName'].notna().all()
    assert df['ShortName'].notna().all()

    assert df['signal'].isna().mean() < 0.02
    assert df['signal'].isna().groupby(df['file']).mean().max() < 0.1
    assert df['netmode'].isna().mean() < 0.02
    assert df['netmode'].isna().groupby(df['file']).mean().max() < 0.1
    assert df['cell_id'].isna().mean() < 0.02
    assert df['cell_id'].isna().groupby(df['file']).mean().max() < 0.1
    assert df['mode'].isna().mean() < 0.03
    assert df['rsrq'].isna().mean() < 0.03
    assert df['rsrp'].isna().mean() < 0.03
    assert df['sinr'].isna().mean() < 0.03
    assert df['rssi'].isna().mean() < 0.40 # Missing often, but can be reconstructed (see utils.py)

    # These exist only for one of two types of measurement
    assert 0.55 > df['ping'].isna().mean() > 0.45
    assert 0.55 > df['owdDown'].isna().mean() > 0.45
    assert 0.55 > df['owdUp'].isna().mean() > 0.45
    assert 0.55 > df['lossUp_count'].isna().mean() > 0.45
    assert 0.55 > df['lossDown_count'].isna().mean() > 0.45

    assert 0.55 > df['datarateDown'].isna().mean() > 0.45
    assert 0.55 > df['datarateDown_app'].isna().mean() > 0.45

def test_backward_time(df, samefile):
    """Backward moving time"""
    assert samefile.mean() > 0.997
    assert (df['time'] > df['time'].shift(1)).where(samefile, np.nan).all()
    assert (df['timestamp'] > df['timestamp'].shift(1)).where(samefile, np.nan).all()
    # Contains NaT's -> Needs reversed test
    assert not (df['gpstime'] < df['gpstime'].shift(1)).where(samefile, np.nan).any()

def test_measuremente_type(df):
    """Measurement type"""
    df['datarateMeasurement'] = df['datarateDown'].notna()
    df['latencyMeasurement'] = ((df['ntp-GPS-PI_reach'] > 0) & (df['ntp-TP-Core_reach'] > 0))
    assert (df['datarateMeasurement'] | df['latencyMeasurement']).mean() > 0.95

def test_ntp_pi(df):
    """NTP sync status of client"""
    # tally
    assert (df['ntp-GPS-PI_tally'].isin([None, 'o', '-', '+', 'x', '*'])).all()

    # remote
    assert (df['ntp-GPS-PI_remote'].dropna() == '127.127.22.0').mean() > 0.93

    # refid
    assert (df['ntp-GPS-PI_refid'].dropna() == '.PPS.').mean() > 0.94

    # st
    assert (df[df['ntp-GPS-PI_st'].notna()]['ntp-GPS-PI_st'].isin([0, 1, 2, 3, 16])).all()
    assert (df['ntp-GPS-PI_st'] == 16).sum() <= 13 # No sync happens only in few cases

    # when
    assert df['ntp-GPS-PI_when'].min() >= 0
    assert df['ntp-GPS-PI_when'].max() < 2100
    assert (df['ntp-GPS-PI_when'] > 70).mean() < 0.01

    # poll
    assert (df['ntp-GPS-PI_poll'].isin([np.nan, 0, 8, 64])).all()

    # reach
    assert (df['ntp-GPS-PI_reach'] == 377).mean() > 0.40
    assert (df['ntp-GPS-PI_reach'] > 0).mean() > 0.45

    # delay
    assert df['ntp-GPS-PI_delay'].min() > -2400
    assert df['ntp-GPS-PI_delay'].max() < 70
    assert (df['ntp-GPS-PI_delay'] < -100).mean() < 0.002

    # offset
    assert df['ntp-GPS-PI_offset'].min() > -2400
    assert df['ntp-GPS-PI_offset'].max() < 370
    assert (df['ntp-GPS-PI_offset'] < -500).mean() < 0.004
    assert abs(df['ntp-GPS-PI_offset'].mean()) < 30
    assert df['ntp-GPS-PI_offset'].std() < 200

    # jitter
    assert df['ntp-GPS-PI_jitter'].min() >= 0
    assert df['ntp-GPS-PI_jitter'].max() < 375
    assert (df['ntp-GPS-PI_jitter'] > 60).mean() < 0.002

def test_ntp_core(df):
    """NTP sync status of server"""
    # tally
    assert (df['ntp-TP-Core_tally'].isin([None, '*'])).all()

    # remote
    assert (df['ntp-TP-Core_remote'].dropna() == '10.10.99.1').mean() > 0.94

    # refid
    assert (df['ntp-TP-Core_refid'].dropna() == '.PPS.').mean() > 0.68

    # st
    assert (df[df['ntp-TP-Core_st'].notna()]['ntp-TP-Core_st'].isin([0, 1, 2, 3])).all()

    # when
    assert df[df['ntp-TP-Core_when'].notna()]['ntp-TP-Core_when'].isin(range(9)).all()
    assert (df['ntp-TP-Core_when'] > 70).mean() < 0.006

    # poll
    assert (df['ntp-TP-Core_poll'].isin([np.nan, 0, 8])).all()

    # reach
    assert (df['ntp-TP-Core_reach'] == 377).mean() > 0.45

    # check delay
    assert df['ntp-TP-Core_delay'].min() >= 0
    assert df['ntp-TP-Core_delay'].max() < 0.2
    assert (df['ntp-TP-Core_delay'] < 0.05).mean() < 0.02
    assert (df['ntp-TP-Core_delay'] > 0.15).mean() < 0.01

    # check offset
    assert df['ntp-TP-Core_offset'].min() > -2.5
    assert df['ntp-TP-Core_offset'].max() < 3.11
    assert (df['ntp-TP-Core_offset'] > 0.3).mean() < 0.006
    assert abs(df['ntp-TP-Core_offset'].mean()) < 0.005
    assert df['ntp-TP-Core_offset'].std() < 0.2

    # check values of jitter
    assert df['ntp-TP-Core_jitter'].min() >= 0
    assert df['ntp-TP-Core_jitter'].max() < 2.7
    assert (df['ntp-TP-Core_jitter'] > 0.5).mean() < 0.006

def test_duration(df):
    """Check duration of file"""
    dff = df.groupby(['file'])
    timespan = dff['time'].max() - dff['time'].min()
    assert (timespan > datetime.timedelta(minutes=30)).sum() <= 2
    assert (timespan < datetime.timedelta(minutes=60)).all()

def test_volume(df):
    """Check the number of measurments per day, week, and month"""
    vol_day = df.groupby(pd.Grouper(key='time', freq='1d'))['lat'].count()
    assert vol_day.min() >= 0
    assert vol_day.median() >= 0
    assert 800 < vol_day.mean() < 1000
    assert vol_day.max() < 8000

    vol_week = df.groupby(pd.Grouper(key='time', freq='1w'))['lat'].count()
    assert vol_week.min() >= 0
    assert 6000 < vol_week.median() < 8000
    assert 6000 < vol_week.mean() < 8000
    assert vol_week.max() < 16000

    # Ignore last month, because it did not have the chance to collect as many measurements
    vol_month = df.groupby(pd.Grouper(key='time', freq='1m'))['lat'].count()
    assert vol_month[:-1].min() > 11000
    assert 26000 < vol_month.median() < 28000
    assert 27000 < vol_month.mean() < 30000
    assert vol_month.max() < 48000

def test_gps(df):
    """General GPS plausibility"""
    # Are values of long and lat in considered rectangle?
    assert df['long'].min() > 13.05
    assert df['long'].max() < 13.35
    assert df['lat'].min() > 47.84
    assert df['lat'].max() < 47.86

    # Check values of alt (alt is not reliable)
    assert df['alt'].min() > -472
    assert df['alt'].max() < 960
    assert (df['alt'] < 380).mean() < 0.0087 # Lowest altitude in Land Salzburg

    # Location of lowest (median) altitude along our track
    # long_of_min_alt = df.groupby(df["long"].round(3))["alt"].median().idxmin()
    # df[df['long'].round(3) == long_of_min_alt][['lat', 'long']].median()
    # -> 47.848097,13.080240
    # https://api.open-elevation.com/api/v1/lookup?locations=47.848097,13.08024 -> 502
    assert (df['alt'] < 502).mean() < 0.02
    assert (df['alt'] < 502-100).mean() < 0.01 # More than 100m lower than possible min

    # Location of highest (median) altitude along our track
    # long_of_max_lat = df.groupby(df["long"].round(3))["alt"].median().idxmax()
    # df[df['long'].round(3) == long_of_max_lat][['lat', 'long']].median()
    # -> 47.850657,13.186000
    # https://api.open-elevation.com/api/v1/lookup?locations=47.850657,13.186000 -> 646
    assert (df['alt'] > 646).mean() < 0.09
    assert (df['alt'] > 646+100).mean() < 0.0025 # More than 100m higher than possible max

    # Compare speed to reasonble speed on highway
    assert df['speed'].min() >= 0
    assert df['speed'].max() < 130
    assert (df['speed'] > 50).sum() <= 3
    assert (df['speed'] > 42).mean() <= 0.9999

    # Physical characteristics: quick changes should be rare
    assert (df['lat'].diff().abs() < 0.0005).mean() > 0.999
    assert (df['long'].diff().abs() < 0.001).mean() > 0.998
    assert (df['alt'].diff().abs() < 10).mean() > 0.998
    assert (df['speed'].diff().abs() < 3).mean() > 0.998
    assert (df['track'].diff().abs() < 10).mean() > 0.998

def test_movement_consistency(df, samefile):
    """Test locations and speed compatibility"""
    # All gps measurements are "new"
    assert (df["gpstime"].diff() != datetime.timedelta(seconds=0)).all()

    no_movement = df['long'].diff() == 0
    const_drive = (samefile & (df["speed"] > 20)
        & (df["speed"].diff(1).abs() < 5) & (df["speed"].diff(-1).abs() < 5))
    df["inconsistent_move"] = const_drive & no_movement
    assert df["inconsistent_move"].mean() < 0.00003
    assert df.groupby("file")["inconsistent_move"].mean().max() < 0.003

def test_gps_consistency(df):
    """Difference between median trajectory and indivudal trajectory"""
    track = df.groupby(df['long'].round(5))['lat'].median()
    df['dlat'] = df['long'].round(5).map(dict(track)) - df['lat']
    df['dlata'] = df['dlat'].abs()

    assert df['dlata'].max() < 0.0022
    assert df['dlata'].mean() < 0.0001
    assert (df.groupby("file")['dlata'].mean() > 0.0002).mean() < 0.02

def test_interpolation(df):
    """Interpolated values (~5%)"""
    assert 0.05 < df['notes'].str.contains('interpolated').mean() < 0.06
    assert df['notes'].str.contains('interpolated').groupby(df['file']).mean().max() < 0.25

def test_position_compare(df, samefile):
    """Plausibility of changes of position"""
    # Calculate difference of GPS data to compare with speed
    long_calc_met = geopy.distance.geodesic((df["lat"].mean(), df["long"].mean()),
        (df["lat"].mean(), df["long"].mean()+1)).meters  # factor for long (degree in m)
    lat_calc_met = geopy.distance.geodesic((df["lat"].mean(), df["long"].mean()),
        (df["lat"].mean()+1, df["long"].mean())).meters  # factor for lat (degree in m)
    # Calculation distance in meters
    df['difflong'] = df['long'].diff() * long_calc_met
    df['difflat'] = df['lat'].diff() * lat_calc_met
    df['timediff'] = df['time'].diff()
    df['posdiff'] = np.sqrt((df['difflong']**2 - df['difflat']**2).abs())
    df['posdiff'] = df['posdiff'] / df['timediff'].dt.seconds   # divide by time to get meter/s
    df['posdiff'] = df['posdiff'].where(samefile, np.nan)

    # Compare speed with calculated difference per sec
    assert (df['speed'].round() == df['posdiff'].round()).mean() > 0.2

    # Difference speed & diff less than 5 m/s
    assert (abs(df['speed'] - df['posdiff']) > 5).mean() < 0.3

    # Heading is between 0 and 360° (from north)
    assert df['track'].min() > 0
    assert df['track'].max() < 360

    # Check if track and direction match
    assert (df[df['difflong'] < 0]['track'] < 180).mean() < 0.003
    assert (df[df['difflong'] > 0]['track'] > 180).mean() < 0.003

    # Check if driving direction in each file is always the same (no turning around)
    file_west = df[df['track'] > 180]['file'].unique()
    file_east = df[df['track'] < 180]['file'].unique()
    assert len(set(file_west).intersection(set(file_east))) == 0

def test_timestamp_gpstime_match(df):
    """Match of timestamps from gps and system clock"""
    # Compare time and timestamp, convert unix timestamp to datetime object
    df['ts'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['gt'] = pd.to_datetime(df['gpstime'], errors='coerce', utc=True)
    delta = ((df["gt"]-df["ts"])/datetime.timedelta(seconds=1))
    assert abs(delta.mean()) < 2
    assert delta.std() < 2.2

def test_trip(df):
    """First and last measurement are always at the end-points"""
    # First measurement (based on time) is either lowest or highest long on trip
    dft = df.groupby('trip')
    longmin_equals_timemin = (dft['long'].min()
        == df.loc[dft['time'].idxmin()].groupby('trip')['long'].first())
    longmax_equals_timemin = (dft['long'].max()
        == df.loc[dft['time'].idxmin()].groupby('trip')['long'].first())
    assert (longmin_equals_timemin | longmax_equals_timemin).all()

    # Same or last measurement
    longmin_equals_timemax = (dft['long'].min()
        == df.loc[dft['time'].idxmax()].groupby('trip')['long'].first())
    longmax_equals_timemax = (dft['long'].max()
        == df.loc[dft['time'].idxmax()].groupby('trip')['long'].first())
    assert (longmin_equals_timemax | longmax_equals_timemax).all()

def test_dropped_lines(df):
    """Check if not too many lines are dropped from each file"""
    dfr = df[-df['notes'].str.contains('interpolated')].copy()
    dfr['linejump'] = dfr['line'].diff() != 1
    assert dfr['linejump'].mean() < 0.06
    assert dfr.groupby('file')['linejump'].mean().max() < 0.28

def test_time_jumps(df, samefile):
    """Test for missing parts of files"""
    df['timediff'] = df['time'].diff().where(samefile, np.nan)
    dfr = df[-df['notes'].str.contains('interpolated')]
    assert dfr['timediff'].min() == datetime.timedelta(seconds=1)
    # Sometimes 'holes' appear in a track because we drop switching to old technology
    assert dfr['timediff'].max() < datetime.timedelta(seconds=90)
    assert (dfr['timediff'] > datetime.timedelta(seconds=1)).mean() < 0.00002
    assert (dfr['timediff'] > datetime.timedelta(seconds=60)).mean() < 0.000002

def test_continuous_time(df, samefile):
    """Test for jumps in final data set (after interpolation)"""
    df['timediff'] = df['time'].diff().where(samefile, np.nan)
    assert df['timediff'].min() == datetime.timedelta(seconds=1)
    assert df['timediff'].max() < datetime.timedelta(minutes=2)
    assert (df['timediff'] > datetime.timedelta(seconds=1)).mean() < 0.00005

def test_netmodes(dfa, dfb):
    """Only LTE (19 in one device and 101 in the other, because of firmware differences?)"""
    assert (dfa['netmode'].isin([0.0, 19.0]) | dfa['netmode'].isna()).all()
    assert (dfb['netmode'].isin([0.0, 101.0]) | dfb['netmode'].isna()).all()

def test_network(df, dfa, dfb):
    """Devices are in the networks we expect"""
    # Check whether device is either GPS-PI-02 or GPS-PI-05
    assert df['device'].isin(['GPS-PI-02', 'GPS-PI-05']).all()

    # Check whether rat is 7
    assert (df['Rat'] == '7').all()

    # Check if Numeric only consists of the 3 Provider Identifiers
    assert df['Numeric'].isin(['23201', '23205', '23210']).all()
    assert dfa['Numeric'].isin(['23201']).all()
    assert dfb['Numeric'].isin(['23205', '23210']).all()

    # Check whether State is always 0
    assert (df['State'] == '0').all()

def test_signal(df):
    """General boundary conditions for signal strength parameters"""
    # Check signal in [0,5]
    assert df['signal'].min() >= 0
    assert df['signal'].max() <= 5
    assert (df['signal'].isin(range(6)) | df['signal'].isna()).all()

    # rsrq
    assert df['rsrq'].min() >= -20
    assert df['rsrq'].max() < -2.5

    # sinr
    assert df['sinr'].max() <= 42
    assert df['sinr'].min () >= -42
    assert df[df['sinr'] < 40]['sinr'].min() <= 30
    assert df[df['sinr'] > -42]['sinr'].max() >= -20
    assert (df['sinr'] == -42).mean() < 0.0005
    assert (df['sinr'] == 42).mean() < 0.02

    # rssi
    assert df['rssi'].min() >= -115
    assert df['rssi'].max() <= -50
    assert (df['rssi'] < -111).mean() < 0.00002

    # rsrp
    assert df['rsrp'].min() >= -141
    assert df['rsrp'].max() <= -49
    assert (df['rsrp'] < -130).mean() < 0.002

def test_cell_id(df, dfa, dfb):
    """Check consistency of cell_ids"""
    # Number of seen cells per day
    dfd = df.groupby(pd.Grouper(key='time', freq='1d'))['cell_id'].nunique()
    assert dfd.max() < 90
    assert ((dfd == 0) | (dfd > 10)).all()

    # Check whether different providers have different cell_ids
    a_cells = dfa['cell_id'].unique()
    b_cells = dfb['cell_id'].unique()

    assert (set(a_cells) & set(b_cells)).issubset(set([None, 'None', '0']))

    # Test if cell_ids appear only at geographically limited locations (ignoring 0 and None)
    dfg = df.groupby('cell_id')
    assert (dfg['long'].max() - dfg['long'].min())[
        ~dfg.first().index.isin(['0', 'None'])].max() < 0.08

def test_data_rate(df, dfa, dfb):
    """Check values of datarateDown"""
    assert df['datarateDown'].min() >= 0
    # Limit of our contract is 150e6, but is exceeded (might be a timedrift problem)
    assert df['datarateDown'].max() < 260e6
    # Very few higher than expected from limit, but occurs too often for individual inspection
    assert (df['datarateDown'] > 150e6).mean() <= 0.99995

    # Both providers
    df_drbyday = df[df['datarateDown'].notna()].groupby(pd.Grouper(key='time', freq='1d'))
    assert df_drbyday['datarateDown'].mean().max() < 55e6
    assert df_drbyday['datarateDown'].max().max() < 260e6
    assert df_drbyday['datarateDown'].mean().min() > 14e6

    # Provider A
    dfa_drbyday = dfa[dfa['datarateDown'].notna()].groupby(pd.Grouper(key='time', freq='1d'))
    assert dfa_drbyday['datarateDown'].mean().max() < 65e6
    assert dfa_drbyday['datarateDown'].max().max() < 260e6
    assert dfa_drbyday['datarateDown'].mean().min() > 15e6

    # Provider B
    dfb_drbyday = dfb[dfb['datarateDown'].notna()].groupby(pd.Grouper(key='time', freq='1d'))
    assert dfb_drbyday['datarateDown'].mean().max() < 45e6
    assert dfb_drbyday['datarateDown'].max().max() < 130e6
    assert dfb_drbyday['datarateDown'].mean().min() > 10e6

def test_datarate_app(df):
    """Compare datarate with the estimate"""
    assert df['datarateDown_app'].min() >= 0
    # Limit of our contract is 150e6, but is exceeded (might be a timedrift problem)
    assert df['datarateDown_app'].max() < 1030e6
    # We have one extreme outlier, but others are also high
    assert (df['datarateDown_app'] > 360e6).sum() <= 1
    # Very few higher than expected from limit, but occurs to often for individual inspection
    assert (df['datarateDown_app'] > 150e6).mean() <= 0.9999

    # Appearing together with datarateDown
    assert (df['datarateDown'].isna() == df['datarateDown_app'].isna()).mean() > 0.97
    assert ((df['datarateDown'].isna()
        == df['datarateDown_app'].isna()).groupby(df['file']).mean() > 0.9).mean() > 0.9

    # Compare individual measurements
    # Extremely high difference between measurement methods
    assert (df['datarateDown'] - df['datarateDown_app']).abs().max() < 992e6
    # Only one extreme outlier, but rest is also high
    assert ((df['datarateDown'] - df['datarateDown_app']).abs() >= 170e6).sum() <= 1
    # <20% with more than 10MBit/s diff
    assert ((df['datarateDown'] - df['datarateDown_app']).abs() > 10e6).mean() < 0.2

    # Compare averages in file
    # Positive overhead (does not hold everyhere, becuase of imperfect alignment)
    assert df['datarateDown'].mean() > df['datarateDown_app'].mean()
    fmd = df.groupby('file')['datarateDown'].mean() - df.groupby('file')['datarateDown_app'].mean()
    assert fmd.min() > -11e6
    assert fmd.max() < 28e6
    assert (fmd.abs() > 5e6).mean() < 0.01

    # Correlation between data rate measurements
    assert df['datarateDown'].corr(df['datarateDown_app']) > 0.94

    # Compare file means
    dff = df.groupby('file')
    assert (dff['datarateDown'].mean() - dff['datarateDown_app'].mean()).abs().max() < 28e6
    assert ((dff['datarateDown'].mean().dropna()
        - dff['datarateDown_app'].mean().dropna()).abs() > 4e6).mean() < 0.2

    # Correlation in files
    # Sometimes correlation between measurement methods is low
    assert dff[['datarateDown','datarateDown_app']].corr().unstack().iloc[:,1].min() > 0.4
    assert (dff[['datarateDown','datarateDown_app']].corr().unstack().iloc[:,1].dropna()
        < 0.75).mean() < 0.085

def test_latency(df):
    """Consistency of latency"""
    # less than 2% of ping und owdDown not togehter
    assert (df['ping'].isna() != df['owdDown'].isna()).mean() < 0.02
    assert (df['ping'].isna() != df['owdUp'].isna()).mean() < 0.02
    # Ignore last day as this might not be complete, due to missing sync at the end
    dfo = df[df.time.dt.date < df.time.max().date()]
    assert (dfo['owdDown'].isna() != dfo['owdUp'].isna()).sum() == 0  # owdDown & owdUp together

    # Values of ping
    assert df['ping'].min() >= 0
    assert df['ping'].max() < 10000
    assert (df['ping'] > 100).mean() < 0.002

    # Values of owdDown and owdUp (with mean over owd values)
    df['owdDownMean'] = df['owdDown'].dropna().apply(lambda x: np.mean(x) if len(x) else np.nan)
    df['owdUpMean'] = df['owdUp'].dropna().apply(lambda x: np.mean(x) if len(x) else np.nan)

    assert df['owdDownMean'].min() > -355e3 # Ahould always be positive! (Timesync problem)
    assert (df['owdDownMean'] < 0).mean() < 0.005
    assert df['owdDownMean'].max() < 4600e3
    assert (df['owdDownMean'] > 2500e3).mean() < 0.0001

    assert df['owdUpMean'].max() < 5000e3
    assert (df['owdUpMean'] > 50e3).mean() < 0.01
    assert df['owdUpMean'].min() > -2500e3

    df['owdSum'] = (df['owdUpMean'] + df['owdDownMean'])/1000
    assert ((df['owdSum'] - df['ping']).abs()).max() < 10000
    assert ((df['owdSum'] - df['ping']).abs() > 50).mean() < 0.004
    assert ((df['owdSum'] - df['ping']).abs() > 20).mean() < 0.06

    assert df['owdSum'].corr(df['ping']) > 0.12 # Seems low for similar measurements

    df['highping'] = df['ping'] > 100
    assert (df.groupby('file')['highping'].mean() < 0.02).all()

    # Up OWD is usually larger than down OWD (as is characteristic)
    assert ((df['owdUpMean'] - df['owdDownMean']).dropna() > 0).mean() > 0.7
    # Test needs to compare differece with 0 instead of directly, because of handling of nan's

def test_loss(df):
    """Plausibility of loss values"""
    # lossUp
    assert df['lossUp_count'].min() >= 0
    assert df['lossUp_count'].max() <= 11
    assert (df['lossUp_count'].isin(range(12)) | df['lossUp_count'].isna()).all()

    # lossDown
    assert df['lossDown_count'].min() >= 0
    assert df['lossDown_count'].max() <= 11
    assert (df['lossDown_count'].isin(range(12)) | df['lossDown_count'].isna()).all()

    # Ignoring last day as only one device might be synced (it is synced next time active)
    dfs = df[df['time'].dt.date < df['time'].max().date()]

    # Consistency of loss with ping and owd
    assert (dfs['owdDown'].isna() != dfs['lossDown_count'].isna()).sum() == 0
    assert (dfs['owdUp'].isna() != dfs['lossUp_count'].isna()).mean() < 0.013
    assert (dfs['ping'].isna() != dfs['lossDown_count'].isna()).mean() < 0.02
    assert (dfs['ping'].isna() != dfs['lossUp_count'].isna()).mean() < 0.03

def test_count_packets(df):
    """Plausibility of lost packet counters"""
    # Count packets of owdUp & owdDown
    df['down_count'] = df[df['owdDown'].notna()]['owdDown'].apply(len)
    df['up_count'] = df[df['owdUp'].notna()]['owdUp'].apply(len)

    assert 9.8 < df['down_count'].mean() < 10
    assert 9.8 < df['up_count'].mean() < 10
    assert df['down_count'].max() <= 11 # Allow 11 instead of 10 for slotting alignment problems
    assert df['up_count'].max() <= 26 # Time sync is better on remote server than on mobile device
    assert (df['up_count'].dropna() < 11).mean() > 0.98 # But mostly OK

    # Check whether owdCount is < 10
    assert (df['down_count'] > 10).mean() < 0.01
    assert (df['up_count'] > 10).mean() < 0.007

    # Check sum of packet counts
    assert 9.9 < (df['lossUp_count'] + df['up_count']).dropna().mean() < 10.1
    assert 9.9 < (df['lossDown_count'] + df['down_count']).dropna().mean() < 10.1
    assert ((df['lossDown_count'] + df['down_count']).dropna() != 10).mean() < 0.025
    assert ((df['lossUp_count'] + df['up_count']).dropna() != 10).mean() < 0.05

    # Check overlap between lossDown/down_count and lossUp/up_count
    assert (df[df['lossDown_count'] == 10]['down_count'] > 0).mean() < 0.02
    # Rather high, but might be caused by not being in sync
    assert (df[df['lossUp_count'] == 10]['up_count'] > 0).mean() < 0.45
    assert (df[df['lossDown_count'] > 0]['down_count'] == 10).mean() < 0.16
    # Rather high, but might be caused by not being in sync
    assert (df[df['lossUp_count'] > 0]['up_count'] == 10).mean() < 0.43
    assert (df[df['down_count'] == 10]['lossDown_count'] > 0).mean() < 0.003
    assert (df[df['up_count'] == 10]['lossUp_count'] > 0).mean() < 0.003
    assert (df[df['down_count'] > 0]['lossDown_count'] == 10).mean() < 0.0002
    assert (df[df['up_count'] > 0]['lossUp_count'] == 10).mean() < 0.002

def test_measurement_types(df):
    """Latency measurements appear together and are non-overlapping with data rate measurements"""
    df['datarateMeasurement'] = df['datarateDown'].notna()
    df['latencyMeasurement'] = df['ping'].notna() | df['owdDown'].notna() | df['owdUp'].notna()

    # At least one type of measurement
    assert (df['datarateMeasurement'] | df['latencyMeasurement']).mean() > 0.998
    # Both types of measurement
    assert (df['datarateMeasurement'] & df['latencyMeasurement']).sum() == 0

    # Test if all measurements on the same day belong to the same type (datarate vs latency)
    # equivalent: on each day, there is either no data rate measurement or no latency measurement
    dfd = df.groupby(pd.Grouper(key='time', freq='1d'))
    no_datarate_measurement = dfd['datarateMeasurement'].sum() == 0
    no_latency_measurement = dfd['latencyMeasurement'].sum() == 0
    assert (no_datarate_measurement | no_latency_measurement).all()

    # Grouped by file
    dff = df.groupby(['file'])
    assert (-dff['datarateMeasurement'].any() | dff['datarateMeasurement'].all()).mean() > 0.98
    assert (-dff['latencyMeasurement'].any() | dff['latencyMeasurement'].all()).all()

def test_daily_file_pairs(dfa, dfb):
    """Checks for daily filecounts of different providers"""
    filediffs = (dfa.groupby(pd.Grouper(key='time', freq='1d'))['file'].nunique()
        - dfb.groupby(pd.Grouper(key='time', freq='1d'))['file'].nunique()).abs()
    assert filediffs.max() < 3

    # Check if not more than these known errors occured
    assert (filediffs == 1).sum() <= 30
    assert (filediffs == 2).sum() <= 8

def test_pairs(df, dfa, dfb):
    """Checks for pairing of data"""
    dft = df.groupby("time")

    # Never more than 2 measurements at the same time
    assert dft["lat"].count().max() <= 2

    # No singles (too strict -> mean check)
    assert (dft["lat"].count() == 2).mean() > 0.75

    # When two measurements are at the same time, they need to come from different devices
    assert ((dft['lat'].count() < 2) | (dft['device'].nunique() == 2)).all()

    # Helper construct for paired measurements
    pairs = dfa.set_index('time').join(dfb.set_index('time'), how='outer', lsuffix='A', rsuffix='B')

    # Measurement problem in this time frame -> ignore
    pairs = pairs[(pairs.index < '2022-05-18') | (pairs.index > '2022-05-25')]

    # No singles test is too hard
    # -> make sure that for each file we have good overlap with measurements from the other device
    pairs['both_OK'] = pairs['timestampA'].notna() & pairs['timestampB'].notna()
    assert (pairs.groupby('fileA')['both_OK'].mean() < 0.9).mean() < 0.055
    assert (pairs.groupby('fileB')['both_OK'].mean() < 0.9).mean() < 0.111

    # No file overlaps with more than two files from the other device
    assert pairs.groupby('fileA')['fileB'].nunique().max() <= 2
    assert pairs.groupby('fileB')['fileA'].nunique().max() <= 2

    # Each file needs to have overlap with another file from the other device
    # and the files with the longest overlap should start roughly at the same time
    # (in case one measurement starts later it will overlap with two on the other device)
    start = df.groupby('file')['timestamp'].min()
    stop = df.groupby('file')['timestamp'].max()
    for direction in [['fileA', 'fileB'], ['fileB', 'fileA']]:  # Both directions
        dir_pairs = pairs.groupby(direction[0])[direction[1]].agg(pd.Series.mode)  # Largest overlap

        # Fraction of files without overlap
        assert (dir_pairs.apply(len) == 0).mean() < 0.082

        # Drop others
        dir_pairs = dir_pairs[dir_pairs.apply(len) > 0]
        assert (dir_pairs.apply(type)==str).all()

        # Compare start times
        start_diff = pd.DataFrame(dir_pairs).reset_index().apply(
            lambda r: np.abs(start[r[direction[0]]]-start[r[direction[1]]]), axis=1)
        assert start_diff.max() < 90000
        assert start_diff.mean() < 2000
        assert start_diff.median() < 1000
        assert (start_diff > 10000).mean() < 0.03

        # Compare stop times
        stop_diff = pd.DataFrame(dir_pairs).reset_index().apply(
            lambda r: np.abs(stop[r[direction[0]]]-stop[r[direction[1]]]), axis=1)
        assert stop_diff.max() < 60000
        assert stop_diff.mean() < 2000
        assert stop_diff.median() < 1000
        assert (stop_diff > 10000).mean() < 0.03

    # Check distance between paired points
    assert ((pairs['longA'] - pairs['longB']).dropna().abs() < 0.002).mean() > 0.997
    assert ((pairs['latA'] - pairs['latB']).dropna().abs() < 0.001).mean() > 0.996
    assert ((pairs['altA'] - pairs['altB']).dropna().abs() < 90).mean() > 0.96
    assert ((pairs['speedA'] - pairs['speedB']).dropna().abs() < 5).mean() > 0.999
    assert ((pairs['trackA'] - pairs['trackB']).dropna().abs() < 8).mean() > 0.99

    # Same/similar core sync
    assert (pairs['ntp-TP-Core_stB'].fillna('nan') == pairs['ntp-TP-Core_stB'].fillna('nan')).all()
    assert (pairs['ntp-TP-Core_offsetB'].fillna('nan') == pairs['ntp-TP-Core_offsetB'].fillna('nan')).all()
    assert (pairs['ntp-TP-Core_delayB'].fillna('nan') == pairs['ntp-TP-Core_delayB'].fillna('nan')).all()
    assert (pairs['ntp-TP-Core_refidA'].fillna('nan') == pairs['ntp-TP-Core_refidB'].fillna('nan')).mean() > 0.91

def test_notes(df):
    """Checks notes"""
    assert (df['notes'] != '').mean() < 0.06

    # Longitude cut labels where expected?
    # Happens max once at each end of file
    assert df['notes'].str.contains('cut-long').groupby(df['file']).sum().max() <= 2
    cutlong = df[df['notes'].str.contains('cut-long')]
    assert np.minimum((cutlong['long']-13.08).abs(), (cutlong['long']-13.33).abs()).max() < 0.03
    assert (np.minimum((cutlong['long']-13.08).abs(),
        (cutlong['long']-13.33).abs()) < 0.0006).mean() > 0.99

    # Latitude cut labels where expected?
    # Happens max once at each end of file
    assert df['notes'].str.contains('cut-lat').groupby(df['file']).sum().max() <= 2
    cutlat = df[df['notes'].str.contains('cut-lat')]
    assert np.minimum((cutlat['lat']-47.84).abs(), (cutlat['lat']-47.857).abs()).max() < 0.009

    # Most trips should have cuts at both their endpoints
    dft = df.groupby('trip')
    timemins = dft['time'].idxmin()
    assert (df.loc[timemins]['notes'].str.contains('cut-long')
        | df.loc[timemins]['notes'].str.contains('cut-lat')).mean() > 0.998
    timemaxs = dft['long'].idxmax()
    assert (df.loc[timemaxs]['notes'].str.contains('cut-long')
        | df.loc[timemaxs]['notes'].str.contains('cut-lat')).mean() > 0.95
    longmins = dft['long'].idxmin()
    assert (df.loc[longmins]['notes'].str.contains('cut-long')
        | df.loc[longmins]['notes'].str.contains('cut-lat')).mean() > 0.94
    longmaxs = dft['long'].idxmax()
    assert (df.loc[longmaxs]['notes'].str.contains('cut-long')
        | df.loc[longmaxs]['notes'].str.contains('cut-lat')).mean()> 0.95

def test_note_appearances(df):
    """Test if all notes appear and no unknown notes appear"""
    notes = ['resample-loss', 'interpolated', 'cut-long', 'cut-lat', 'cut-track', 'est_error',
        'neg-time-diff-time', 'neg-time-diff-gpstime', 'old-tech',
        'high-timestamp-position', 'high-timestamp-stdOut', 'low-timestamp-signalStrength',
        'low-timestamp-download', 'low-timestamp-stdOut',
        'incomplete-signalStrength', 'low-timestamp-position']
    # Need to add file-specific notes as they appear (that is, when this test fails)

    # All notes in dataset are in list
    for note in itertools.chain.from_iterable(df['notes'].str.split(',').tolist()):
        if note != '':
            assert note in notes

def test_negative_speed_fix(df):
    """Test repaired negative speed"""
    negative_speed_lines = df['notes'].str.contains('neg-speed')
    # Make sure the replacement is between its neighboring values (with margin)
    assert (np.minimum(df['speed'].shift(1), df['speed'].shift(-1)) - 0.1
        < df['speed'])[negative_speed_lines].all()
    assert (df['speed']
        < np.maximum(df['speed'].shift(1), df['speed'].shift(-1)) + 0.1)[negative_speed_lines].all()

def test_reconstruct_rssi(df):
    """Test reconstruction of RSSI"""
    # Check initial assumptions
    assert df['rssi'].max() <= -53

    dfr = utils.reconstruct.reconstruct_rssi(df)
    assert dfr['rssiI'].isna().mean() < 0.025
