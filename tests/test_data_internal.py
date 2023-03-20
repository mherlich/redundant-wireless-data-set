"""
Internal tests that are (partly) only executable with raw data and test recentness of the data
"""

import os
import re
import time
import glob
import datetime
import itertools

import pandas as pd

def test_full_columns(full):
    """Columns in data match columns in documentation"""
    descriptions = []
    for line in open('README.md', 'r', encoding="utf8"):
        match = re.match('-([\\w-]+):.+', line)
        if match:
            descriptions.append(match.group(1))
    assert set(descriptions)-set(['trip']) == set(full.columns) # trip is only in df

def test_time(full):
    """Check time plausibility"""
    assert (datetime.datetime(year=2020, month=6, day=1, tzinfo=datetime.timezone.utc)
            < full['time'].min())
    assert full['time'].max() < datetime.datetime.now(datetime.timezone.utc)

def test_full_interpolation(full):
    """Interpolated values (~5%)"""
    assert 0.05 < full['notes'].str.contains('interpolated').mean() < 0.06

def test_contains_new_data(df, full, newest_dataset_name):
    """New file and new content"""
    # New content in full with cutoff at 10 days
    age = datetime.timedelta(days=10)
    assert time.time() - age.total_seconds() < os.path.getmtime(newest_dataset_name)

    cutoff = datetime.datetime.now(datetime.timezone.utc) - age
    assert full['time'].max() > cutoff

    # Test individual devices
    for device in full['device'].unique():
        # Any measurement from device
        assert full[full['device']==device]['time'].max() > cutoff

        # Test types of measurement
        assert full[(full['device']==device) & (full['ping'].notna())]['time'].max() > cutoff
        assert (full[(full['device']==device) & (full['datarateDown'].notna())]['time'].max()
                > cutoff)

    # Allow 17 days for df
    age = datetime.timedelta(days=17)
    cutoff = datetime.datetime.now(datetime.timezone.utc) - age
    assert df['time'].max() > cutoff

    # Test individual devices
    for device in df['device'].unique():
        # Any measurement from device
        assert df[df['device']==device]['time'].max() > cutoff

        # Test types of measurement
        assert df[(df['device']==device) & (df['ping'].notna())]['time'].max() > cutoff
        assert df[(df['device']==device) & (df['datarateDown'].notna())]['time'].max() > cutoff

def test_recent_gps_time(df, dfa, dfb):
    """Timesync over GPS has happened recently"""
    age = datetime.timedelta(days=17)
    cutoff = datetime.datetime.now(datetime.timezone.utc) - age
    assert df[df['ntp-TP-Core_refid'] == '.PPS.']['time'].max() > cutoff
    assert dfa[dfa['ntp-GPS-PI_refid'] == '.PPS.']['time'].max() > cutoff
    assert dfb[dfb['ntp-GPS-PI_refid'] == '.PPS.']['time'].max() > cutoff

def test_gap_mark(full):
    """Some marks indicate that parts are missing: Test if neighboring rows are missing"""
    gap_before = (full['time'].diff(1) > datetime.timedelta(seconds=1))
    gap_after = (-full['time'].diff(-1) > datetime.timedelta(seconds=1))
    gap_at = gap_before | gap_after
    for note in ['old-tech', 'cut-long', 'cut-lat', 'cut-track']:
        assert gap_at[full['notes'].str.contains(note)].all()

    # Test inverse; do both neighbors exist
    assert not gap_at[full['notes'].str.contains('interpolated')].any()

def test_download_details(df, full):
    """Plausibility of application level measurement details"""
    assert (df['download_total_sum'].dropna() == 0).mean() < 0.055
    assert (df['download_total_sum'].dropna() > 1).mean() < 0.005

    assert full['download_total_sum'].max() < 15
    assert full['download_connect_sum'].max() < 8
    assert full['download_starting_sum'].max() < 7
    assert full['download_done_sum'].max() < 6
    assert full['download_cannot_sum'].max() < 6
    assert full['download_timeout_sum'].max() < 6

    # Test if more downloads finish than start (not perfect as lines with downloads can be dropped)
    fullf = full.groupby("file")
    assert (((fullf['download_starting_sum'].sum() - fullf['download_done_sum'].sum()) >= 0).mean()
            > 0.975)
    assert (fullf['download_starting_sum'].sum() - fullf['download_done_sum'].sum()).max() <= 12

    # total_bw lines > 0 -> est available (not always because of misplaced linebreaks)
    assert ((full['download_total_sum'] > 0) == full['datarateDown_app'].notna()).mean() > 0.9999

def test_filename_content(full):
    """Filename corresponds to data within"""
    file_times = full.groupby('file')[['time']].min()
    file_times['filetime'] = pd.to_datetime(file_times.index.str[0:15],
        errors='coerce').tz_localize("Europe/Vienna").tz_convert("UTC")
    assert ((file_times['filetime'] - file_times['time']).abs().max()
            < datetime.timedelta(minutes=13))

def test_full_note_appearances(df, full):
    """Test if all notes appear and no unknown notes appear"""
    notes = ['resample-loss', 'interpolated', 'cut-long', 'cut-lat', 'cut-track', 'est_error',
        'neg-time-diff-time', 'neg-time-diff-gpstime', 'old-tech', 'high-timestamp-position',
        'high-timestamp-stdOut', 'low-timestamp-signalStrength', 'low-timestamp-download',
        'low-timestamp-stdOut', 'incomplete-signalStrength', 'low-timestamp-position']
    # Need to add file-specific notes as they appear (that is, when this test fails)

    # All types of notes appear in full dataset
    for note in notes:
        assert full['notes'].str.contains(note).any() | df['notes'].str.contains(note).any()

    # All notes in dataset are in list
    for note in itertools.chain.from_iterable(full['notes'].str.split(',').tolist()):
        if note != '':
            assert note in notes

def test_abandoned_measurement_logs(raw_folder):
    """Test if days with measurement logs have data files"""
    logs = glob.glob(raw_folder + '**/*-measurement.log', recursive=True)
    log_days = set([l[:-20] for l in logs])
    measurements = glob.glob(raw_folder + '**/*.txt', recursive=True)
    empty_days = [day for day in log_days if len([m for m in measurements if m.startswith(day)])==0]
    assert len(empty_days) <= 30 # Known value of empty days
