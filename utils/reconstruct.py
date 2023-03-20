"""
Utility functions to reconstruct data
"""

def reconstruct_rssi(df):
    """Reconstruct RSSI from RSRQ, RSRP and RSSI"""

    # Estimate Number of Physical Ressource Blocks (PRB)
    N = df['rsrq'] - df['rsrp'] + df['rssi']
    assert 16 < N.mean() < 20
    assert N.std() < 4

    # Reconstruct RSSI from #PRB, RSRQ and RSRP
    df['rssiR'] = N.median() - df['rsrq'] + df['rsrp']
    assert (df['rssiR'] % 1).max() == 0
    assert df[['rssi', 'rssiR']].corr().iloc[0,1] > 0.89

    # Join Reconstructed values with measurements to imputed values
    df['rssiI'] = df['rssi'].where(df['rssi'].notna(), df['rssiR'])

    return df
