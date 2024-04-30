# Main

This repository contains the data set and code published together with our paper:
TODO: Add reference to paper

It contains:
* The data set in data/ in feather and csv format
* An overview of the data set with the graphs shown in the paper in overview.ipynb
* Utilities to reconstruct some missing values in util/
* Tests for the data set in tests/test_data.py
* Additional tests we use internaly, which do not work with this repository, but we still provide as a reference here in tests/test_data/internal.py
* The library versions we use to run this code in environment.yml 

Short description of the columns in the data set:
- time: time reindexed to whole seconds
- gpstime: time as returned by gps python module (probably only correct to the nearest second)
- timestamp: system time in milliseconds as set by ntp (hopefully with the help of GPS, see ntp-values)
- lat: latitude from GPS
- long: longitude from GPS
- alt: altitude from GPS
- speed: speed from GPS (in m/s)
- track: course over ground, degrees from true north from GPS
- file: filename of raw data file that is the source of the GPS-based information (even when interpolated)
- line: line number in the position file (interpolated positions do not have line numbers)
- trip: name of first file on row of succeding measurements
- device: device that created the measurement
- Rat: radio access technology
- Numeric: mobile country code + mobile network code (Identifer for the provider)
- State: state returned by wireless module
- FullName: name of the provider
- ShortName: name of the provider
- signal: signal strength in "bars"
- netmode: operating mode
- rsrq: reference signals received quality
- cell_id: identifier of the cell currently connected to
- ecio: downlink carrier-to-interference ratio
- sinr: signal-to-interference-and-noise-ratio (">=30dB" is coded as 42 and "<-20dB"] as -42)
- rscp: received signal code power
- pci: physical cell identifier
- mode: operating mode
- sc: scrambling code
- rssi: received signal strength indicator (Missing for ~40% of measurements, see reconstruct.py for possible reconstruction)
- rsrp: reference signal received power
- ping: round trip latency in ms
- owdDown: One-way-delay from server to mobile device
- owdUp: One-way-delay from mobile device to server
- lossUp_count: Lost packets from mobile device to server (out of 10 per second)
- lossDown_count: Lost packets from server to mobile device (out of 10 per second)
- ntp-GPS-PI_tally: single-character code indicating current value of the select field of the peer status word
- ntp-GPS-PI_remote: host name (or IP number) of peer
- ntp-GPS-PI_refid: ip adress of ntp server used for synchronisation
- ntp-GPS-PI_st: stratum - the devices which are considered independent time sources are classified as stratum 0 sources; the servers directly connected to stratum 0 devices are classified as stratum 1 sources; servers connected to stratum 1 sources are then classified as stratum 2 sources and so on
- ntp-GPS-PI_when: number of seconds passed since last response
- ntp-GPS-PI_poll: poll interval (NTP Client sends NTP packets at intervals ranging from 8s to 36h)
- ntp-GPS-PI_reach: The reach variable is an 8-bit shift register displayed in octal format. When a valid packet is received, the rightmost bit is lit. When a packet is sent, the register is shifted left one bit with 0 replacing the rightmost bit. If the reach value is nonzero, the server is reachable; otherwise, it is unreachable.
- ntp-GPS-PI_delay: round trip delay from GPS-PI to NTP server
- ntp-GPS-PI_offset: time difference between GPS-PI and NTP server
- ntp-GPS-PI_jitter: jitter of ntp sync
- ntp-TP-Core_tally: see above
- ntp-TP-Core_remote: see above
- ntp-TP-Core_refid: see above
- ntp-TP-Core_st: see above
- ntp-TP-Core_when: see above
- ntp-TP-Core_poll: see above
- ntp-TP-Core_reach: see above
- ntp-TP-Core_delay: see above
- ntp-TP-Core_offset: see above
- ntp-TP-Core_jitter: see above
- datarateDown: data rate in bit/s (averaged over one second)
- datarateDown_app: data rate in bit/s (averaged over one second) as seen from downloader application
- download_total_sum: Number of datarate estimates in this interval
- download_connect_sum: Number of downloads connecting (not consistenly used)
- download_starting_sum: Number of downloads starting
- download_done_sum: Number of finished downloads
- download_cannot_sum: Number of failed downloads
- download_timeout_sum: Number of downloads that timed out
- download_progress_sum: Number of progress outputs
- dedicated: Trip was a dedicated measurement trip
- notes: Notes generated while importing data (indicator for problems?)

Acknowledgement: This repository is part of MDI-Lab of Salzburg Research, which is partially funded by the Austrian Federal Ministry of Climate Action, Environment, Energy, Mobility, Innovation and Technology (BMK) and the Austrian state Salzburg.
