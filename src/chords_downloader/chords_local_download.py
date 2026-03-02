import requests
import warnings
from json import dumps
from json import loads
import numpy as np
from datetime import datetime, timedelta
import argparse
from chords_downloader import resources
from pathlib import Path

def main(portal_url:str, portal_name:str, data_path:Path, instrument_IDs:list, user_email:str, 
         api_key:str, start:str, end:str, fill_empty='', include_test:bool=False, columns_desired:list=[], 
         time_window_start:str='', time_window_end:str=''):
 
    # User input validation -----------------------------------------------------------------------------------------------------------
    format_str = "%Y-%m-%d %H:%M:%S"
    timestamp_start = datetime.strptime(start, format_str) 
    timestamp_end = datetime.strptime(end, format_str)
    if timestamp_start > timestamp_end:
            raise ValueError(f"Starting time cannot be after end time.\n\t\t\tStart: {timestamp_start}\t\tEnd: {timestamp_end}")
    if (timestamp_start < datetime.now() - timedelta(days=365*2)):
            warnings.warn(
                f"[WARNING]: timestamp_start before CHORDS cutoff (2 years): {timestamp_start}\n\t Will pull 2 year archive only.\n"
            )
    if (timestamp_end > datetime.now()) or (timestamp_start > datetime.now()):
            warnings.warn(
                f"[WARNING]: timestamp_start or timestamp_end in the future: {timestamp_start}\t{timestamp_end}\n\t Will pull up to today's date only.\n"
            )

    # Determine whether a time window was requested. Both start AND end must be provided together.
    # Initialize window timestamps to None so they are always defined for the processing loop below.
    timestamp_window_start = None
    timestamp_window_end = None
    use_time_window = False  # flag that drives the branching logic below

    if time_window_start != "" and time_window_end != "":
        # Both endpoints were supplied — parse and validate them.
        format_str_window = "%H:%M:%S"
        timestamp_window_start = datetime.strptime(time_window_start, format_str_window).time()
        timestamp_window_end   = datetime.strptime(time_window_end,   format_str_window).time()
        # Compare the parsed time objects, NOT the raw strings (avoids lexicographic edge cases).
        if timestamp_window_start > timestamp_window_end:
            raise ValueError(
                f"The start time for the time window is after the end time: "
                f"{time_window_start} > {time_window_end}"
            )
        use_time_window = True
    elif time_window_start != "" or time_window_end != "":
        # Only one of the two was provided — that is ambiguous, so raise early.
        raise ValueError(
            "Both -time_window_start and -time_window_end must be provided together. "
            f"Received: time_window_start='{time_window_start}', time_window_end='{time_window_end}'"
        )

    from chords_downloader.resources.functions import PORTAL_LOOKUP 
    if portal_name.lower() not in PORTAL_LOOKUP:
        raise ValueError(f"{portal_name} not found. Supported CHORDS portals include:\n{PORTAL_LOOKUP}")
    
    # Processing loop ------------------------------------------------------------------------------------------------------------------
    for iD in instrument_IDs:
        if not isinstance(iD, int):
            raise TypeError(f"The instrument id's must be integers, passed {type(iD)} for id {iD}")

        print(f"---> Reading instrument ID {iD}\t\t\t\t\t\t\t{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if not use_time_window:
            # ---- Standard path: download full date range, no time-of-day filtering ----
            time         = []  # list of strings  (e.g. '2023-12-17T00:00:04Z')
            measurements = []  # list of dicts    (e.g. {'t1': 25.3, 'rh1': 92.7, ...})
            test         = []  # list of strings  ('true' or 'false')

            total_num_measurements = 0
            total_num_timestamps   = 0

            url = f"{portal_url}/api/v1/data/{iD}?start={start}&end={end}&email={user_email}&api_key={api_key}"
            response = requests.get(url=url)
            if resources.has_errors(response, portal_name, iD):
                continue

            # Deep copy of JSON-formatted CHORDS response
            all_fields = loads(dumps(response.json()))
            
            if resources.has_excess_datapoints(all_fields):
                # Response hit the 200 000-observation API cap — chunk it into smaller calls.
                print("\t Large data request -- reducing.")
                reduced_data = resources.reduce_datapoints(
                    all_fields['errors'][0], int(iD), timestamp_start, timestamp_end,
                    portal_url, user_email, api_key, fill_empty
                )   # returns [time, measurements, test, total_num_measurements]
                time                   = reduced_data[0]
                measurements           = reduced_data[1]
                test                   = reduced_data[2]
                total_num_measurements = reduced_data[3]
            else:
                # Response is within limits — parse it directly.
                data = all_fields['features'][0]['properties']['data']
                # data is a list of dicts: {'time': '...Z', 'test': 'false', 'measurements': {...}}
                for i in range(len(data)):
                    time.append(str(data[i]['time']))
                    total_num_measurements += len(data[i]['measurements'].keys())
                    total_num_timestamps   += 1
                    to_append = resources.write_compass_direction(dict(data[i]['measurements']), fill_empty)
                    measurements.append(to_append)
                    test.append(str(data[i]['test']))

        else:
            # ---- Time-window path: return only observations within a daily HH:MM:SS window ----
            # resources.time_window() handles both the API chunking AND the hourly filtering.
            print(f"\t\t Time window specified.\n\t\t Returning data from {time_window_start} -> {time_window_end}")
            window_data = resources.time_window(
                int(iD),
                timestamp_start, timestamp_end,               # full date range bounds
                timestamp_window_start, timestamp_window_end,  # daily time-of-day filter
                portal_url, user_email, api_key,
                portal_name, data_path, fill_empty
            )   # returns [time, measurements, test, total_num_measurements]
            time                   = window_data[0]
            measurements           = window_data[1]
            test                   = window_data[2]
            total_num_measurements = window_data[3]

        # ---- Build output regardless of which path was taken ----
        headers      = resources.build_headers(measurements, columns_desired, include_test, portal_name)
        time         = np.array(time)
        measurements = np.array(measurements)
        test         = np.array(test)
        
        if resources.struct_has_data(measurements, time, test): 
            file_path = data_path / f"{portal_name}_Instrument-{iD}_{timestamp_start.date()}_{timestamp_end.date()}.csv"
            resources.csv_builder(headers, time, measurements, test, file_path, include_test, fill_empty)
            print(f"\t Finished writing to file.\t\t\t\t\t\t{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"\t Total number of measurements: {total_num_measurements}")
        else:
            warnings.warn(
                f"[WARNING]: No data found at specified timeframe for {portal_name} Instrument ID: {iD}\n"
            )
            file_path = data_path / f"{portal_name}_Instrument-{iD}_[WARNING].txt"
            with open(file_path, 'w') as file:
                file.write("No data was found for the specified time frame.\nCheck the CHORDS portal to verify.")


def parse_args() -> tuple[str, str, Path, list[int], str, str, str, str]:
    parser = argparse.ArgumentParser(
        description="Process API user parameters: portal_url, portal_name, data_path, instrument_IDs, user_email, api_key, start, and end."
    )

    parser.add_argument("portal_url",           type=str,   help="The url for the CHORDS online portal.")
    parser.add_argument("portal_name",          type=str,   help="The name of the CHORDS portal.")
    parser.add_argument("data_path",            type=Path,  help="The folder path to local storage where csv's will be exported.")
    parser.add_argument("instrument_IDs",       type=Path,  help="All the instruments to download data from. Use the Instrument Id from CHORDS portal.")
    parser.add_argument("user_email",           type=str,   help="The email login information in order to access the CHORDS online portal.")
    parser.add_argument("api_key",              type=str,   help="The API key which corresponds to the user's email address.")
    parser.add_argument("start",                type=str,   help="The timestamp from which to start downloading data (MUST be in the following format: YYYY-MM-DD HH:MM:SS).")
    parser.add_argument("end",                  type=str,   help="The timestamp at which to stop downloading data (MUST be in the following format: YYYY-MM-DD HH:MM:SS).")
    parser.add_argument("-fill_empty",                      help="Enter whatever value should be used to signal no data (e.g. -999.99 or 'NaN'). Empty string by default (creates smaller files).")
    parser.add_argument("-include_test",        type=bool,  help="Set to True to include boolean columns next to each data column which specify whether data collected was test data (False by default). ")
    parser.add_argument("-columns_desired",     type=list,  help="Enter the shortnames for the columns to include in csv (e.g. ['t1', 't2', 't3']). Includes all if left blank.")
    parser.add_argument("-time_window_start",   type=str,   default="", help="Timestamp from which to collect subset of data (MUST be in the following format: 'HH:MM:SS'). Includes all timestamps if left blank.")
    parser.add_argument("-time_window_end",     type=str,   default="", help="Timestamp from which to stop collecting subset of data (MUST be in the following format: 'HH:MM:SS'). Includes all timestamps if left blank.")

    args = parser.parse_args()
    return (
        args.portal_url, 
        args.portal_name, 
        args.data_path,
        args.instrument_IDs, 
        args.user_email, 
        args.api_key, 
        args.start, 
        args.end, 
        args.fill_empty, 
        args.include_test,
        args.columns_desired,
        args.time_window_start,
        args.time_window_end
    )


if __name__ == "__main__":
    main(*parse_args())
