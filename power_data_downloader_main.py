import os
import time
from datetime import datetime, timedelta
from power_data_downloader_architecture import download

# Start timer
start_time = time.time()

########################################################################################################
# General settings 1 ## MODIFY THESE ##
########################################################################################################

# Path to ChromeDriver executable
chrome_driver_filepath = r"<path_to_chromedriver>\chromedriver.exe"

# Root directory for project
root = r"<path_to_project_directory>"

########################################################################################################
# General settings 2
########################################################################################################

# Backoff time for retries in case of failures
backoff_time = 0.8

########################################################################################################
# Dayahead auction settings
########################################################################################################

# File paths for dayahead data
dayahead_tracking_file_filepath = os.path.join(root, "epex_dayahead_tracking.csv")
dayahead_market_areas = {"AT" : ["SDAC"],
                "BE" : ["SDAC"],
                "DE-LU" : ["SDAC"],
                "DK1" : ["SDAC"],
                "DK2" : ["SDAC"],
                "FI" : ["SDAC"],
                "FR" : ["SDAC"],
                "NL" : ["SDAC"],
                "NO1" : ["SDAC"],
                "NO2" : ["SDAC"],
                "NO3" : ["SDAC"],
                "NO4" : ["SDAC"],
                "NO5" : ["SDAC"], 
                "PL" : ["SDAC"],
                "SE1" : ["SDAC"],
                "SE2" : ["SDAC"],
                "SE3" : ["SDAC"],
                "SE4" : ["SDAC"], 
                "CH" : ["CH"],
                "GB" : ["GB DAA 1 (60')", "GB DAA 2 (30')"]
                }
dayahead_modality = "Auction"
dayahead_sub_modality = "DayAhead"

########################################################################################################
# Intraday auction settings
########################################################################################################

# File paths for intraday data
intraday_tracking_file_filepath = os.path.join(root, "epex_intraday_tracking.csv")
intraday_market_areas = {"AT":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "BE":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "DE-LU":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "DK1":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "DK2":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "FI":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "FR":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "NL":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "NO1":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "NO2":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "NO3":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "NO4":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "NO5":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "PL":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "SE1":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "SE2":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "SE3":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"],
                "SE4":["SIDC IDA1", "SIDC IDA2", "SIDC IDA3"], 
                "CH":["CH-IDA1", "CH-IDA2"],
                "GB":["GB-IDA1", "GB-IDA2"]
                }
intraday_modality = "Auction"
intraday_sub_modality = "Intraday"

########################################################################################################
# Continuous trading settings
########################################################################################################

# File paths for continuous trading data
continuous_tracking_file_filepath = os.path.join(root, "epex_continuous_tracking.csv")
continuous_market_areas = {"AT": [60, 15],
                "BE": [60, 30, 15],
                "DE": [60, 30, 15],
                "DK1": [60, 15],                
                "DK2": [60, 15],
                "FI": [60, 15],
                "FR": [60, 30],
                "NL": [60, 30, 15],
                "NO1": [60],
                "NO2": [60],
                "NO3": [60],
                "NO4": [60],
                "NO5": [60],
                "PL": [60, 15],
                "SE1": [60, 15],
                "SE2": [60, 15],
                "SE3": [60, 15],
                "SE4": [60, 15], 
                "CH": [60, 30, 15],
                "GB": [30]
                }
continuous_modality = "Continuous"
continuous_sub_modality = "Intraday"

########################################################################################################
# Aggregated curve settings
########################################################################################################

# File paths for aggregated curves data
aggregated_curves_tracking_file_filepath = os.path.join(root, "epex_aggregated_curves_tracking.csv")
# The other settings can be taken from dayahead and intraday settings

########################################################################################################
# Dates
########################################################################################################

# Get current date
today = datetime.today()

# Calculate relevant days for dayahead auction (previous 3 days)
dayahead_relevant_days = [(today, today + timedelta(days=1)), 
    (today - timedelta(days=1), today), 
    (today - timedelta(days=2), today - timedelta(days=1))
]

# Relevant days for intraday auction (previous 3 days)
intraday_relevant_days = [
    (today - timedelta(days=1), today), 
    (today - timedelta(days=2), today - timedelta(days=1)), 
    (today - timedelta(days=3), today - timedelta(days=2))
]

# Relevant days for continous trading (previous 2 days)
yesterday = today - timedelta(days=1)
day_before_yesterday = today - timedelta(days=2)
continuous_relevant_days = [(yesterday, yesterday), (day_before_yesterday, day_before_yesterday)]

########################################################################################################
# Downloads
########################################################################################################

dayahead_start_time = time.time()

# Start downloading dayahead auction data
print(f"Starting dayahead auction downloads.")

dayahead_total_errors = download(
    type="dayahead",
    tracking_file_filepath=dayahead_tracking_file_filepath,
    date_combinations=dayahead_relevant_days,
    market_areas=dayahead_market_areas,
    trading_modality=dayahead_modality,
    sub_modality=dayahead_sub_modality,
    chromedriver_filepath=chrome_driver_filepath,
    folder_path=root,
    backoff_time=backoff_time
)

# Measure time taken for dayahead download
dayahead_end_time = time.time()

print(f"Dayahead auction done. Now starting intraday auction downloads.")

# Start downloading intraday auction data
intraday_total_errors = download(
    type="intraday",
    tracking_file_filepath=intraday_tracking_file_filepath,
    date_combinations=intraday_relevant_days,
    market_areas=intraday_market_areas,
    trading_modality=intraday_modality,
    sub_modality=intraday_sub_modality,
    chromedriver_filepath=chrome_driver_filepath,
    folder_path=root,
    backoff_time=backoff_time
)

# Measure time taken for intraday download
intraday_end_time = time.time()

print(f"Intraday auction done. Now starting continuous trading downloads.")

# Start downloading continuous trading data
continuous_total_errors = download(
    type="continuous",
    tracking_file_filepath=continuous_tracking_file_filepath,
    date_combinations=continuous_relevant_days,
    market_areas=continuous_market_areas,
    trading_modality=continuous_modality,
    sub_modality=continuous_sub_modality,
    chromedriver_filepath=chrome_driver_filepath,
    folder_path=root,
    backoff_time=backoff_time
)

# Measure time taken for continuous download
continuous_end_time = time.time()

print(f"Continuous trading done. Now starting aggregated curves downloads.")

# Start downloading aggregated curves data
aggregated_curves_dayahead_total_errors = download(
    type="aggregated_curves_dayahead",
    tracking_file_filepath=aggregated_curves_tracking_file_filepath,
    date_combinations=dayahead_relevant_days,
    market_areas=dayahead_market_areas,
    trading_modality=dayahead_modality,
    sub_modality=dayahead_sub_modality,
    chromedriver_filepath=chrome_driver_filepath,
    folder_path=root,
    backoff_time=backoff_time
)

aggregated_curves_intraday_total_errors = download(
    type="aggregated_curves_intraday",
    tracking_file_filepath=aggregated_curves_tracking_file_filepath,
    date_combinations=intraday_relevant_days,
    market_areas=intraday_market_areas,
    trading_modality=intraday_modality,
    sub_modality=intraday_sub_modality,
    chromedriver_filepath=chrome_driver_filepath,
    folder_path=root,
    backoff_time=backoff_time
)

# Measure time taken for aggregated curves downloads
aggregated_curves_end_time = time.time()

print(f"Aggregated curves done.")

########################################################################################################
# Final statements
########################################################################################################

dayahead_execution_time = dayahead_end_time - dayahead_start_time
intraday_execution_time = intraday_end_time - dayahead_end_time
continuous_execution_time = continuous_end_time - intraday_end_time
aggregated_curves_execution_time = aggregated_curves_end_time - continuous_end_time

# Print execution times and error count
print()
print(f"Dayahead execution time: {dayahead_execution_time / 60:.1f} minutes. Total errors: {dayahead_total_errors}.")
print(f"Intraday execution time: {intraday_execution_time / 60:.1f} minutes. Total errors: {intraday_total_errors}.")
print(f"Continuous execution time: {continuous_execution_time / 60:.1f} minutes. Total errors: {continuous_total_errors}.")
print(f"Aggregated curves execution time: {aggregated_curves_execution_time / 60:.1f} minutes. Total errors: {aggregated_curves_dayahead_total_errors+aggregated_curves_intraday_total_errors}.")
print()

end_time = time.time()
total_execution_time = end_time - start_time

if ((total_execution_time/60)<60):
    print(f"Total execution time: {total_execution_time / 60:.1f} minutes.")
else: 
    total_execution_time_minutes = (total_execution_time/60)
    total_execution_time_hours = total_execution_time_minutes//60
    total_execution_time_minutes_residual = total_execution_time_minutes - 60*total_execution_time_hours
    print(f"Total execution time: {total_execution_time_hours} hours {total_execution_time_minutes_residual} minutes.")