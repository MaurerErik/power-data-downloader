import os
import time
from datetime import datetime, timedelta
from power_data_architecture import download_data

# Start timer
start_time = time.time()

####################################################
# General settings
####################################################

# Path to ChromeDriver executable
chrome_driver_filepath = r"<path_to_chromedriver>\chromedriver.exe"

# Root directory for project
root = r"<path_to_project_directory>"

# Backoff time for retries in case of failures
backoff_time = 3

####################################################
# Dayahead auction settings
####################################################

# File paths for dayahead data
dayahead_data_tracking_file_filepath = os.path.join(root, "epex_auction_dayahead", "epex_auction_dayahead_tracking.csv")
dayahead_archive_hourly_data_filepath = os.path.join(root, "epex_auction_dayahead", "epex_auction_dayahead_hourly_data_archive.xlsx")
dayahead_archive_base_peak_data_filepath = os.path.join(root, "epex_auction_dayahead", "epex_auction_dayahead_basepeak_data_archive.xlsx")

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
                "GB_1" : ["GB DAA 1 (60')"],
                "GB_2" : ["GB DAA 2 (30')"]}

dayahead_modality = "Auction"
dayahead_sub_modality = "DayAhead"

####################################################
# Intraday auction settings
####################################################

# File paths for intraday data
intraday_data_tracking_file_filepath = os.path.join(root, "epex_auction_intraday", "epex_auction_intraday_tracking.csv")
intraday_archive_hourly_data_filepath = os.path.join(root, "epex_auction_intraday", "epex_auction_intraday_hourly_data_archive.xlsx")
intraday_archive_base_peak_data_filepath = os.path.join(root, "epex_auction_intraday", "epex_auction_intraday_basepeak_data_archive.xlsx")

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

####################################################
# Dates and downloads
####################################################

# Get current date
today = datetime.today()

# Calculate relevant days for dayahead auction (previous 3 days)
dayahead_relevant_days = [(today - timedelta(days=i), today - timedelta(days=i-1)) for i in range(1, 4)]

# Calculate relevant days for intraday auction (previous 3 days)
intraday_relevant_days = [
    (today - timedelta(days=1), today), 
    (today - timedelta(days=2), today - timedelta(days=1)), 
    (today - timedelta(days=3), today - timedelta(days=2))
]
# Start downloading dayahead auction data
print(f"Starting dayahead auction downloads.")

download_data(
    type="Dayahead",
    market_areas=dayahead_market_areas,
    modality=dayahead_modality,
    sub_modality=dayahead_sub_modality,
    relevant_days=dayahead_relevant_days,
    tracking_file_filepath=dayahead_data_tracking_file_filepath,
    archive_hourly_data_filepath=dayahead_archive_hourly_data_filepath,
    archive_base_peak_data_filepath=dayahead_archive_base_peak_data_filepath,
    chrome_driver_filepath=chrome_driver_filepath,
    backoff_time=backoff_time
)

# Measure time taken for dayahead download
end_time_dayahead = time.time()

print(f"Dayahead auction done. Now starting intraday auction downloads.")

# Start downloading intraday auction data
download_data(
    type="Intraday",
    market_areas=intraday_market_areas,
    modality=intraday_modality,
    sub_modality=intraday_sub_modality,
    relevant_days=intraday_relevant_days,
    tracking_file_filepath=intraday_data_tracking_file_filepath,
    archive_hourly_data_filepath=intraday_archive_hourly_data_filepath,
    archive_base_peak_data_filepath=intraday_archive_base_peak_data_filepath,
    chrome_driver_filepath=chrome_driver_filepath,
    backoff_time=backoff_time
)

# Measure total execution time
end_time = time.time()
execution_time_dayahead = end_time_dayahead - start_time
execution_time_intraday = end_time - end_time_dayahead
execution_time_total = end_time - start_time


# Print execution times
print(f"Dayahead execution time: {execution_time_dayahead / 60:.1f} minutes")
print(f"Intraday execution time: {execution_time_intraday / 60:.1f} minutes")
print(f"Total execution time: {execution_time_total / 60:.1f} minutes")