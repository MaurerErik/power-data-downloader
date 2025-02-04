# Power Data Downloader

This tool automates the downloading, processing and archiving of dayahead, intraday and continuous trading power market data for all market areas from the EPEX (European Power Exchange, https://www.epexspot.com/en), the platform where power trading takes place across European markets such as Germany, France and the Nordics. **It is meant to be scheduled daily at 6PM Berlin time** but to ensure redundancy in case of issues like bad connections it can also be scheduled to run multiple times (e.g., 6 PM, 7 PM, 8 PM). As of January 2025, EPEX provides data up to three days in the past, so the process can still work if a scheduled run was aborted on a given day. The system automatically detects whether an observation is missing in the archive or whether the record already exists. The tool uses Python's "selenium" for browser automation, "beautifulsoup" for efficiently navigating the html content, "pandas" for data manipulation and custom utilities for handling data downloads, plausibility checks and cleaning.

## As of:

- Last updated: **February 4, 2025, 19:47 Berlin time**, accessing the EPEX website now works headless which is useful for environments without a graphical interface like servers or cloud-based virtual machines, some options to improve stability in resource-constrained environments were added  
- Previous update: January 30, 2025, 11:12 Berlin time, updated continuous trading data collection for the Great Britain market area

## Features

- **Dayahead Market Data**: Downloads and processes dayahead auction **table and aggregated curves data** from the official EPEX Spot website for specified market areas.
- **Intraday Market Data**: Downloads and processes intraday auction **table and aggregated curves data** from the official EPEX Spot website for specified market areas.
- **Continuous Trading Market Data**: Downloads and processes continuous trading **table data** from the official EPEX Spot website for specified market areas.
- **Data Archiving**: Automatically updates tracking files to allow monitoring of download success and archives data into pre-defined csv files.
- **Error Handling**: Implements backoff time to avoid overloading the EPEX server.
- **Extensibility**: Modular structure with reusable utility functions for data operations.

---

## Prerequisites

### Python Version
This project requires **Python 3.12 or higher**.

### Libraries and Dependencies
The required Python packages are listed in "requirements.txt". Install them using: """bash pip install -r requirements.txt"""

### ChromeDriver
The script uses Selenium WebDriver with ChromeDriver. Download the appropriate version of ChromeDriver for your system from the official website and update the path in the script:  
chrome_driver_filepath = r"<path_to_chromedriver>\chromedriver.exe"

---

## Example Directory Structure
project-root/  
│  
├── AT/  
│   ├── epex_AT_aggregated_curves_dayahead_archive.csv  
│   ├── epex_AT_aggregated_curves_intraday_archive.csv  
│   ├── epex_AT_continuous_archive.csv  
│   ├── epex_AT_dayahead_base_peak_archive.csv  
│   ├── epex_AT_dayahead_hours_archive.csv  
│   ├── epex_AT_intraday_base_peak_archive.csv  
│   └── epex_AT_intraday_hours_archive.csv  
│  
├── BE/  
│   ├── epex_BE_aggregated_curves_dayahead_archive.csv  
│   ├── epex_BE_aggregated_curves_intraday_archive.csv  
│   ├── epex_BE_continuous_archive.csv  
│   ├── epex_BE_dayahead_base_peak_archive.csv  
│   ├── epex_BE_dayahead_hours_archive.csv  
│   ├── epex_BE_intraday_base_peak_archive.csv  
│   └── epex_BE_intraday_hours_archive.csv  
│  
.  
.  
.  
├── epex_aggregated_curves_tracking.csv  
├── epex_continuous_tracking.csv  
├── epex_dayahead_tracking.csv  
├── epex_intraday_tracking.csv  
│  
├── power_data_downloader_main.py  
├── power_data_downloader_architecture.py  
└── power_data_downloader_utils.py  

---

## Configuration

### General Settings

Update the script with your local paths:

- **Chromedriver Path**: Replace `<path_to_chromedriver>` with the location of your ChromeDriver executable.
- **Project Directory**: Replace `<path_to_project_directory>` with the root directory where you want to store the tracking files. In this location, market area specific folders will be created. In these, market area specific data will be stored in archive CSV files.

Example:  
chrome_driver_filepath = r"C:/Users/YourName/Downloads/chromedriver.exe"  
root = r"C:/Users/YourName/Projects/PowerDataDownloader/"

### Backoff Time
Backoff Time: Adjust the backoff time (in seconds) for downloads:

backoff_time = 3

### Market Areas
The tool supports multiple market areas for dayahead and intraday auctions as well as the continuous trading segments. These areas and modalities are defined in dictionaries in the power_data_downloader_main.py script:

Dayahead Market Areas:
dayahead_market_areas = {
    "AT": ["SDAC"], "BE": ["SDAC"], ... "GB_1": ["GB DAA 1 (60')"], "GB_2": ["GB DAA 2 (30')"]
}

Intraday Market Areas:
intraday_market_areas = {
    "AT": ["SIDC IDA1", "SIDC IDA2", ...], "GB": ["GB-IDA1", "GB-IDA2"]
}

Continuous Trading Market Areas:
continuous_market_areas = {
    "AT": [60, 15], "BE": [60, 30, 15]
}

---

## Usage

### Running the Script

Ensure that all prerequisites are met and the configuration is updated.

Execute the script:
python power_data_downloader_main.py

The script will:
1. Download dayahead auction data.
2. Process and archive the dayahead auction data.
3. Download intraday auction data.
4. Process and archive the intraday auction data.
5. Download the continuous trading data.
6. Process and archive continuous trading the data.
7. Download the dayahead aggregated curves data.
8. Process and archive the dayahead aggregated curves data.
9. Download the intraday aggregated curves data.
10. Process and archive the intraday aggregated curves data.

Outputs
Tracking Files: Updated .csv files tracking the status of downloads.
Archived Data: Updated .csv files containing processed data.

Logs
Current Market Area, Auction, success information as well as execution times for each process are displayed in the console.

---

## Dependencies

-pandas (1.3.5 or later)  
-beautifulsoup4 (4.10.0 or later)  
-selenium (4.1.0 or later)  

Install dependencies with:

pip install -r requirements.txt

---

## Additional Notes

-Redundancy: Running the script multiple times can help prevent data loss due to failed attempts. For example, scheduling the script at 6 PM, 7 PM and 8 PM Berlin time adds redundancy in case of network issues.  
-Data Archiving: The project automatically detects previously downloaded data to prevent duplication in archives. It also automatically detects previously failed download or data processing attempts and retries to download these cases in case it is run again. 