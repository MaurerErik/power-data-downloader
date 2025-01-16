# Power Data Downloader

This project automates the downloading, processing, and archiving of dayahead and intraday power market data for various market areas from the EPEX (European Power Exchange, https://www.epexspot.com/en), the platform where power trading takes place across European markets such as Germany, France, and the Nordics. **It is meant to be scheduled daily at 6PM Berlin time** but it can also be scheduled to run multiple times (e.g., 6 PM, 7 PM, 8 PM) to ensure redundancy in case of issues like bad connections. As of January 2025, EPEX provides data up to three days in the past, so the process can still work if not run on a given day. The system automatically detects whether an observation is already in the archive. This project uses Python's "selenium" for browser automation, "pandas" for data manipulation, and custom utilities for handling data downloads, plausibility checks, and cleaning.

## As of:

- Last updated: **January 16, 2025, 14:57 Berlin time**

## Features

- **Dayahead Market Data**: Downloads and processes dayahead auction data from the official EPEX Spot website for specified market areas.
- **Intraday Market Data**: Downloads and processes intraday auction data from the official EPEX Spot website for specified market areas.
- **Data Archiving**: Automatically updates tracking files to allow monitoring of download success and archives data into pre-defined Excel files.
- **Error Handling**: Implements backoff time to avoid overloading the server.
- **Extensibility**: Modular structure with reusable utility functions for data operations.

---

## Prerequisites

### Python Version
This project requires **Python 3.12 or higher**.

### Libraries and Dependencies
The required Python packages are listed in "requirements.txt". Install them using the bash command:  
"""pip install -r requirements.txt"""

### ChromeDriver
The script uses Selenium WebDriver with ChromeDriver. Download the appropriate version of ChromeDriver for your system from the official website and update the path in the script:  
chrome_driver_filepath = r"<path_to_chromedriver>\chromedriver.exe"

---

## Directory Structure
project-root/  
│  
├── epex_auction_dayahead/  
│   ├── epex_auction_dayahead_tracking.csv  
│   ├── epex_auction_dayahead_hourly_data_archive.xlsx  
│   └── epex_auction_dayahead_basepeak_data_archive.xlsx  
│  
├── epex_auction_intraday/  
│   ├── epex_auction_intraday_tracking.csv  
│   ├── epex_auction_intraday_hourly_data_archive.xlsx  
│   └── epex_auction_intraday_basepeak_data_archive.xlsx  
│  
├── power_data_architecture.py  
├── power_data_utils.py  
└── main_script.py  

---

## Configuration

### General Settings
Update the script with your local paths:

- **Chromedriver Path**: Replace `<path_to_chromedriver>` with the location of your ChromeDriver executable.
- **Project Directory**: Replace `<path_to_project_directory>` with the root directory where you want to store tracking and archive files.

Example:  
chrome_driver_filepath = r"C:/Users/YourName/Downloads/chromedriver.exe"  
root = r"C:/Users/YourName/Projects/PowerDataDownloader/"

### Backoff Time
Backoff Time: Adjust the backoff time (in seconds) for downloads:  
backoff_time = 3

### Market Areas
The script supports multiple market areas for both dayahead and intraday auctions. These areas and modalities are defined in dictionaries:

Dayahead Market Areas:  
dayahead_market_areas = {  
    "AT": ["SDAC"], "BE": ["SDAC"], ... "GB_1": ["GB DAA 1 (60')"], "GB_2": ["GB DAA 2 (30')"]  
    }

Intraday Market Areas:  
intraday_market_areas = {  
    "AT": ["SIDC IDA1", "SIDC IDA2", ...], "GB": ["GB-IDA1", "GB-IDA2"]  
    }

---

## Usage

### Running the Script
Ensure that all prerequisites are met and the configuration is updated.

Execute the script:  
python main_script.py

The script will:  
1. Download dayahead auction data.  
2. Process and archive the data.  
3. Download intraday auction data.  
4. Process and archive the data.  

Outputs:  
Tracking Files: Updated .csv files tracking the status of downloads.  
Archived Data: Updated .xlsx files containing processed data.  

Logs:  
Current Market Area, Auction, success information as well as execution times for each process are displayed in the console.

---

## Dependencies
-pandas (2.2.2 or later)  
-beautifulsoup4 (4.12.3 or later)  
-selenium (4.27.1 or later)  

Install dependencies with:  
pip install -r requirements.txt

---

## Additional Notes
-Redundancy: Running the script multiple times can help prevent data loss due to failed attempts. For example, scheduling the script at 6 PM, 7 PM, and 8 PM Berlin time adds redundancy in case of network issues.  
-Data Archiving: The project automatically detects previously downloaded data to prevent duplication in archives.  