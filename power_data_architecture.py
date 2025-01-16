import os
import csv
import time
import pandas as pd
from datetime import datetime, timezone
from power_data_utils import import_xlsx, check_archive, extract_soup, extract_hours, extract_last_update, extract_baseload_peakload, extract_volume_and_price_data, clean_df, plausibility_checks

def download_data(type:str,
                  market_areas:dict,
                  modality: str,
                  sub_modality:str,
                  relevant_days:list,
                  tracking_file_filepath:str,
                  archive_hourly_data_filepath:str,
                  archive_base_peak_data_filepath:str,
                  chrome_driver_filepath:str,
                  backoff_time:int):
    """
    Downloads market data from a specified website, processes it, and saves it to Excel archives and tracking files.

    This function iterates through a list of market areas, relevant days, and auction names. For each combination, 
    it checks if the data already exists in the archive files. If not, it downloads the data from a specified website 
    (using Selenium and Chrome), processes it, and appends it to the appropriate Excel files (archive hourly data and 
    base-peak data). Additionally, the function tracks the download status and logs it into a CSV file.

    Steps:
    1. Validates the combination of "type" and filenames to ensure they are consistent.
    2. Iterates over each market area and auction name for the given days.
    3. Checks if the requested data already exists in the archive files; skips if found.
    4. If data is missing, constructs the appropriate URL and uses Selenium to scrape data from the website.
    5. Processes the downloaded data into pandas DataFrames and performs plausibility checks.
    6. Appends valid data to the corresponding Excel archive files (hourly data and base-peak data).
    7. Tracks the success/failure of each download attempt and logs the information into the CSV tracking file.
    8. Waits for the specified "backoff_time" between requests to avoid overloading the website.

    Parameters:
    type (str): The type of data to download (either "Dayahead" or "Intraday") (used for validation).
    market_areas (dict): A dictionary where keys are market areas, and values are lists of auction names.
    modality (str): The trading modality (e.g., Auction).
    sub_modality (str): The sub-modality of the trading data (e.g., technology type).
    relevant_days (list): A list of tuples, each containing a pair of "trading_date" and "delivery_date".
    tracking_file_filepath (str): Path to the CSV file where tracking data will be logged.
    archive_hourly_data_filepath (str): Path to the Excel file where hourly market data will be stored.
    archive_base_peak_data_filepath (str): Path to the Excel file where base-peak market data will be stored.
    chrome_driver_filepath (str): Path to the Chrome WebDriver for Selenium.
    backoff_time (int): Time to wait (in seconds) before attempting to download data for the next entry.

    Returns:
    None

    Example:
    download_data(
        type="Intraday",
        market_areas={"DE": ["SIDC IDA1", "SIDC IDA2"]},
        modality="Auction",
        sub_modality="Technology",
        relevant_days=[(datetime(2023, 5, 1), datetime(2023, 5, 2))],
        tracking_file_filepath="tracking.csv",
        archive_hourly_data_filepath="hourly_data.xlsx",
        archive_base_peak_data_filepath="base_peak_data.xlsx",
        chrome_driver_filepath="chromedriver",
        backoff_time=5
    )
    """
    
    strings_to_check = [type, tracking_file_filepath, archive_hourly_data_filepath, archive_base_peak_data_filepath]
    total_check = len(strings_to_check)
    check_dayahead = sum(1 for string in strings_to_check if "head" in string)
    check_intraday = sum(1 for string in strings_to_check if "traday" in string)

    if (check_dayahead>check_intraday) and (check_dayahead!=total_check):
        raise ValueError("Please provide correct combinations of type and filenames.")
    if (check_intraday>check_dayahead) and (check_intraday!=total_check):
        raise ValueError("Please provide correct combinations of type and filenames.")
    if (check_intraday==check_dayahead):
        raise ValueError("Please provide correct combinations of type and filenames.")
    
    assert (total_check==check_dayahead) or (total_check==check_intraday)

    for date_combination in relevant_days:

        trading_date = date_combination[0]
        delivery_date= date_combination[1]
        trading_date_str = trading_date.strftime("%Y-%m-%d")
        delivery_date_str = delivery_date.strftime("%Y-%m-%d")

        print()
        print()
        print(f"Now working on delivery date {delivery_date_str}. Corresponding trading date is {trading_date_str}.")
        print()
        print()

        # Iterate over specified market areas
        for market_area, auctions in market_areas.items():

            # Iterate over auctions
            for auction_name in auctions:

                num_hours_records = None
                base_peak_sucess = None
                current_utc_time = None

                if os.path.exists(archive_hourly_data_filepath):
                    archive_hourly_data_df = import_xlsx(archive_hourly_data_filepath)
                    archive_hourly_data_df["TradingDate"] = pd.to_datetime(archive_hourly_data_df["TradingDate"]).dt.date
                    archive_hourly_data_df["DeliveryDate"] = pd.to_datetime(archive_hourly_data_df["DeliveryDate"]).dt.date
                else:
                    archive_hourly_data_df = pd.DataFrame()

                if os.path.exists(archive_base_peak_data_filepath):
                    archive_basepeak_data_df = import_xlsx(archive_base_peak_data_filepath)            
                    archive_basepeak_data_df["TradingDate"] = pd.to_datetime(archive_basepeak_data_df["TradingDate"]).dt.date
                    archive_basepeak_data_df["DeliveryDate"] = pd.to_datetime(archive_basepeak_data_df["DeliveryDate"]).dt.date
                else:
                    archive_basepeak_data_df = pd.DataFrame()

                print(f"Now working on {market_area}, {auction_name}, delivery day {delivery_date_str}.")
                
                if "head" in type:
                    if "GB" in market_area:
                        market_area = "GB"

                    auction_name_clean = " "
                    
                    if auction_name=="SDAC":
                        auction_name_clean = "MRC"
                    elif auction_name == "GB DAA 1 (60')":
                        auction_name_clean = "GB"
                    elif auction_name=="GB DAA 2 (30')":
                        auction_name_clean = "30-call-GB"
                    else:
                        auction_name_clean=auction_name

                elif "traday" in type:

                    auction_name_clean = " "

                    if auction_name=="SIDC IDA1":
                        auction_name_clean = "IDA1"
                    elif auction_name == "SIDC IDA2":
                        auction_name_clean = "IDA2"
                    elif auction_name == "SIDC IDA3":
                        auction_name_clean = "IDA3"
                    elif "CH" in auction_name:
                        auction_name_clean = auction_name
                    elif "GB" in auction_name:
                        auction_name_clean = auction_name

                combination_to_check = [market_area, pd.Timestamp(trading_date).date(), pd.Timestamp(delivery_date).date(), modality, sub_modality, auction_name]
                if os.path.exists(archive_hourly_data_filepath):
                    archive_check_hours = check_archive(archive_df=archive_hourly_data_df, combination=combination_to_check, columns_to_check=["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "MarketSegment", "AuctionName"])
                else:
                    archive_check_hours = False
                if os.path.exists(archive_base_peak_data_filepath):
                    archive_check_base_peak = check_archive(archive_df=archive_basepeak_data_df, combination=combination_to_check, columns_to_check=["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "MarketSegment", "AuctionName"])
                else:
                    archive_check_base_peak = False

                # only if both are in the archive the combination can be skipped directly
                if archive_check_hours == True and archive_check_base_peak == True:
                    print(f"{market_area}, {trading_date.date()}, {delivery_date.date()}, {modality}, {sub_modality}, {auction_name} is already in the hours and base peak archives.")
                else: 
                    print(f"{market_area}, {trading_date.date()}, {delivery_date.date()}, {modality}, {sub_modality}, {auction_name} must be added to at least one archive.")
                    print("Starting download ... ", end="")
                    # Assemble the target URL
                    url = f"https://www.epexspot.com/en/market-results?market_area={market_area}&auction={auction_name_clean}&trading_date={trading_date_str}&delivery_date={delivery_date_str}&underlying_year=&modality=Auction&sub_modality={sub_modality}&technology=&data_mode=table&period=&production_period="
                    current_utc_time = datetime.now(timezone.utc)
                    try:
                        soup = extract_soup(chrome_driver_filepath, url)
                        print(f"successful.")
                    except Exception as e:
                        print(e)
                        print(f"failed.")
                        print(f"Website unresponsive for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        print(f"Problem with URL: {url}.")
                        num_hours_records = "Error"
                        base_peak_sucess = "Error"

                    try: 
                        last_update = extract_last_update(soup)
                    except Exception as e:
                        print(e)
                        print(f"Error extracting last update for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"
                        base_peak_sucess = "Error"
                        
                    if len(last_update)==0 or last_update is None:
                        print(f"Error extracting last update for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"
                        base_peak_sucess = "Error"
                        
                    elif len(last_update)<10:
                        print(f"Error extracting last update for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"
                        base_peak_sucess = "Error"

                if archive_check_hours == False and num_hours_records != "Error":
                    # Extract the additional data
                    try:
                        hours = extract_hours(soup)
                    except Exception as e:
                        print(e)
                        print(f"Error extracting hours for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"

                    if (len(hours)==0 and num_hours_records != "Error") or (hours is None and num_hours_records != "Error"):
                        print(f"Error extracting hours for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"

                    elif (len(hours[0])<2 and num_hours_records != "Error"):
                        print(f"Error extracting hours for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"
    
                    try: 
                        hourly_data = extract_volume_and_price_data(soup)
                    except Exception as e:
                        print(e)
                        print(f"Error extracting volume and price data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"

                    if (len(hourly_data)==0 and num_hours_records != "Error") or (hourly_data is None and num_hours_records != "Error"):
                        print(f"Error extracting volume and price data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"
                        
                    elif (len(hourly_data[0])==0 and num_hours_records != "Error") or (hourly_data[0] is None and num_hours_records != "Error"):
                        print(f"Error extracting volume and price data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"
                        
                    elif (len(hourly_data[-1])==0 and num_hours_records != "Error") or (hourly_data[-1] is None and num_hours_records != "Error"):
                        print(f"Error extracting volume and price data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"
                        
                    elif (len(hourly_data[0]) != 4 and num_hours_records != "Error") or (len(hourly_data[-1]) != 4 and num_hours_records != "Error"):
                        print(f"Error extracting volume and price data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}.")
                        num_hours_records = "Error"

                    if num_hours_records != "Error":
                        try:
                            # Create dataframe for hourly data
                            hourly_data_list = []

                            for index, row in enumerate(hourly_data):
                                # Add metadata
                                row_clean = [market_area, trading_date.date(), delivery_date.date(), modality, sub_modality, auction_name, last_update]
                
                                # Add hour
                                hour = hours[index]
                                row_clean.append(hour)

                                # Add obtained actual info
                                for entry in hourly_data[index]:
                                    row_clean.append(entry)
                                
                                # Append clean row
                                hourly_data_list.append(row_clean)

                            # Convert to pandas DataFrame
                            columns_hourly = ["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "MarketSegment", "AuctionName", "LastUpdate", "Hours", "BuyVolume(MWh)", "SellVolume(MWh)", "Volume(MWh)", "Price(€/MWh)"]
                            hourly_data_df = pd.DataFrame(hourly_data_list, columns=columns_hourly)
                            
                            hourly_data_df = clean_df(hourly_data_df)

                        except Exception as e:
                            print(e)
                            print(f"Error extracting data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}, hours case.")
                            num_hours_records = "Error"

                        if (len(hourly_data_df)==0 and num_hours_records != "Error") or (hourly_data_df.empty and num_hours_records != "Error"):
                            print(f"Error extracting data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}, hours case.")
                            num_hours_records = "Error"

                        if plausibility_checks(hourly_data_df) and num_hours_records != "Error":
                            archive_hourly_data_df = pd.concat([archive_hourly_data_df, hourly_data_df], ignore_index=True)
                            archive_hourly_data_df.to_excel(archive_hourly_data_filepath, index=False)
                            num_hours_records = int(len(hourly_data_df))
                            print(f"{market_area}, {trading_date.date()}, {delivery_date.date()}, {modality}, {sub_modality}, {auction_name} added to the hours archive.")
                        else:
                            print(f"One column for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name} contained NaNs only, hours case.")
                            num_hours_records = "Error"
                    
                if archive_check_base_peak == False and base_peak_sucess != "Error":
                    try:
                        base_peak = extract_baseload_peakload(soup)
                        baseload = base_peak[0]
                        peakload = base_peak[1]
                    except Exception as e:
                        print(e)
                        print(f"Error extracting base and peakload values for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}, base peak case.")
                        base_peak_sucess = "Error"
                    if base_peak is None and base_peak_sucess != "Error":
                        print(f"Error extracting base and peakload values for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}, base peak case.")
                        base_peak_sucess = "Error"
                    elif (baseload is None and base_peak_sucess != "Error") or (peakload is None and base_peak_sucess != "Error"):
                        print(f"Error extracting base and peakload values for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}, base peak case.")
                        base_peak_sucess = "Error"
                    
                    if base_peak_sucess != "Error":
                    
                        try:
                            base_peak_data_list = [[market_area, trading_date.date(), delivery_date.date(), modality, sub_modality, auction_name, last_update, baseload, peakload]]

                            # Convert to pandas DataFrame
                            columns_base_peak = ["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "MarketSegment", "AuctionName", "LastUpdate", "Baseload(€/MWh)", "Peakload(€/MWh)"]
                            base_peak_data_df = pd.DataFrame(base_peak_data_list, columns=columns_base_peak)

                            base_peak_data_df = clean_df(base_peak_data_df)
                        except Exception as e:
                            print(e)
                            print(f"Error extracting data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}, base peak case.")
                            base_peak_sucess = "Error"

                        if (len(base_peak_data_df)==0 and base_peak_sucess != "Error") or (base_peak_data_df.empty and base_peak_sucess != "Error"):
                            print(f"Error extracting data for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name}, base peak case.")
                            base_peak_sucess = "Error"

                        if (plausibility_checks(base_peak_data_df) and base_peak_sucess != "Error"):
                            archive_basepeak_data_df = pd.concat([archive_basepeak_data_df, base_peak_data_df], ignore_index=True)
                            archive_basepeak_data_df.to_excel(archive_base_peak_data_filepath, index=False)
                            base_peak_sucess = "Success"
                            print(f"{market_area}, {trading_date.date()}, {delivery_date.date()}, {modality}, {sub_modality}, {auction_name} added to the base peak archive.")
                        else:
                            print(f"One column for {market_area}, {trading_date_str}, {delivery_date_str}, {modality}, {sub_modality}, {auction_name} contained NaNs only, base peak case.")
                            base_peak_sucess = "Error"
                
                if (archive_check_hours == False) or (archive_check_base_peak == False):
                    tracking_data = [
                        market_area,
                        trading_date_str,
                        delivery_date_str,
                        modality,
                        sub_modality,
                        auction_name,
                        current_utc_time.isoformat(),
                        str(num_hours_records),
                        str(base_peak_sucess)
                        ]

                    # Write to CSV
                    file_exists = os.path.isfile(tracking_file_filepath)

                    if file_exists:
                        # Read the existing content of the CSV file
                        with open(tracking_file_filepath, mode='r', newline='') as file:
                            reader = csv.reader(file)
                            rows = list(reader)

                        header = rows[0]
                        data_rows = rows[1:]

                        # Insert the new row after the header
                        data_rows.insert(0, tracking_data)

                        # Write the updated content back to the CSV file
                        with open(tracking_file_filepath, mode='w', newline='') as file:
                            writer = csv.writer(file)
                            writer.writerow(header)  # Write the header first
                            writer.writerows(data_rows)  # Write the modified data rows
                    else:

                        with open(tracking_file_filepath, mode='a', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)

                            writer.writerow([
                                "MarketArea",
                                "TradingDate",
                                "DeliveryDate",
                                "Modality",
                                "SubModality",
                                "AuctionName",
                                "WebsiteAccessTimeUTC",
                                "NumberHourlyEntries",
                                "BasePeakSuccess"
                            ])

                            # Write the data rows
                            writer.writerow(tracking_data)

                    print(f"Tracking data saved in csv file.")
                    
                print()
                time.sleep(backoff_time)