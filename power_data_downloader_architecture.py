import os
import json
import time
import pandas as pd
from datetime import datetime, timezone
from power_data_downloader_utils import import_exsting_combinations_file, check_existing_combinations, extract_soup, extract_soup_aggregated_curves, extract_last_update, extract_hours, extract_volume_and_price_data, extract_baseload_peakload, extract_hours_continous, extract_volume_and_price_data_continuous, clean_df, import_csv, update_tracking_file

def download(type:str,
            tracking_file_filepath:str,
            date_combinations:list,
            market_areas:dict,
            trading_modality:str,
            sub_modality:str,
            chromedriver_filepath:str,
            folder_path:str,
            backoff_time:int) -> int:
    """
    For a given type (either "dayahead" or "intraday" or "continuous" or "aggregated_curves_dayahead" or
    "aggregated_curves_intraday"), date combinations, market areas, trading modality and sub modality,
    download the corresponding table or aggregated curves data from the EPEX Spot website (https://www.epexspot.com).
    Downloaded data is processed and saved to archive CSV files. Files are stored under the respective
    market areas. Download successes are tracked and stored as overviews in the respective tracking files.
    Returns the total error count for documentation purposes.

    Parameters:
    type (str): The type of data to download (either "dayahead" or "intraday" or "continuous" or 
    "aggregated_curves_dayahead" or "aggregated_curves_intraday").
    tracking_file_filepath (str): Path to the CSV file where tracking data will be logged.
    date_combinations (list): A list of tuples, each containing a pair of "trading_date" and "delivery_date".
    market_areas (dict): A dictionary where keys are market areas and values are lists of auction names (in the
    case of type dayahead or intraday or aggregated
    curves) or products (in the case of continuous).
    trading_modality (str): The trading modality (e.g., Auction).
    sub_modality (str): The sub-modality of the trading data (e.g., Intraday).
    chromedriver_filepath (str): Path to the Chrome WebDriver for Selenium.    
    folder_path (str): Path to the folder where the data will be stored. Market area specific folders
    will be created at this location.
    backoff_time (int): Time to wait (in seconds) before attempting to download data for the next entry.

    Returns:
    (int) : The total error count.

    Example:
    download_data(
        type="intraday",
        tracking_file_filepath="intraday_tracking.csv",
        date_combinations=[(datetime(2023, 5, 1), datetime(2023, 5, 2))],
        market_areas={"DE": ["SIDC IDA1", "SIDC IDA2"]},
        trading_modality="Auction",
        sub_modality="Technology",
        chromedriver_filepath="root/chromedriver.exe",
        folder_path="epex_data",
        backoff_time=1
    )
    """

    # First, check if there is a tracking file
    existing_combinations_file = os.path.exists(tracking_file_filepath)
    
    # If yes, import the file
    if existing_combinations_file:
        if (type=="continuous"):
            columns_to_check=["MarketArea", "DeliveryDate", "TradingModality", "Product(min)"]
            exsting_combinations = import_exsting_combinations_file(tracking_file_filepath, columns_to_check)
        else:
            columns_to_check=["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "SubModality", "AuctionName"]
            exsting_combinations = import_exsting_combinations_file(tracking_file_filepath, columns_to_check)
            exsting_combinations["TradingDate"] = pd.to_datetime(exsting_combinations["TradingDate"]).dt.date

    # Track number of errors
    total_error_count = 0

    # Iterate backwards such that the earliest date is done first
    for date_combination in date_combinations[::-1]:

        # Extract the relevant days
        trading_date = date_combination[0]
        delivery_date= date_combination[1]
        trading_date_str = trading_date.strftime("%Y-%m-%d")
        delivery_date_str = delivery_date.strftime("%Y-%m-%d")

        print()
        print()
        print(f"Now working on delivery date {delivery_date_str}.")
        print()
        print()

        # Iterate over specified market areas
        for market_area, values in market_areas.items():

            # Iterate over auctions
            for value in values:

                print(f"Now working on {market_area}, {value}, delivery day {delivery_date_str}.")

                success_indicator = None
                current_utc_time = None

                # In the relevant cases, clean market area and auction information
                if (type=="dayahead") or (type=="aggregated_curves_dayahead"):

                    if ("GB" in market_area):
                        market_area = "GB"

                    value_clean = " "
                    
                    if (value=="SDAC"):
                        value_clean = "MRC"
                    elif (value == "GB DAA 1 (60')"):
                        value_clean = "GB"
                    elif (value=="GB DAA 2 (30')"):
                        value_clean = "30-call-GB"
                    else:
                        value_clean=value

                elif (type=="intraday") or (type=="aggregated_curves_intraday"):

                    value_clean = " "

                    if (value=="SIDC IDA1"):
                        value_clean = "IDA1"
                    elif (value == "SIDC IDA2"):
                        value_clean = "IDA2"
                    elif (value == "SIDC IDA3"):
                        value_clean = "IDA3"
                    elif ("CH" in value):
                        value_clean = value
                    elif ("GB" in value):
                        value_clean = value

                # Check whether this combination is already in the archive
                if (type=="continuous"):
                    combination_to_check = [market_area, pd.Timestamp(delivery_date).date(), trading_modality, value]
                else:
                    combination_to_check = [market_area, pd.Timestamp(trading_date).date(), pd.Timestamp(delivery_date).date(), trading_modality, sub_modality, value]

                if existing_combinations_file:
                    archive_check = check_existing_combinations(exsting_combinations, combination_to_check)
                else:
                    archive_check = False
                
                if (archive_check==True):
                    print(f"{type}, {market_area}, delivery date {delivery_date.date()}, {trading_modality}, {sub_modality}, {value} is already in the respective archive(s).")
                else:
                    success_indicator = "Error"
                    print(f"{type}, {market_area}, delivery date {delivery_date.date()}, {trading_modality}, {sub_modality}, {value} must be added to the respective archive.")

                    # Assemble the target URL and collect timestamp
                    if (type=="dayahead") or (type=="intraday"):
                        url = f"https://www.epexspot.com/en/market-results?market_area={market_area}&auction={value_clean}&trading_date={trading_date_str}&delivery_date={delivery_date_str}&underlying_year=&modality=Auction&sub_modality={sub_modality}&technology=&data_mode=table&period=&production_period="
                    elif (type=="continuous"):
                        url = f"https://www.epexspot.com/en/market-results?market_area={market_area}&auction=&trading_date=&delivery_date={delivery_date_str}&underlying_year=&modality=Continuous&sub_modality=&technology=&data_mode=table&period=&production_period=&product={str(value)}"
                    elif ("aggregated_curves" in type):
                        url = f"https://www.epexspot.com/en/market-results?market_area={market_area}&auction={value_clean}&trading_date={trading_date_str}&delivery_date={delivery_date_str}&underlying_year=&modality=Auction&sub_modality={sub_modality}&technology=&data_mode=aggregated&period=&production_period="
                    current_utc_time = datetime.now(timezone.utc)
                    
                    print("Starting download ... ", end="")

                    # Access website and convert html into soup object
                    try:
                        if (type=="dayahead") or (type=="intraday"):
                            soup = extract_soup(chromedriver_filepath, url)
                        elif ("aggregated_curves" in type) or (type=="continuous"):
                            soup = extract_soup_aggregated_curves(chromedriver_filepath, url)
                        success_indicator = "Success"
                        print(f"successful.")
                    except Exception as e:
                        success_indicator = "Error"
                        print(e)
                        print(f"failed.")
                        print(f"Website unresponsive for {type}, {market_area}, delivery day: {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                        print(f"Problem with URL: {url}.")
                        print(f"URL accessed at {current_utc_time}.")

                    # Now extract last update time from soup and perform plausibility checks on the obtained value
                    if (success_indicator!="Error"):
                        try: 
                            last_update = extract_last_update(soup)
                        except Exception as e:
                            last_update = None
                            success_indicator = "Error"
                            print(e)
                            print(f"Error extracting last update for {type}, {market_area}, {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                        if (last_update is not None):
                            if (success_indicator!="Error") and (len(last_update)==0):
                                success_indicator = "Error"
                                print(f"Error extracting last update for {type}, {market_area}, {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and len(last_update)<10:
                                success_indicator = "Error"
                                print(f"Error extracting last update for {type}, {market_area}, {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")      
                    # Now, if the last update was extracted correctly, proceed with the main logic
                    if (success_indicator!="Error"):

                        if (type=="dayahead") or (type=="intraday"):
                            hours_column_names = ["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "MarketSegment", "AuctionName", "LastUpdate", "Hours", "BuyVolume(MWh)", "SellVolume(MWh)", "Volume(MWh)", "Price(EUR/MWh)"]
                            base_peak_column_names = ["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "MarketSegment", "AuctionName", "LastUpdate", "Baseload(EUR/MWh)", "Peakload(EUR/MWh)"]

                            # First extract hours data
                            # Extract hours and perform some plausibility checks on extracted data
                            try:
                                hours = extract_hours(soup)
                            except Exception as e:
                                success_indicator = "Error"
                                print(e)
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                                
                            if (success_indicator!="Error") and (len(hours)==0):
                                success_indicator = "Error"
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (hours is None):
                                success_indicator = "Error"
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (len(hours[0])<2):
                                success_indicator = "Error"
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                                
                            # Now extract actual data
                            try: 
                                dayahead_intraday_data = extract_volume_and_price_data(soup)
                            except Exception as e:
                                dayahead_intraday_data = None
                                success_indicator = "Error"
                                print(e)
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")

                            if (success_indicator!="Error") and (len(dayahead_intraday_data)==0):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (dayahead_intraday_data is None):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (len(dayahead_intraday_data[0])==0):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (dayahead_intraday_data[0] is None):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (len(dayahead_intraday_data[-1])==0):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (dayahead_intraday_data[-1] is None):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (len(dayahead_intraday_data[0]) != 4):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (len(dayahead_intraday_data[-1]) != 4):
                                success_indicator = "Error"
                                print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")

                            if (success_indicator!="Error"):
                                try:
                                    # Create dataframe for hourly data
                                    hours_data_list = []

                                    for index, row in enumerate(dayahead_intraday_data):
                                        # Add metadata
                                        row_clean = [market_area, trading_date.date(), delivery_date.date(), trading_modality, sub_modality, value, last_update]
                        
                                        # Add hour
                                        hour = hours[index]
                                        row_clean.append(hour)

                                        # Add obtained actual info
                                        for entry in dayahead_intraday_data[index]:
                                            row_clean.append(entry)
                                        
                                        # Append clean row
                                        hours_data_list.append(row_clean)

                                except Exception as e:
                                    hours_data_list = None
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")

                            # Second extract base- and peakload data
                            # Extract base- and peakload data and perform some plausibility checks
                            try:
                                base_peak = extract_baseload_peakload(soup)
                                baseload = base_peak[0]
                                peakload = base_peak[1]
                            except Exception as e:
                                baseload = None
                                peakload = None
                                success_indicator = "Error"
                                print(e)
                                print(f"Error extracting base- and peakload values for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            
                            if (success_indicator!="Error") and (baseload is None):
                                success_indicator = "Error"
                                print(f"Error extracting base- and peakload values for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (peakload is None):
                                success_indicator = "Error"
                                print(f"Error extracting base- and peakload values for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            
                            if (success_indicator!="Error"):
                                # Now extract actual data
                                try:
                                    base_peak_data_list = [[market_area, trading_date.date(), delivery_date.date(), trading_modality, sub_modality, value, last_update, baseload, peakload]]
                                except Exception as e:
                                    base_peak_data_list = None
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Error extracting base- and peakload values for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                        
                        elif (type=="continuous"):

                            column_names = ["MarketArea", "DeliveryDate", "TradingModality", "Product(min)", "LastUpdate", "Hours", "Low(EUR/MWh)", "High(EUR/MWh)",
                                        "Last(EUR/MWh)", "WeightAvg(EUR/MWh)", "IDFull(EUR/MWh)", "ID1(EUR/MWh)", "ID3(EUR/MWh)", "BuyVolume(MWh)", "SellVolume(MWh)", "Volume(MWh)", "RPD(POUND/MWh)", "RPD HH(POUND/MWh)"]

                            # Extract hours and perform some plausibility checks on extracted data
                            try:
                                hours = extract_hours_continous(soup)
                                success_indicator = "Success" 
                            except Exception as e:
                                success_indicator = "Error" 
                                print(e)                           
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                                
                            if (success_indicator!="Error") and (len(hours)==0):
                                success_indicator = "Error"
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                            elif (success_indicator!="Error") and (hours is None):
                                success_indicator = "Error"
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                            elif ((success_indicator!="Error") and (len(hours[0])<2)):
                                success_indicator = "Error"
                                print(f"Error extracting hours for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                            
                            if (success_indicator!="Error"):
                                try: 
                                    continuous_data = extract_volume_and_price_data_continuous(soup)
                                except Exception as e:
                                    continuous_data = None
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                                
                                if (len(continuous_data)==0) and (success_indicator!="Error"):
                                    success_indicator = "Error"
                                    print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                                elif (continuous_data is None) and (success_indicator!="Error"):
                                    success_indicator = "Error"
                                    print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                                elif (len(continuous_data[0])<4) and (success_indicator!="Error"):
                                    print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                                    success_indicator = "Error"
                                elif ((len(continuous_data[-1])<4) and (success_indicator!="Error")):
                                    print(f"Error extracting volume and price data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")
                                    success_indicator = "Error"
                                    
                            if (success_indicator!="Error"):
                                # Extract headers
                                try:
                                    soup_header_row = soup.find_all('tr')[1]
                                    headers = [th.get_text(strip=True) for th in soup_header_row.find_all('th')]
                                except:
                                    success_indicator = "Error"
                                    headers = None

                            if (success_indicator!="Error"):
                                id_full_indicator = False
                                id_1_indicator = False
                                id_3_indicator = False
                                rpd_indicator = False

                                for header_item in headers:
                                    if ("id" in str(header_item).lower()) and ("full" in str(header_item).lower()):
                                        id_full_indicator = True
                                    if ("id" in str(header_item).lower()) and ("1" in str(header_item).lower()):
                                        id_1_indicator = True
                                    if ("id" in str(header_item).lower()) and ("3" in str(header_item).lower()):
                                        id_3_indicator = True
                                    if ("rpd" in str(header_item).lower()):
                                        rpd_indicator = True
                                        
                                try:
                                    data_list = []

                                    for index,row in enumerate(continuous_data):
                                        row_clean = [market_area, delivery_date.date(), trading_modality, value, last_update]
                                        hour = hours[index]
                                        row_clean.append(hour)
                                        row_counter = len(row_clean)-1 # Will be 5
                                        for entry in continuous_data[index]:
                                            row_counter += 1
                                            if ((id_full_indicator is False) and (row_counter==10)):
                                                # Need None for ID Full
                                                row_clean.append(None)
                                                row_counter += 1
                                            if ((id_1_indicator is False and rpd_indicator is False) and (row_counter==11)):
                                                # Needs None for ID1
                                                row_clean.append(None)
                                                row_counter += 1
                                            if ((id_3_indicator is False and rpd_indicator is False) and (row_counter==12)):
                                                # Needs None for ID3
                                                row_clean.append(None)
                                                row_counter += 1                                        
                                            row_clean.append(entry)
                                        if ("GB" not in market_area): # append two None for rpd columns
                                            row_clean.append(None)
                                            row_clean.append(None)
                                        else: # GB case
                                            rpd = row_clean[11]
                                            rpd_hh = row_clean[12]
                                            row_clean[11] = None
                                            row_clean[12] = None
                                            row_clean.append(rpd)
                                            row_clean.append(rpd_hh)

                                        data_list.append(row_clean)

                                except Exception as e:
                                    data_list = None
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Error transforming data for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}min product.")

                        elif ("aggregated_curves" in type):

                            column_names = ["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "MarketSegment", "AuctionName", "LastUpdate", "Hours", "Participant", "Volume(MWh)", "Price(EUR/MWh)"]

                            # Navigate to actual data by looking for appropriate tag
                            try:
                                script_tag = soup.find('script', {'type': 'application/json', 'data-drupal-selector': 'drupal-settings-json'})
                                success_indicator = "Success"
                            except Exception as e:
                                script_tag = None
                                success_indicator = "Error"
                                print(e)
                                print(f"Error finding correct tag in soup for aggregated curves of {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")

                            # Extract the JSON content
                            if (success_indicator!="Error"):
                                try:
                                    json_content = script_tag.string.strip()
                                except Exception as e:
                                    json_content = None
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Error parsing JSON for aggregated curves of {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                                    
                            # Parse the JSON string
                            if (success_indicator!="Error"):
                                try:
                                    parsed_json = json.loads(json_content)
                                    # Actual data extraction
                                    demand = json.loads(parsed_json['charts']['aggregated'])['demand']
                                    supply = json.loads(parsed_json['charts']['aggregated'])['supply']
                                    demand_and_supply = [demand, supply]

                                    data_list = []

                                    for participant in demand_and_supply:
                                        for key in participant['data'].keys():
                                            for entry in participant['data'][key]:
                                                current_entry = [market_area, trading_date.date(), delivery_date.date(), trading_modality, sub_modality, value, last_update]
                                                
                                                MWh = float(entry['x'])
                                                price = float(entry['y'])
                                                date_stamp = entry['dateTime'].split(" (")
                                                date_string = date_stamp[0]
                                                date_object = datetime.strptime(date_string, "%d %B %Y").date()
                                                hour_range = date_stamp[1].replace(")","")
                                                
                                                current_entry.append(hour_range)
                                                current_entry.append(participant['key']) # key is either supply or demand
                                                current_entry.append(MWh)
                                                current_entry.append(price)
                                                
                                                # Replace the delivery date from the URL with the one from the JSON content
                                                current_entry[2] = date_object
                                                
                                                data_list.append(current_entry)
                                except:
                                    data_list = None
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Error decoding JSON for aggregated curves of {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            
                    if (success_indicator!="Error"):
                        if (type=="dayahead") or (type=="intraday"):
                            # Create and clean DataFrames
                            try:
                                hours_data_df = clean_df(pd.DataFrame(hours_data_list, columns=hours_column_names))
                            except Exception as e:
                                hours_data_df = None
                                success_indicator = "Error"
                                print(e)
                                print(f"Cannot assemble DataFrame for hours data of {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            
                            if (success_indicator!="Error"):
                                try:
                                    base_peak_data_df = clean_df(pd.DataFrame(base_peak_data_list, columns=base_peak_column_names))
                                except Exception as e:
                                    base_peak_data_df = None
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Cannot assemble DataFrame for base peak data of {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                                
                            if (success_indicator!="Error") and ((hours_data_df is None) or (base_peak_data_df is None)):
                                success_indicator = "Error"
                                print(f"Assembled empty DataFrame for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and ((len(hours_data_df)==0) or (len(base_peak_data_df)==0)):
                                success_indicator = "Error"
                                print(f"Assembled empty DataFrame for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and ((hours_data_df.empty) or (base_peak_data_df.empty)):
                                success_indicator = "Error"
                                print(f"Assembled empty DataFrame for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")

                            # Write into archive
                            # First hours data
                            if (success_indicator!="Error"):
                                for elem in ["hours", "basepeak"]:
                                    if (success_indicator!="Error"):
                                        try:
                                            archive_folder_market_area = os.path.join(folder_path, market_area, "")
                                            if (elem=="hours"):
                                                archive_data_filepath = f"{archive_folder_market_area}epex_{market_area}_{type}_hours_archive.csv"
                                            else:
                                                archive_data_filepath = f"{archive_folder_market_area}epex_{market_area}_{type}_base_peak_archive.csv"
                                            if (os.path.exists(archive_folder_market_area)):
                                                if (os.path.exists(archive_data_filepath)):
                                                    archive_data_df = import_csv(archive_data_filepath)
                                                    archive_data_df["TradingDate"] = pd.to_datetime(archive_data_df["TradingDate"]).dt.date
                                                    archive_data_df["DeliveryDate"] = pd.to_datetime(archive_data_df["DeliveryDate"]).dt.date
                                                else:
                                                    archive_data_df = pd.DataFrame()
                                            else:
                                                os.makedirs(archive_folder_market_area)
                                                archive_data_df = pd.DataFrame()
                                            if (elem=="hours"):
                                                archive_data_df = pd.concat([archive_data_df, hours_data_df], ignore_index=True)
                                            else:
                                                archive_data_df = pd.concat([archive_data_df, base_peak_data_df], ignore_index=True)
                                            archive_data_df.to_csv(archive_data_filepath, index=False)
                                            success_indicator = "Success"
                                            if (elem=="hours"):
                                                print(f"Hours of {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value} added to the {market_area} hours archive.")
                                            else:
                                                print(f"Base- and peakloads of {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value} added to the {market_area} base- and peakload archive.")
                                        except Exception as e:
                                            success_indicator = "Error"
                                            print(e)
                                            if (elem=="Hours"):
                                                print(f"Archive error for hours of {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                                            else:
                                                print(f"Archive error for base- and peakloads of {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                                    else:
                                        break

                        elif (type=="continuous") or ("aggregated_curves" in type):
                            # Create and clean DataFrame
                            try:
                                data_df = clean_df(pd.DataFrame(data_list, columns=column_names))
                            except Exception as e:
                                data_df = None
                                success_indicator = "Error"
                                print(e)
                                print(f"Cannot assemble DataFrame for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                                
                            if (success_indicator!="Error") and (data_df is None):
                                success_indicator = "Error"
                                print(f"Assembled empty DataFrame for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (len(data_df)==0):
                                success_indicator = "Error"
                                print(f"Assembled empty DataFrame for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                            elif (success_indicator!="Error") and (data_df.empty):
                                success_indicator = "Error"
                                print(f"Assembled empty DataFrame for {type}, {market_area}, delivery date {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")

                            # Write into archive
                            if (success_indicator!="Error"):
                                try:
                                    archive_folder_market_area = os.path.join(folder_path, market_area, "")
                                    archive_data_filepath = f"{archive_folder_market_area}epex_{market_area}_{type}_archive.csv"
                                    if (os.path.exists(archive_folder_market_area)):
                                        if (os.path.exists(archive_data_filepath)):
                                            archive_data_df = import_csv(archive_data_filepath)
                                            if ("aggregated_curves" in type):
                                                archive_data_df["TradingDate"] = pd.to_datetime(archive_data_df["TradingDate"]).dt.date
                                            archive_data_df["DeliveryDate"] = pd.to_datetime(archive_data_df["DeliveryDate"]).dt.date
                                        else:
                                            archive_data_df = pd.DataFrame()
                                    else:
                                        os.makedirs(archive_folder_market_area)
                                        archive_data_df = pd.DataFrame()
                                    archive_data_df = pd.concat([archive_data_df, data_df], ignore_index=True)
                                    archive_data_df.to_csv(archive_data_filepath, index=False)
                                    success_indicator = "Success"
                                    print(f"{type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value} added to the respective archive.")
                                except Exception as e:
                                    success_indicator = "Error"
                                    print(e)
                                    print(f"Archive error for {type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value}.")
                    
                    if (success_indicator=="Error"):
                        total_error_count += 1

                    # Finally, update tracking file
                    if ("dayahead" in type) or ("intraday" in type):
                        tracking_data = [market_area, trading_date_str, delivery_date_str, trading_modality, sub_modality, value, current_utc_time.isoformat(), str(success_indicator)]
                        header_row = ["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "SubModality", "AuctionName", "WebsiteAccessTimeUTC", "SuccessIndicator"]
                    elif ("continuous" in type):
                        tracking_data = [market_area, delivery_date_str, trading_modality, value, current_utc_time.isoformat(), str(success_indicator)]
                        header_row = ["MarketArea", "DeliveryDate", "TradingModality", "Product(min)", "WebsiteAccessTimeUTC", "SuccessIndicator"]
                    elif ("aggregated_curves" in type):
                        tracking_data = [market_area, trading_date_str, delivery_date_str, trading_modality, sub_modality, value, current_utc_time.isoformat(), str(success_indicator)]
                        header_row = ["MarketArea", "TradingDate", "DeliveryDate", "TradingModality", "SubModality", "AuctionName", "WebsiteAccessTimeUTC", "SuccessIndicator"]

                    if (success_indicator=="Error") and (not os.path.exists(folder_path)):
                        os.makedirs(folder_path)
                    update_tracking_file(tracking_file_filepath, tracking_data, header_row)
                    print(f"{type}, {market_area}, delivery day {delivery_date_str}, {trading_modality}, {sub_modality}, {value} tracking data saved in CSV file.")
                    
                    time.sleep(backoff_time)

                print()

    return total_error_count