import os
import csv
import pandas as pd
import bs4 as bs4
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def import_exsting_combinations_file(tracking_file_filepath:str, columns_to_check:list) -> pd.DataFrame:
    """
    Imports the tracking file which stores information on which market area, trading segment and product/auction
    was successfully downloaded. These variables should be passed in "columns_to_check" as these are the base for
    creating the existing combinations dataframe. It drops observations with "Error" in the "SuccessIndicator" 
    column so that a new download attempt is started. In the remaining records, duplicates are removed. 

    This function filters the input CSV file ("tracking_file_filepath") to retain only the specified columns
    ("columns_to_check"). Observations with "SuccessIndicator" = Error are removed. 
    It then removes duplicate rows based on the filtered columns.

    Parameters:
    tracking_file_filepath (str): The path of the tracking file.
    columns_to_check (list): A list of column names that should be retained in the DataFrame. All other columns 
    will be dropped.

    Returns:
    (pd.DataFrame): A pandas DataFrame without duplicates representing the combinations already existing in the
    corresponding archive.

    Example:
    path = archive_tracking/data.csv
    columns_to_check = ["A", "B"]
    result = import_exsting_combinations_file(path, columns_to_check)
    """
    
    # Import csv file
    archive_df = import_csv(tracking_file_filepath)

    # Convert date columns to correct datatype and format
    archive_df["DeliveryDate"] = pd.to_datetime(archive_df["DeliveryDate"]).dt.date

    # To retry unsuccessful entries, remove all errors
    archive_df = archive_df[archive_df["SuccessIndicator"]!="Error"]

    # Now remove columns that are not required anymore
    columns_to_drop = []
    for column_name in archive_df.columns:
        if (column_name not in columns_to_check):
            columns_to_drop.append(column_name)

    # Drop specified columns
    archive_df = archive_df.drop(columns=columns_to_drop)

    # Drop duplicates
    archive_unique_df = archive_df.drop_duplicates()

    return archive_unique_df

def check_existing_combinations(archive_df:pd.DataFrame, combination:list) -> bool:
    """
    Checks if a specific combination of values exists in a filtered and deduplicated pandas DataFrame.

    This function checks if a given combination of values ("combination") exists in a pandas DataFrame.

    Parameters:
    archive_df (pandas.DataFrame): The input DataFrame to be filtered and deduplicated.
    combination (list): A list representing the specific combination of values to check for existence in the
    filtered and deduplicated pandas DataFrame.

    Returns:
    (bool): True if the combination exists in the filtered and deduplicated pandas DataFrame, False otherwise.

    Example:
    data = {
         "A": [1, 1, 2],
         "B": [3, 3, 4],
         "C": [5, 6, 7]
     }
    df = pd.DataFrame(data)
    columns_to_check = ["A", "B"]
    combination = [1, 3]
    result = check_existing_combinations(df, combination)
    print(result)
    True
    """

    # Check if the combination exists in the archive DataFrame
    record_exists = ((archive_df == combination).all(axis=1)).any()

    return record_exists
    
def import_csv(file_path:str) -> pd.DataFrame:
    """
    Import a .csv file as a pandas DataFrame. Removes columns which have names that contain "Unnamed".
    
    Parameters:
    file_path (str): The path to the .csv file to be imported.
    
    Returns:
    (pd.DataFrame): The DataFrame containing the data from the .csv file.
    """

    if not os.path.exists(file_path):
        print(f"The file does not exist: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path)

        # Remove columns that contain "Unnamed" in their name
        df = df.loc[:, ~df.columns.str.contains("Unnamed")]

        # Reset the index
        df.reset_index(drop=True, inplace=True)

        return df
    
    except Exception as e:
        print(f"Error reading or processing the file: {e}")
        return None

def extract_soup(chrome_driver_path:str, url:str) -> bs4.BeautifulSoup:
    """
    Extracts the HTML content of a webpage as a BeautifulSoup object using the Selenium WebDriver.

    This function uses Selenium to open a given URL in a Chrome browser, retrieves the page source, 
    and converts it into a BeautifulSoup object for further web scraping or parsing. It then closes 
    the browser and returns the parsed HTML content as a BeautifulSoup object.

    Parameters:
    chrome_driver_path (str): The file path to the Chrome WebDriver executable.
    url (str): The URL of the webpage to extract the content from.

    Returns:
    BeautifulSoup: A BeautifulSoup object containing the parsed HTML content of the webpage.

    Example:
    soup = extract_soup('/path/to/chromedriver', 'https://example.com')
    print(soup.prettify())
    """

    # Initialize WebDriver
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    # Open the target URL
    driver.get(url)

    # Get page source
    page_source = driver.page_source

    # Close the browser
    driver.quit()

    # Convert to bs4 soup object
    soup = BeautifulSoup(page_source, "html.parser")

    return soup

def extract_soup_aggregated_curves(chromedriver_path:str, url:str) -> bs4.BeautifulSoup:
    """
    Extracts the HTML content of a webpage as a BeautifulSoup object using a Selenium WebDriver.

    This function uses Selenium to open a given URL in a Chrome browser. It waits until a specified 
    script tag is present in the underlying HTML content but max 20 seconds. Then, it retrieves the 
    page source, and converts it into a BeautifulSoup object for further web scraping or parsing. 
    It then closes the browser and returns the parsed HTML content as a BeautifulSoup object.

    Parameters:
    chromedriver_path (str): The file path to the Chrome WebDriver executable.
    url (str): The URL of the webpage to extract the content from.

    Returns:
    (BeautifulSoup): A BeautifulSoup object containing the parsed HTML content of the webpage.

    Example:
    soup = extract_soup('/path/to/chromedriver', 'https://example.com')
    print(soup.prettify())
    """

    # Initialize WebDriver
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    
    # Open the target URL
    driver.get(url)
    
    # Wait up to 20 seconds for the script tag to be present in the DOM
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "script[type='application/json'][data-drupal-selector='drupal-settings-json']"))
        )

    # Get page source
    page_source = driver.page_source

    # Close the browser
    driver.quit()

    # Convert to bs4 soup object
    soup = BeautifulSoup(page_source, "html.parser")

    return soup

def extract_hours(soup:bs4.BeautifulSoup) -> list:
    """
    Extracts the hours column in a table from a given BeautifulSoup object representing an HTML page.

    This function searches for a specific 'div' containing a list of hours, extracts the hours from
    the corresponding '<ul>' and '<li>' elements, and returns a list of clean hour strings.

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    (list): A list of strings representing the operating hours extracted from the HTML.

    Example:
    from bs4 import BeautifulSoup
    html_content = '<div class="fixed-column js-table-times"><ul><li><a>9:00 AM - 10:00 AM</a></li><li><a>10:00 AM - 11:00 AM</a></li></ul></div>'
    soup = BeautifulSoup(html_content, 'html.parser')
    extract_hours(soup)
    ['9:00 AM - 10:00 AM', '10:00 AM - 11:00 AM']
    """
        
    # Find the div containing the hours list
    hours_div = soup.find("div", class_="fixed-column js-table-times")

    # Find the <li> elements within the <ul> (which contains the hours)
    hours_list = hours_div.find("ul").find_all("li")

    hours_clean = []

    for hour_item in hours_list:
        # Extract the text of the <a> tag
        hour = hour_item.find("a").text.strip()  
        hours_clean.append(str(hour))

    return hours_clean

def extract_hours_continous(soup:bs4.BeautifulSoup) -> list:
    """
    Extracts the main hour ranges (e.g., "00 - 01", "01 - 02") and smaller time intervals 
    (e.g., "00:00 - 00:15", "00:15 - 00:30") from a BeautifulSoup object and interleaves them.

    This function searches for '<li>' elements with the class 'child' to extract the main hour ranges
    and '<li>' elements with the class 'sub-child lvl-2' to extract smaller time intervals. It then 
    combines these lists into a single list, interleaving one main hour range followed by four smaller 
    intervals.

    Parameters:
    html_content (str): A string representing the HTML content of a webpage.

    Returns:
    (list): A list of strings where each main hour range is followed by its corresponding intervals.

    Example:
    from bs4 import BeautifulSoup
    html_content = '''
    <div class="fixed-column js-table-times">
        <ul>
            <li class="child"><a>00 - 01</a></li>
            <li class="sub-child lvl-2"><a>00:00 - 00:15</a></li>
            <li class="sub-child lvl-2"><a>00:15 - 00:30</a></li>
            <li class="sub-child lvl-2"><a>00:30 - 00:45</a></li>
            <li class="sub-child lvl-2"><a>00:45 - 01:00</a></li>
            <li class="child"><a>01 - 02</a></li>
            <li class="sub-child lvl-2"><a>01:00 - 01:15</a></li>
            <li class="sub-child lvl-2"><a>01:15 - 01:30</a></li>
            <li class="sub-child lvl-2"><a>01:30 - 01:45</a></li>
            <li class="sub-child lvl-2"><a>01:45 - 02:00</a></li>
        </ul>
    </div>
    '''
    soup = BeautifulSoup(html_content, 'html.parser')
    result = extract_all_intervals(html_content)
    print(result)
    # Output: ['00 - 01', '00:00 - 00:15', '00:15 - 00:30', '00:30 - 00:45', '00:45 - 01:00',
    #          '01 - 02', '01:00 - 01:15', '01:15 - 01:30', '01:30 - 01:45', '01:45 - 02:00']
    """
    
    hours_div = soup.find("div", class_="fixed-column js-table-times")
    
    if (not hours_div):
        return []

    # Extract all intervals
    intervals = []
    
    # Process each level-0 (hour) block
    for hour_block in hours_div.find_all("li", class_="child"):
        # Add the full-hour range
        hour_range = hour_block.find("a").text.strip()
        intervals.append(hour_range)
        
        # Find the next siblings for this hour (level-1 and level-2 intervals)
        next_sibling = hour_block.find_next_sibling()
        while next_sibling and "child" not in next_sibling.get("class", []):
            if ("lvl-1" in next_sibling.get("class", [])):
                # Add the half-hour interval
                half_hour_interval = next_sibling.find("a").text.strip()
                intervals.append(half_hour_interval)
            elif ("lvl-2" in next_sibling.get("class", [])):
                # Add the smaller interval
                smaller_interval = next_sibling.find("a").text.strip()
                intervals.append(smaller_interval)
            # Move to the next sibling
            next_sibling = next_sibling.find_next_sibling()
    
    return intervals

def extract_last_update(soup:bs4.BeautifulSoup) -> str:
    """
    Extracts the last update date or timestamp from a given BeautifulSoup object.

    This function searches for a "span" or "div" element with the class "last-update", extracts the text
    content indicating the last update and returns the cleaned version of that text. If the 
    element is not found, a "ValueError" is raised.

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    (str): A cleaned string representing the last update information.

    Raises:
    ValueError: If the "last-update" span element is not found in the HTML.

    Example:
    from bs4 import BeautifulSoup
    html_content = '<span class="last-update">Last update: January 8, 2025</span>'
    soup = BeautifulSoup(html_content, 'html.parser')
    extract_last_update(soup)
    '13 January 2025 (10:33:43 CET/CEST)'
    """

    # Locate the "last-update" span element
    last_update_span = soup.find("span", class_="last-update")

    if (last_update_span):
        # Extract and clean the text content
        last_update_text = last_update_span.get_text(strip=True).replace("Last update:", "").strip().replace("\n", " ").strip()
        cleaned_text = " ".join(last_update_text.split())
        return cleaned_text
    else:
        # Locate the "last-update" span element
        last_update_div = soup.find("div", class_="last-update")

        if (last_update_div):
            # Extract and clean the text content
            last_update_text = last_update_div.get_text(strip=True).replace("Last update:", "").strip().replace("\n", " ").strip()
            cleaned_text = " ".join(last_update_text.split())
            return cleaned_text
        else:
            raise ValueError("Last update information not found.")
    
def extract_volume_and_price_data(soup) -> list:
    """
    Extracts volume and price data from a table in a given BeautifulSoup object.

    This function searches for a table with the class 'table-01' in the HTML, iterates through its rows
    and cells, and extracts the numerical data (volume and price) while cleaning it. The data is returned
    as a list of lists, where each inner list represents a row of data containing cleaned numerical values.

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    (list): A list of lists, where each inner list contains numerical values (floats) representing 
          the volume and price data extracted from the table.

    Example:
    from bs4 import BeautifulSoup
    html_content = '<table class="table-01"><tr><td>1,000</td><td>20.5</td></tr><tr><td>2,000</td><td>25.0</td></tr></table>'
    soup = BeautifulSoup(html_content, 'html.parser')
    extract_volume_and_price_data(soup)
    [[1000.0, 20.5], [2000.0, 25.0]]
    """
    
    # Find table with class "table-01" and extract rows
    table = soup.find("table", class_="table-01")
    rows = table.find_all("tr")

    data = []

    # Iterate over each row
    for row in rows:
        row_data = []

        # Iterate over each cell in the row
        for cell in row.find_all("td"):
            # Skip empty cells
            if not cell.text.strip():
                continue
            
            # Clean and convert the cell data to float
            try:
                cell_data = float(cell.text.strip().replace(",", ""))
                row_data.append(cell_data)
            except ValueError:
                # Handle cases where conversion to float fails
                continue
        
        # Append the row data if it contains valid values
        if (row_data):
            data.append(row_data)

    return data

def extract_volume_and_price_data_continuous(soup:bs4.BeautifulSoup) -> list:
    """
    Extract volume and price data from a table in a BeautifulSoup object.

    This function locates a table with the class 'table-01' in the provided BeautifulSoup object,
    iterates through its rows and cells, and extracts numerical data (volume and price). The data
    is returned as a list of lists, where each inner list represents a row of numerical values.

    If a cell's content cannot be converted to a float, it will be replaced with "None".

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    (list): A list of lists, where each inner list contains numerical values (as floats) or "None", 
    representing the volume and price data extracted from the table.

    Example:
    --------
    from bs4 import BeautifulSoup
    html_content = '''
    <table class="table-01">
        <tr><td>1,000</td><td>20.5</td></tr>
        <tr><td>2,000</td><td>25.0</td></tr>
    </table>
    '''
    soup = BeautifulSoup(html_content, 'html.parser')
    extract_volume_and_price_data_continuous(soup)
    # Output: [[1000.0, 20.5], [2000.0, 25.0]]
    """
    
    # Find table with class "table-01" and extract rows
    table = soup.find("table", class_="table-01")
    rows = table.find_all("tr")

    data = []

    # Iterate over each row
    for row in rows:
        row_data = []

        # Iterate over each cell in the row
        for cell in row.find_all("td"):
            # Skip empty cells
            if not cell.text.strip():
                continue
            
            # Clean and convert the cell data to float
            try:
                cell_data = float(cell.text.strip().replace(",", ""))
                row_data.append(cell_data)
            except ValueError:
                # Handle cases where conversion to float fails
                row_data.append(None)
        
        # Append the row data if it contains valid values
        if (len(row_data)!=0):
            data.append(row_data)

    return data

def extract_baseload_peakload(soup:bs4.BeautifulSoup) -> tuple:
    """
    Extracts the baseload and peakload values from a given BeautifulSoup object representing an HTML page.

    This function searches for rows in the HTML table containing the terms 'Baseload' and 'Peakload',
    and extracts their corresponding values. It returns a tuple containing the baseload and peakload values 
    as floats. If the peakload value cannot be converted to a float, it returns a string representing the 
    value. If a value is missing or invalid, it will be returned as "-".

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    (tuple): A tuple containing the baseload and peakload values. The values are either floats or "None",
    and the peakload can be a string if the value is not numeric.

    Example:
    from bs4 import BeautifulSoup
    html_content = '<tr><th>Baseload</th><td><span>5,000</span></td></tr><tr><th>Peakload</th><td><span>8,000</span></td></tr>'
    soup = BeautifulSoup(html_content, 'html.parser')
    extract_baseload_peakload(soup)
    (5000.0, 8000.0)
    """
    
    # Initialize variables to store baseload and peakload values
    baseload_value = None
    peakload_value = None

    # Find all rows in the table
    rows = soup.find_all("tr")

    # Iterate through rows to find "Baseload" and "Peakload"
    for row in rows:
        # Check if the row contains the term "Baseload"
        if (row.find("th") and "Baseload" in row.find("th").text):
            # Extract and clean the baseload value
            baseload_value = float(row.find("span").text.strip().replace(",", ""))

        # Check if the row contains the term "Peakload"
        if (row.find("th") and "Peakload" in row.find("th").text):
            # Extract the peakload value, handle exceptions if necessary
            try:
                peakload_value = float(row.find("span").text.strip().replace(",", ""))
            except ValueError:
                # If the peakload value isn't numeric, store it as a string
                peakload_value = row.find("span").text.strip().replace(",", "")
                if peakload_value == "-":
                    peakload_value = "-"

    return (baseload_value, peakload_value)

def clean_df(df:pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a DataFrame by standardizing column names, stripping whitespace from string data, 
    and formatting datetime columns.

    This function performs the following operations on the input DataFrame:
    1. Strips any leading or trailing whitespace from column names.
    2. Strips leading and trailing whitespace from all string (object) columns.
    3. Converts columns of datetime-like types to date-only format, ensuring consistency.

    Parameters:
    df (pd.DataFrame): The input pandas DataFrame to be cleaned.

    Returns:
    (pd.DataFrame): A cleaned DataFrame with standardized column names, trimmed string data and properly formatted datetime columns.

    Example:
    data = {
         " Name ": [" Alice ", " Bob", "Charlie "],
         "Join Date": ["2023-01-01", "2024-02-02", "2025-03-03"],
         "Age": [25, 30, 35]
     }
    df = pd.DataFrame(data)
    df["Join Date"] = pd.to_datetime(df["Join Date"])  # Ensure datetime format for testing
    clean_df(df)
         Name    Join Date  Age
    0   Alice  2023-01-01   25
    1     Bob  2024-02-02   30
    2  Charlie  2025-03-03   35
    """
    
    # Strip any whitespace from column names
    df.columns = df.columns.str.strip()  

    # Strip any whitespace from columns of type str and convert datetime correctly
    for column in df.columns:
        if (df[column].dtype == "str"):
            df[column] = df[column].str.strip()
        elif (pd.api.types.is_datetime64_any_dtype(df[column])):
            df[column] = pd.to_datetime(df[column]).dt.date
            df[column] = pd.to_datetime(df[column]).dt.date

    return df

def update_tracking_file(tracking_file_filepath:str, tracking_data: list, header_row:list):
    """
    Update or create a tracking file by adding new tracking data to a CSV file.

    This function updates a CSV file at the specified filepath. If the file already exists, it reads 
    the existing content, appends the new tracking data as the first data row (after the header) 
    and writes the updated content back to the file. If the file does not exist, the function creates 
    a new file with the provided header row and tracking data. The "header_row" parameter is only used
    when creating a new file. If the file exists, the header from the file is preserved.

    Parameters:
    tracking_file_filepath (str): The file path of the tracking CSV file to be updated or created.
    tracking_data (list): A list representing the row of data to be added to the file.
    header_row (list): A list representing the header row to be written if the file does not exist.

    Returns:
    (None)

    Example:
    update_tracking_file(
        tracking_file_filepath="intraday_tracking.csv",
        tracking_data=["DE-LU", "13.01.2025", "Intraday"],
        header_row=["MarketArea", "DeliveryDate", "TradingModality"]
    )
    """

    assert (len(tracking_data)==len(header_row))

    file_exists = os.path.isfile(tracking_file_filepath)
       
    if (file_exists):
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
            # Write the header first
            writer.writerow(header)
            # Write the modified data rows
            writer.writerows(data_rows)
    
    else:
        with open(tracking_file_filepath, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(header_row)
            writer.writerow(tracking_data)