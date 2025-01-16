import os
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

def import_xlsx(file_path):
    """
    Import an .xlsx file as a pandas DataFrame.
    
    Parameters:
    file_path (str): The path to the .xlsx file to be imported.
    
    Returns:
    pd.DataFrame: The DataFrame containing the data from the .xlsx file.
    """

    if not os.path.exists(file_path):
        print(f"The file does not exist: {file_path}")
        return None

    try:
        df = pd.read_excel(file_path)

        # Remove columns that contain "Unnamed" in their name
        df = df.loc[:, ~df.columns.str.contains("Unnamed")]

        # Reset the index
        df.reset_index(drop=True, inplace=True)

        return df
    
    except Exception as e:
        print(f"Error importing {file_path}: {e}")
        return None
    
def check_archive(archive_df:pd.DataFrame, combination:list, columns_to_check:list) -> bool:
    """
    Checks if a specific combination of values exists in a filtered and deduplicated DataFrame.

    This function filters the input DataFrame ("archive_df") to retain only the specified columns
    ("columns_to_check"). It then removes duplicate rows based on the filtered columns and checks
    if a given combination of values ("combination") exists in the resulting DataFrame.

    Parameters:
    archive_df (pandas.DataFrame): The input DataFrame to be filtered and deduplicated.
    combination (list): A list representing the specific combination of values to check for existence in the filtered and deduplicated DataFrame.
    columns_to_check (list):A list of column names that should be retained in the DataFrame. All other columns will be dropped.

    Returns:
    bool: True if the combination exists in the filtered and deduplicated DataFrame, False otherwise.

    Example:
    data = {
         "A": [1, 1, 2],
         "B": [3, 3, 4],
         "C": [5, 6, 7]
     }
    df = pd.DataFrame(data)
    columns_to_check = ["A", "B"]
    combination = [1, 3]
    result = check_archive_hours(df, combination, columns_to_check)
    print(result)
    True
    """

    columns_to_drop = []

    for column_name in archive_df.columns:
        if column_name not in columns_to_check:
            columns_to_drop.append(column_name)

    # Drop specified columns
    archive_df = archive_df.drop(columns=columns_to_drop)

    # Drop duplicates:
    archive_unique_df = archive_df.drop_duplicates()

    # Check if the combination exists in the archive DataFrame
    record_exists = ((archive_unique_df == combination).all(axis=1)).any()

    return record_exists

def extract_soup(chrome_driver_path:str, url:str):
    """
    Extracts the HTML content of a webpage as a BeautifulSoup object using a Selenium WebDriver.

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

def extract_hours(soup)->list:
    """
    Extracts the operating hours from a given BeautifulSoup object representing an HTML page.

    This function searches for a specific 'div' containing a list of hours, extracts the hours from
    the corresponding '<ul>' and '<li>' elements, and returns a list of clean hour strings.

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    list: A list of strings representing the operating hours extracted from the HTML.

    Example:
    from bs4 import BeautifulSoup
    html_content = '<div class="fixed-column js-table-times"><ul><li><a>9:00 AM - 5:00 PM</a></li></ul></div>'
    soup = BeautifulSoup(html_content, 'html.parser')
    extract_hours(soup)
    ['9:00 AM - 5:00 PM']
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

def extract_last_update(soup)->str:
    """
    Extracts the last update date or timestamp from a given BeautifulSoup object.

    This function searches for a 'span' element with the class 'last-update', extracts the text
    content indicating the last update, and returns the cleaned version of that text. If the 
    element is not found, a 'ValueError' is raised.

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    str: A cleaned string representing the last update information.

    Raises:
    ValueError: If the "last-update" span element is not found in the HTML.

    Example:
    from bs4 import BeautifulSoup
    html_content = '<span class="last-update">Last update: January 8, 2025</span>'
    soup = BeautifulSoup(html_content, 'html.parser')
    extract_last_update(soup)
    'January 8, 2025'
    """

    # Locate the "last-update" span element
    last_update_span = soup.find("span", class_="last-update")

    if last_update_span:
        # Extract and clean the text content
        last_update_text = last_update_span.get_text(strip=True).replace("Last update:", "").strip().replace("\n", " ").strip()
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
    list: A list of lists, where each inner list contains numerical values (floats) representing 
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
        if row_data:
            data.append(row_data)

    return data

def extract_baseload_peakload(soup) -> tuple:
    """
    Extracts the baseload and peakload values from a given BeautifulSoup object representing an HTML page.

    This function searches for rows in the HTML table containing the terms 'Baseload' and 'Peakload',
    and extracts their corresponding values. It returns a tuple containing the baseload and peakload values 
    as floats. If the peakload value cannot be converted to a float, it returns a string representing the 
    value. If a value is missing or invalid, it will be returned as '-'.

    Parameters:
    soup (BeautifulSoup): A BeautifulSoup object representing the parsed HTML content of a webpage.

    Returns:
    tuple: A tuple containing the baseload and peakload values. The values are either floats or None,
           and the peakload can be a string if the value isn't numeric.

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
        if row.find("th") and "Baseload" in row.find("th").text:
            # Extract and clean the baseload value
            baseload_value = float(row.find("span").text.strip().replace(",", ""))

        # Check if the row contains the term "Peakload"
        if row.find("th") and "Peakload" in row.find("th").text:
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
    pd.DataFrame : A cleaned DataFrame with standardized column names, trimmed string data and properly formatted datetime columns.


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
        if df[column].dtype == "str":
            df[column] = df[column].str.strip()
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = pd.to_datetime(df[column]).dt.date
            df[column] = pd.to_datetime(df[column]).dt.date

    return df

def plausibility_checks(df:pd.DataFrame) -> bool:
    """
    Performs plausibility checks on a DataFrame to ensure it is not empty or composed entirely of NaN values.

    This function checks the following conditions:
    1. If the entire DataFrame consists of NaN values, it returns `False`.
    2. If any column in the DataFrame is entirely composed of NaN values, it returns `False`.

    Parameters:
    df (pd.DataFrame): The input pandas DataFrame to be checked for plausibility.

    Returns:
    bool : "True" if the DataFrame contains valid, non-NaN data (at least one non-NaN value exists in the DataFrame), "False" if the entire DataFrame is NaN or if any column is entirely NaN.

    Example:
    data = {
         "A": [1, 2, 3],
         "B": [None, None, None],
         "C": [4, 5, 6]
     }
    df = pd.DataFrame(data)
    plausibility_checks(df)
    False  # Since column 'B' is all NaN

    data_valid = {
         "A": [1, 2, 3],
         "B": [7, 8, 9],
         "C": [4, 5, 6]
     }
    df_valid = pd.DataFrame(data_valid)
    plausibility_checks(df_valid)
    True  # DataFrame contains valid, non-NaN data
    """

    all_nan = df.isna().all().all()

    if all_nan:
        return False

    columns = df.isna().all(axis=0)

    all_nan_columns = []

    for col_name, value in columns.items():
        if value == True:
            all_nan_columns.append(col_name)

    number_all_nans = len(all_nan_columns)

    if number_all_nans > 0:
        return False
    
    return True