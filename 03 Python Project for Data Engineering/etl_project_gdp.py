import requests
import pandas as pd
import sqlite3
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO

# -------------------------- #
#      Logging Setup         #
# -------------------------- #
logging.basicConfig(
    filename="etl_project_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_step(message):
    """
    Logs and prints a step of the ETL process with a timestamp.
    """
    print(message)
    logging.info(message)

# -------------------------- #
#      ETL Functions         #
# -------------------------- #

def extract_data(url):
    """
    Extracts the GDP table from the given URL.

    Parameters:
        url (str): The archived Wikipedia URL

    Returns:
        pd.DataFrame: Raw GDP table as extracted from the HTML
    """
    log_step("Starting data extraction...")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to fetch webpage.")

    soup = BeautifulSoup(response.text, 'html.parser')
    all_tables = soup.find_all("table", {"class": "wikitable"})

    # Identify correct table by its caption
    for table in all_tables:
        caption = table.find("caption")
        if caption and "GDP (USD million) by country" in caption.text:
            log_step("Found the correct GDP table using caption.")
            df = pd.read_html(StringIO(str(table)))[0]
            return df

    raise Exception("Could not find the correct GDP table by caption.")

def transform_data(df):
    """
    Cleans and transforms the extracted GDP data:
    - Selects Country and IMF Estimate columns (0 and 2)
    - Removes footnotes, commas, and em-dashes
    - Converts GDP to billions and drops missing values
    """
    log_step("Starting data transformation...")

    # Select Country and IMF Estimate columns
    df = df.iloc[:, [0, 2]]
    df.columns = ['Country', 'GDP_USD_million']

    # Remove 'World' row and any rows with missing or bad data
    df = df[~df['Country'].str.lower().isin(['world', '—'])]

    # Clean GDP column: remove commas, em-dashes, footnotes
    df['GDP_USD_million'] = df['GDP_USD_million'].replace(
        to_replace=r'\[.*?\]|,|—', value='', regex=True
    )

    # Convert to float (invalid strings become NaN)
    df['GDP_USD_million'] = pd.to_numeric(df['GDP_USD_million'], errors='coerce')

    # Drop rows with missing GDP
    df = df.dropna(subset=['GDP_USD_million'])

    # Convert to billions and round
    df['GDP_USD_billion'] = (df['GDP_USD_million'] / 1000).round(2)

    # Keep final cleaned columns
    df = df[['Country', 'GDP_USD_billion']]

    log_step("Transformation complete. Sample data:\n" + str(df.head()))
    return df


def load_to_csv(df, filename="Countries_by_GDP.csv"):
    """
    Saves the DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): Transformed DataFrame
        filename (str): Name of the CSV file to write
    """
    log_step(f"Saving data to CSV file: {filename}")
    df.to_csv(filename, index=False)

def load_to_database(df, db_name="World_Economies.db"):
    """
    Loads the DataFrame into a SQLite database table named 'Countries_by_GDP'.

    Parameters:
        df (pd.DataFrame): Transformed DataFrame
        db_name (str): Name of the database file

    Returns:
        sqlite3.Connection: Active SQLite connection for querying
    """
    log_step(f"Saving data to database: {db_name}")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Drop and recreate the table
    cursor.execute("DROP TABLE IF EXISTS Countries_by_GDP")
    cursor.execute("""
        CREATE TABLE Countries_by_GDP (
            Country TEXT,
            GDP_USD_billion REAL
        )
    """)

    # Insert data
    df.to_sql("Countries_by_GDP", conn, if_exists="append", index=False)
    conn.commit()
    log_step("Data loaded into database.")
    return conn

def query_database(conn):
    """
    Executes a SQL query to display countries with GDP > 100 billion USD.

    Parameters:
        conn (sqlite3.Connection): Open connection to the database
    """
    log_step("Running query for countries with GDP > 100 billion USD...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion > 100
    """)
    results = cursor.fetchall()

    # Log the results
    log_step(f"Query returned {len(results)} countries:\n" + "\n".join([str(row) for row in results]))

# -------------------------- #
#           Main             #
# -------------------------- #

def main():
    """
    Main ETL function:
    - Extracts GDP data
    - Transforms and cleans it
    - Saves to CSV and database
    - Executes a query on the database
    """
    log_step("ETL process started.")
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

    try:
        df_raw = extract_data(url)
        df_transformed = transform_data(df_raw)
        load_to_csv(df_transformed)
        conn = load_to_database(df_transformed)
        query_database(conn)
        conn.close()
        log_step("ETL process completed successfully.")
    except Exception as e:
        log_step(f"ETL process failed: {str(e)}")

# -------------------------- #
#         Run Script         #
# -------------------------- #

if __name__ == "__main__":
    main()
