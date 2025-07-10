# Code for ETL operations on Largest Banks by Market Cap

# ---------------------------
# Importing the required libraries
# ---------------------------
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from io import StringIO

# ---------------------------
# Task 1: Logging Progress
# ---------------------------
def log_progress(message):
    '''Logs the given message with timestamp to code_log.txt'''
    with open("code_log.txt", "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{timestamp} : {message}\n")

# ---------------------------
# Task 2: Extract Data
# ---------------------------
def extract(url, table_attribs):
    '''Extracts table with bank market cap data from Wikipedia archive'''
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the <h2> with the correct id
    h2_tag = soup.find("h2", {"id": "By_market_capitalization"})
    if not h2_tag:
        raise Exception("Heading with ID 'By_market_capitalization' not found")

    # Find the next wikitable after the heading
    next_table = h2_tag.find_next("table", class_="wikitable")
    if not next_table:
        raise Exception("Target table not found after heading")

    # Convert the HTML table to DataFrame
    df = pd.read_html(StringIO(str(next_table)))[0]

    # Extract relevant columns
    df = df.iloc[:, [0, 2]]  # 0 = Bank name, 2 = GlobalData Market Cap
    df.columns = table_attribs
    df = df.dropna(subset=['MC_USD_Billion'])  # Drop rows with missing market cap
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')  # ensure float

    log_progress("Data extraction complete. Initiating Transformation process")
    return df


# ---------------------------
# Task 3: Transform Data
# ---------------------------
def transform(df, csv_path):
    '''Adds GBP, EUR, and INR market cap columns using exchange rates from CSV'''
    rates_df = pd.read_csv(csv_path)
    rates = dict(zip(rates_df['Currency'], rates_df['Rate']))

    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * rates['GBP']).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * rates['EUR']).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * rates['INR']).round(2)

    log_progress("Data transformation complete. Initiating Loading process")
    return df

# ---------------------------
# Task 4: Load to CSV
# ---------------------------
def load_to_csv(df, output_path):
    '''Saves the transformed data to a CSV file'''
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")

# ---------------------------
# Task 5: Load to SQLite DB
# ---------------------------
def load_to_db(df, sql_connection, table_name):
    '''Loads the dataframe to a SQLite database'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

# ---------------------------
# Task 6: Run Queries
# ---------------------------
def run_query(query_statement, sql_connection):
    '''Runs a SQL query and prints the result'''
    print(f"Query: {query_statement}")
    result = pd.read_sql(query_statement, sql_connection)
    print(result)
    log_progress("Process Complete")

# ---------------------------
# Task 7: Execution Pipeline
# ---------------------------

# Define variables
URL = "https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_CSV = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
OUTPUT_CSV = "./Largest_banks_data.csv"
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
ATTRIBUTES = ['Name', 'MC_USD_Billion']

# Perform ETL
log_progress("Preliminaries complete. Initiating ETL process")
df_extracted = extract(URL, ATTRIBUTES)
df_extracted['MC_USD_Billion'] = pd.to_numeric(df_extracted['MC_USD_Billion'], errors='coerce')  # ensure numeric
df_transformed = transform(df_extracted, EXCHANGE_CSV)
load_to_csv(df_transformed, OUTPUT_CSV)

conn = sqlite3.connect(DB_NAME)
log_progress("SQL Connection initiated")
load_to_db(df_transformed, conn, TABLE_NAME)

# Run 3 queries
run_query(f"SELECT * FROM {TABLE_NAME}", conn)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}", conn)
run_query(f"SELECT Name FROM {TABLE_NAME} LIMIT 5", conn)

conn.close()
log_progress("Server Connection closed")
