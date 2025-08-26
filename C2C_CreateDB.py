import sqlite3
import os
import re
import pandas as pd
import openpyxl
from datetime import datetime

# Define the path to your SQLite database file
db_path = os.path.join("/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling/Database/C2Cdatabase.db")
C2Cfiles_path = "/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling"

#### Create/update C2C database with CAS numbers from Excel files ####

try:
    connection = sqlite3.connect(db_path)
    print("Connected to SQLite database at:", db_path)

    # Create table if not existing
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS MainOverview (
            ID TEXT PRIMARY KEY,
            LastUpdate TEXT NOT NULL,
            FileName TEXT NOT NULL,
            Comments TEXT NOT NULL
        )
    ''')

    # Regex pattern to extract CAS number from filename
    file_pattern = re.compile(r'CAS (.*?)\.xlsx')
    cas_pattern = re.compile(r'CAS (\d{2,7}[-‐‑–—]\d{2,3}[-‐‑–—]\d{1})(.*?)\.xlsx', re.IGNORECASE)
    cas_pattern_strict = re.compile(r'CAS (\d{2,7}[-‐‑–—]\d{2,3}[-‐‑–—]\d{1})', re.IGNORECASE)
    ec_pattern = re.compile(r'EC (\d{2,7}[-‐‑–—]\d{3}[-‐‑–—]\d{1})')

    # Loop through Excel files with CAS number and add them to database
    for filename in os.listdir(C2Cfiles_path):
        full_path = os.path.join(C2Cfiles_path, filename)
        if os.path.isfile(full_path):
            match = file_pattern.search(filename)
            if match:
                # Get last modification time and format it as DD/MM/YYYY
                mod_time = os.path.getmtime(full_path)
                last_update = datetime.fromtimestamp(mod_time).strftime("%d/%m/%Y")

                # Extract CAS number or EC number if applicable
                match_inv = cas_pattern.search(filename)    # Check for CAS number
                comments = "There should be something here. Please check."
                if match_inv:     # If CAS
                    comments = "CAS"
                    if match_inv.group(2):  # If there is additional info after the CAS number, save it in comments
                        comments = "CAS, " +  match_inv.group(2)

                else:   # if no CAS, then check for EC
                    match_inv = ec_pattern.search(filename)
                    comments = "EC"
                if match_inv:   # If a standard format is found, save for use in the database
                    inv_number = match_inv.group(1)
                else:   # Else print the file for which there is an issue
                    print(filename)

                # print(inv_number)

                # Check if CAS number already exists & if new entry is more recent than existing
                cursor.execute('SELECT 1 FROM MainOverview WHERE ID = ? AND LastUpdate > ?', (inv_number,last_update))
                exists = cursor.fetchone()
                if not exists:
                    cursor.execute('INSERT OR IGNORE INTO MainOverview (ID, LastUpdate, FileName , Comments) VALUES (?,?,?,?)',
                                   (inv_number,last_update,filename,comments))

    connection.commit()
    print("SQL updated")

except sqlite3.Error as e:
    print("SQLite error:", e, inv_number)

finally:
    if connection:
        connection.close()
        print("Connection closed.")



#### Save Database as Excel ####

# Get current date in YYYY-MM-DD format
current_date = datetime.now().strftime("%Y-%m-%d")
# Connect to the SQLite database
connection = sqlite3.connect(db_path)
# Get a list of all tables in the database
tables = pd.read_sql_query("SELECT name FROM sqlite_master  WHERE type='table';", connection)

# Create a Pandas Excel writer using openpyxl
with pd.ExcelWriter(C2Cfiles_path + '/Database/C2Cdatabase ' + current_date + '.xlsx', engine='openpyxl') as writer:
    for table_name in tables['name']:
        # Read each table into a DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", connection)
        # Write the DataFrame to a sheet named after the table
        df.to_excel(writer, sheet_name=table_name, index=False)

# Close the database connection
connection.close()




