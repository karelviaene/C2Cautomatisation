print("Running ECHA-CHEM extraction")

#### Code to get C&L url based on a CAS number ####
import requests
import pandas as pd
from datetime import datetime
import tkinter as tk
from tkinter import filedialog
import os
import json
import sqlite3
import re

# Function to load in a file
def select_file():
    # Create a hidden root window
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    # Make sure the window appears on top (important for macOS)
    root.call('wm', 'attributes', '.', '-topmost', True)

    # Open the file selection dialog
    file_path = filedialog.askopenfilename(
        title="Select an input file",
        filetypes=[("XLSX files", "*.xlsx"), ("All files", "*.*")],
        initialdir="~"
    )
    # Return the selected file path or None
    return file_path

# Function to select target directory
def select_folder():
    # Create a hidden root window
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    # Make sure the window appears on top (important for macOS)
    root.call('wm', 'attributes', '.', '-topmost', True)

    # Open the file selection dialog
    folder_path = filedialog.askdirectory(
        title="Select folder",
    )
    # Return the selected folder path or None
    return folder_path

# Function to check if file was downloaded today
today = datetime.now().date()
def file_downloaded_today(file_path):
    # Check if the file exists
    if os.path.exists(file_path):
        # Get the file's modification time
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
        # Check if the file was modified today
        return file_mod_time == today
    return False


### SET UP ###
# Load the CSV file with CAS numbers
# print("Loading xlsx file")
# file_path = select_file()
# CASallpd = pd.read_excel(file_path)
# if "CAS" in CASallpd.columns:
#     CASall = [cas.strip() for cas in CASallpd['CAS'].dropna().tolist()] # Also remove white spaces per CAS
# else:
#     print("The 'CAS' column was not found in the Excel file.")

CASall =["37872-24-5", "8028-89-5"]
formatted_cas = [{"casNumber": cas, "ecNumber": ""} for cas in CASall]
formatted_ec = [{"casNumber": "", "ecNumber": ec} for ec in CASall]

print('Checking API')
url = "https://api.nextsds.com/echa"
headers = {
    "accept": "application/json",
    "Authorization": "Bearer b4077cae-b5b0-49a3-9c93-9925740adfe6",
    "Content-Type": "application/json"
}
data = formatted_cas
response = requests.post(url, headers=headers, json=data)

print("Status Code:", response.status_code)
print("Response JSON:", response.json())

# Save to a JSON file
currentdir = os.getcwd()
exportpath = os.path.join(currentdir,"output")
if not os.path.exists(exportpath):
    os.makedirs(exportpath)
formatted_time = datetime.now().strftime("%Y-%m-%d %H-%M")  # Customize format as needed
exportjson = os.path.join(exportpath, "CnLscreener exportJSON " + formatted_time +".json")
with open(exportjson, "w") as json_file:
    json.dump(response.json(), json_file, indent=2)


#### Upload info to CnL database ####

# Define the path to your SQLite database file
db_path = os.path.join("/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling/Database/C2Cdatabase.db")
C2Cfiles_path = "/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling"

#### Create/update C2C database with CAS numbers from Excel files ####

try:
    connection = sqlite3.connect(db_path)
    print("Connected to SQLite database at:", db_path)


    # NEED A GOOD WAY TO CONVERT THE API JSON TO SQL DATABASE


    # # Create table if not existing
    # cursor = connection.cursor()
    # cursor.execute('''
    #     CREATE TABLE IF NOT EXISTS MainOverview (
    #         ID TEXT PRIMARY KEY,
    #         LastUpdate TEXT NOT NULL,
    #         FileName TEXT NOT NULL,
    #         Comments TEXT NOT NULL
    #     )
    # ''')

    # Check if CAS number in database, update if available, otherwise add
    # print(response.json()["output"])
    # for output in response.json()["output"]:
    #     output
        # CASnr = output["casNumber"]
        # ECnr = output["ecNumber"]
        # if output["found"] != "false":  # Only add the info if it was found on ECHA-CHEM CnL
        #     print()
        #
        # else:
        #     print()

    # Load the JSON file into a DataFrame
    # CnL_df = pd.read_json('/Users/arche/Documents/Python/EDscreener/venv/output/EDscreener exportJSON 2025-07-30 15-51.json')  # Replace with your actual file path
    # CnL_df = pd.read_json('/Users/arche/Documents/Python/EDscreener/venv/output/EDscreener exportJSON 2025-07-30 15-51.json', orient='records')

    # CnL_df = pd.DataFrame(response.json()["output"])
    # print(CnL_df)
    #
    # CnL_df.to_sql('CnL', connection, if_exists='append', index=False)


    print("SQL updated")

# except sqlite3.Error as e:
#     print("SQLite error:", e, cas_number)


finally:
    if connection:
        connection.close()
        print("Connection closed.")
