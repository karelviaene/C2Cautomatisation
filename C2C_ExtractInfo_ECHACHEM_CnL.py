#### This script collects all the C&L info from ECHA-CHEM for a list of CAS (loaded from an Excel) and
# stores it in a json file. It also adds key information to the ECHACHEM_CL SQLite database

#### Code to get C&L url based on a CAS number ####
import requests
import pandas as pd
from datetime import datetime
import tkinter as tk
from tkinter import filedialog
import os
import json
import sqlite3
import time

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
print("Loading xlsx file")
file_path = select_file()
CASallpd = pd.read_excel(file_path)
if "CAS" in CASallpd.columns:
    CASall = [cas.strip() for cas in CASallpd['CAS'].dropna().tolist()] # Also remove white spaces per CAS
else:
    print("The 'CAS' column was not found in the Excel file.")

# CASall =["37872-24-5", "8028-89-5"]
formatted_cas = [{"casNumber": cas, "ecNumber": ""} for cas in CASall]
formatted_ec = [{"casNumber": "", "ecNumber": ec} for ec in CASall]

print('Checking API')

# Load the API key from file: It's on the dropbox under Science/Data searches/ED screener/input databases/NextSDS API key.txt
with open('/Users/arche/Arche Dropbox/Science/Data searches/ED screener/input databases/NextSDS API key.txt') as creds:
    api_key = creds.readlines()[0]  # API key is on the first line

# Split up list in smaller parts (chunks)
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

start_url = "https://api.nextsds.com/jobs/start"
status_url = "https://api.nextsds.com/jobs/retrieve"
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}
# Step 1: Submit all jobs
jobs = []
for idx, cas_chunk in enumerate(chunk_list(CASall, 250)):
    data = {
        "taskId": "echa-api",
        "payload": cas_chunk
    }
    try:
        response = requests.post(start_url, headers=headers, json=data)
        if response.status_code == 200:
            job_id = response.json().get("id")
            jobs.append({"id": job_id, "index": idx + 1, "done": False, "output": None})
            print(f"Chunk {idx + 1}: Job submitted successfully: {job_id}")
        else:
            print(f"Chunk {idx + 1}: Failed to submit job")
    except Exception as e:
        print(f"Chunk {idx + 1}: Exception during job submission: {str(e)}")

# Step 2: Monitor all jobs in one loop
while not all(job["done"] for job in jobs):
    time.sleep(10)
    for job in jobs:
        if job["done"]:
            continue
        try:
            status_response = requests.get(f"{status_url}/{job['id']}", headers=headers)
            if status_response.status_code == 200:
                status_data = status_response.json()
                job_status = status_data.get("status")
                print(f"Chunk {job['index']}: Job status: {job_status}")
                if job_status not in ["STARTED", "EXECUTING"]:
                    job["done"] = True
                    job["output"] = status_data.get("output", [])
            elif status_response.status_code in [400, 404]:
                print(f"Chunk {job['index']}: Job error ({status_response.status_code})")
                job["done"] = True
        except Exception as e:
            print(f"Chunk {job['index']}: Exception during status check: {str(e)}")

# Step 3: Combine all outputs
CnL_json = []
for job in jobs:
    if job["output"]:
        CnL_json.extend(job["output"])

# Save to a JSON file
currentdir = os.getcwd()
exportpath = os.path.join(currentdir,"output")
if not os.path.exists(exportpath):
    os.makedirs(exportpath)
formatted_time = datetime.now().strftime("%Y-%m-%d %H-%M")  # Customize format as needed
exportjson = os.path.join(exportpath, "CnLscreener exportJSON " + formatted_time +".json")
with open(exportjson, "w") as json_file:
    json.dump(CnL_json, json_file, indent=2)


#### Upload info to CnL database ####

# Define the path to your SQLite database file
# db_path = os.path.join("/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling/Database/C2Cdatabase.db")
# C2Cfiles_path = "/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling"
C2Cpath = "/Users/arche/Documents/Python/C2Cautomatisation/Testing"
db_path = os.path.join(C2Cpath,"Database/C2Cdatabase.db")
C2Cfiles_path = os.path.join(C2Cpath,"CPS")

#### Create/update C2C database with CAS numbers from Excel files ####

try:
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Ensure ECHACHEM_CL table exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ECHACHEM_CL (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT ,
        on_cl TEXT,
        cas TEXT,
        ec TEXT,
        name_echachem TEXT,
        type_classification TEXT,
        hazards TEXT,
        FOREIGN KEY (code) REFERENCES DATABASE_C2C(ID)
    )
    """)


    print("Connected to SQLite database at:", db_path)

    for entry in CnL_json:
        print(entry)
        # Set up dictionary to collect all relevant info
        sqlinfo = {"code": entry.get("casNumber"), "on_cl": "-", "cas": "-", "ec": "-", "name_echachem": "-",
                   "type_classification": "-", "hazards": "-"}
        print(f"Adding chemical: {entry.get("casNumber")}")

        #### ECHA-CHEM C&L from NEXTSDS-API ####
        if entry.get("found") == False:  # If the chemical was NOT found on C&L
            sqlinfo["on_cl"] = "No"
        else:  # If the chemical was found on C&L (then there is no "found" entry)
            sqlinfo["on_cl"] = "Yes"
            sqlinfo["cas"] = entry.get("cas")
            sqlinfo["ec"] = entry.get("ecNumber")
            sqlinfo["name_echachem"] = entry.get("name")
            if entry.get("isHarmonized") == True:
                sqlinfo["type_classification"] = "Harmonized"
            else:
                sqlinfo["type_classification"] = "Self-classification"
            sqlinfo["hazards"] = entry.get("hazards")["hazardClasses"]

        # Insert into ECHACHEM_CL
        print(sqlinfo)
        cursor.execute("""
        INSERT INTO ECHACHEM_CL (code, on_cl, cas, ec, name_echachem, type_classification, hazards)
        VALUES (?, ?, ?, ?, ?, ?, ?)    """,
        (sqlinfo["code"],sqlinfo["on_cl"],sqlinfo["cas"],sqlinfo["ec"],sqlinfo["name_echachem"],sqlinfo["type_classification"],sqlinfo["hazards"]))

    print("SQL updated")

finally:
    if connection:
        connection.commit()
        connection.close()
        print("Connection closed.")
