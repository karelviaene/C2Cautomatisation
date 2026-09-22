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
with open('/Users/juliakulpa/Desktop/Test_echa/NextSDS API key.txt') as creds:
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
C2Cpath = "/Users/juliakulpa/Desktop/Test_echa/Testing"
db_path = os.path.join(C2Cpath,"Database/C2Cdatabase.db")
C2Cfiles_path = os.path.join(C2Cpath,"CPS")

#### Create/update C2C database with CAS numbers from Excel files ####

try:
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
 #id INTEGER PRIMARY KEY AUTOINCREMENT,
    # Ensure ECHACHEM_CL table exists
    cursor.execute("PRAGMA foreign_keys = ON;")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ECHACHEM_CL (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,
        on_cl TEXT,
        cas TEXT,
        ec TEXT,
        name_echachem TEXT,
        type_classification TEXT,
        hazards TEXT,
        date_checked TEXT,
        FOREIGN KEY (code) REFERENCES C2C_DATABASE(ID)
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

        # Check if CAS already exists
        cursor.execute("SELECT 1 FROM ECHACHEM_CL WHERE cas = ?", (sqlinfo["cas"],))
        exists = cursor.fetchone()

        if exists:
            print(f"CAS {sqlinfo['cas']} already in database — skipping insert.")
            cursor.execute("SELECT 1 FROM ECHACHEM_CL WHERE cas = ? AND hazards = ?", (sqlinfo["cas"],sqlinfo["hazards"]))
            same_hazard = cursor.fetchone()
            if same_hazard:
                print(f"Hazard {sqlinfo['cas']} already in database — skipping insert.")
            else:
                print(f"Inserting CAS {sqlinfo['cas']}...")
                cursor.execute("""
                    UPDATE ECHACHEM_CL
                    SET hazards = ?, date_checked = ?
                    WHERE cas = ?
                """, (
                    sqlinfo["hazards"],
                    today,
                    sqlinfo["cas"]
                ))
                connection.commit()
                print("Hazards updated.")

        else:
            print(f"Inserting CAS {sqlinfo['cas']}...")
            cursor.execute(
                "INSERT INTO C2C_DATABASE (ID) VALUES (?)",
                (sqlinfo["code"],)
            ) # adds a CAS to the main database so it can create a key
            cursor.execute("""
                INSERT INTO ECHACHEM_CL (code, on_cl, cas, ec, name_echachem, type_classification, hazards, date_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sqlinfo["code"],
                sqlinfo["on_cl"],
                sqlinfo["cas"],
                sqlinfo["ec"],
                sqlinfo["name_echachem"],
                sqlinfo["type_classification"],
                sqlinfo["hazards"],
                today
            ))
            connection.commit()
            print("Insert complete.")

        cursor.execute("SELECT hazards FROM ECHACHEM_CL WHERE code = ?",
                                     (sqlinfo["code"],))

        row = cursor.fetchone()
        hazards_list = row[0].split(",") if row and row[0] else []
        print(hazards_list)

    print("SQL updated")

finally:
    if connection:
        connection.commit()
        connection.close()
        print("Connection closed.")
        print(CASall)


# second half of the code as a function
# #### Create/update C2C database with CAS numbers from Excel files ####
# def insert_json_info_to_DB(CnL_json, db_path, target_cas_list):
#     cas_hazards = {} # used later to create a list of things to update
#     with open(CnL_json, "r", encoding="utf-8") as f:
#         data = json.load(f)
#     try:
#         connection = sqlite3.connect(db_path)
#         cursor = connection.cursor()
#      #id INTEGER PRIMARY KEY AUTOINCREMENT,
#         # Ensure ECHACHEM_CL table exists
#         cursor.execute("PRAGMA foreign_keys = ON;")
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS ECHACHEM_CL (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             code TEXT NOT NULL,
#             on_cl TEXT,
#             cas TEXT,
#             ec TEXT,
#             name_echachem TEXT,
#             type_classification TEXT,
#             hazards TEXT,
#             date_checked TEXT,
#             FOREIGN KEY (code) REFERENCES C2C_DATABASE(ID)
#         )
#         """)
#
#
#         print("Connected to SQLite database at:", db_path)
#
#         today = datetime.now().date()
#
#         for target_cas in target_cas_list:
#
#             # Find the entry for the CAS you want
#             entry = next((e for e in data if e.get("casNumber") == target_cas), None)
#
#             if entry is None:
#                 print(f"CAS {target_cas} not found in JSON.")
#             else:
#                 # Set up dictionary to collect all relevant info
#                 sqlinfo = {
#                     "code": entry.get("casNumber"),
#                     "on_cl": "-",
#                     "cas": "-",
#                     "ec": "-",
#                     "name_echachem": "-",
#                     "type_classification": "-",
#                     "hazards": "-"
#                 }
#
#                 print(f"Testing for: {entry.get('casNumber')}")
#
#                 #### ECHA-CHEM C&L from NEXTSDS-API ####
#                 if entry.get("found") is False:  # If the chemical was NOT found on C&L
#                     sqlinfo["on_cl"] = "No"
#                 else:  # If the chemical was found on C&L
#                     sqlinfo["on_cl"] = "Yes"
#                     sqlinfo["cas"] = entry.get("cas")
#                     sqlinfo["ec"] = entry.get("ecNumber")
#                     sqlinfo["name_echachem"] = entry.get("name")
#
#                     if entry.get("isHarmonized") is True:
#                         sqlinfo["type_classification"] = "Harmonized"
#                     else:
#                         sqlinfo["type_classification"] = "Self-classification"
#
#                     # Safe hazards extraction (prevents crashes if hazards is missing/not a dict)
#                     hazards = entry.get("hazards", {})
#                     if isinstance(hazards, dict):
#                         sqlinfo["hazards"] = hazards.get("hazardClasses", "-")
#                 print(sqlinfo)
#
#         #     with open(CnL_json, "r", encoding="utf-8") as f:
#         #         data = json.load(f)
#         #
#         #         # If JSON is a list of entries
#         #         for entry in data:
#         #             print(entry)
#         #         # Set up dictionary to collect all relevant info
#         #         sqlinfo = {"code": entry.get("casNumber"), "on_cl": "-", "cas": "-", "ec": "-", "name_echachem": "-",
#         #                    "type_classification": "-", "hazards": "-"}
#         #         print(f"Adding chemical: {entry.get("casNumber")}")
#         #
#         #         #### ECHA-CHEM C&L from NEXTSDS-API ####
#         #         if entry.get("found") == False:  # If the chemical was NOT found on C&L
#         #             sqlinfo["on_cl"] = "No"
#         #         else:  # If the chemical was found on C&L (then there is no "found" entry)
#         #             sqlinfo["on_cl"] = "Yes"
#         #             sqlinfo["cas"] = entry.get("cas")
#         #             sqlinfo["ec"] = entry.get("ecNumber")
#         #             sqlinfo["name_echachem"] = entry.get("name")
#         #             if entry.get("isHarmonized") == True:
#         #                 sqlinfo["type_classification"] = "Harmonized"
#         #             else:
#         #                 sqlinfo["type_classification"] = "Self-classification"
#         #             sqlinfo["hazards"] = entry.get("hazards")["hazardClasses"]
#         #
#                 # Check if CAS already exists
#                 cursor.execute("SELECT 1 FROM ECHACHEM_CL WHERE cas = ?", (sqlinfo["cas"],))
#                 exists = cursor.fetchone()
#                 if exists:
#                     print(f"CAS {sqlinfo['cas']} already in database")
#                     cursor.execute("SELECT 1 FROM ECHACHEM_CL WHERE cas = ? AND hazards = ?", (sqlinfo["cas"],sqlinfo["hazards"]))
#                     same_hazard = cursor.fetchone()
#                     if same_hazard:
#                         print(f"Hazards for {sqlinfo['cas']} are the same as for the last update. NO ACTION NEEDED.")
#                     else:
#                         print(f"Inserting CAS {sqlinfo['cas']}...")
#                         cursor.execute("""
#                             UPDATE ECHACHEM_CL
#                             SET hazards = ?, date_checked = ?
#                             WHERE cas = ?
#                         """, (
#                             sqlinfo["hazards"],
#                             today,
#                             sqlinfo["cas"]
#                         ))
#                         connection.commit()
#                         print(f"Hazards for {sqlinfo['cas']} are DIFFERENT as for the last update. INFO IN TABLE CnL UPDATED. ACTION REQUIRED.")
#
#                         ### Needed for the next step to gather info and update
#                         cursor.execute("SELECT hazards FROM ECHACHEM_CL WHERE code = ?",
#                                        (sqlinfo["code"],))
#                         # needed if info was added
#                         row = cursor.fetchone()
#                         hazards_list = row[0].split(",") if row and row[0] else []
#                         cas_hazards[target_cas] = hazards_list
#                         print(hazards_list)
#
#
#                 else:
#                     print(f"CAS not in CnL database: {sqlinfo['cas']}")
#                     cursor.execute(
#                         "SELECT 1 FROM C2C_DATABASE WHERE ID = ?",
#                         (sqlinfo["code"],)
#                     )
#                     exists = cursor.fetchone()
#                     if not exists:
#                         cursor.execute(
#                             "INSERT INTO C2C_DATABASE (ID) VALUES (?)",
#                             (sqlinfo["code"],)
#                         )
#                         print(f"CAS was not in the main C2C database. CAS added to the main C2C database: {sqlinfo['cas']}")
#                     else:
#                         print(f"CAS already exists in the main C2C database: {sqlinfo['cas']}")
#
#                     cursor.execute("""
#                         INSERT INTO ECHACHEM_CL (code, on_cl, cas, ec, name_echachem, type_classification, hazards, date_checked)
#                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#                     """, (
#                         sqlinfo["code"],
#                         sqlinfo["on_cl"],
#                         sqlinfo["cas"],
#                         sqlinfo["ec"],
#                         sqlinfo["name_echachem"],
#                         sqlinfo["type_classification"],
#                         sqlinfo["hazards"],
#                         today
#                     ))
#                     connection.commit()
#                     print(f"Information inserted to CnL database: {sqlinfo['cas']}")
#                     ### Needed for the next step to gather info and update
#                     cursor.execute("SELECT hazards FROM ECHACHEM_CL WHERE code = ?",
#                                                  (sqlinfo["code"],))
#                     #needed if info was added
#                     row = cursor.fetchone()
#                     hazards_list = row[0].split(",") if row and row[0] else []
#                     cas_hazards[target_cas] = hazards_list
#                     print(hazards_list)
#
#
#         print("SQL updated")
#         return cas_hazards
#     #
#     finally:
#         if connection:
#             connection.commit()
#             connection.close()
#             print("Connection closed.")
#
# list = insert_json_info_to_DB(CnL_json, db_path, CASall)
# print(list)