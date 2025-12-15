import streamlit as st
import pandas as pd
import sqlite3
import os
import re
import pandas as pd
import openpyxl
from datetime import datetime
import traceback
import zipfile
import re
import requests
from bs4 import BeautifulSoup
from io import BytesIO, StringIO
import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter
import logging
import zipfile
import random
import time
from datetime import datetime
import json
import copy

from streamlit import empty

st.title("ARCHE CAS database")
# text
st.markdown(
    """ 
    Connects automatically to the database.
    But before anything:
    """)
# green color:)
st.markdown("""
    <style>
    .stApp {
        background-color: #90EE90;
    }
    </style>
""", unsafe_allow_html=True)
# useless button
if st.button(":green[Press this button if you need some cheering up]"):
    st.balloons()
    st.toast("You can do this!", icon="🎉")
st.markdown("""
<style>
div.stButton > button {
    background-color: #0BDA51;     
    color: white;                 
    border-radius: 8px;
    border: none;
    padding: 0.6em 1.2em;
}
/* HOVER */
div.stButton > button:hover {
    background-color: #45a049;
    color: white;
}
""", unsafe_allow_html=True)
# CAS file upload
st.header(
    """ 
    Upload excel file with CAS numbers
    """)
uploaded_file = st.file_uploader("Upload Excel file with CAS: A column with name CAS containing all CAS/EC numbers to screen in individual rows below. This should be on the first sheet.", type=["xlsx", "xlsm"])
database_location = st.file_uploader("Upload a text file with database location in .txt", type=["txt"])
#st.selectbox
# save api key
API_key = '/Users/juliakulpa/Desktop/Test_echa/NextSDS API key.txt'
# folder with excels
folder_excels = "/Users/juliakulpa/Desktop/test/CPS"
# directory to save JSON
save_json_dirr = "/Users/juliakulpa/Desktop/test/JSON"

# Uploading the excel with CAS numbers
if uploaded_file is not None:
    # write if the file was uploaded
    CASallpd = pd.read_excel(uploaded_file)
    if "CAS" in CASallpd.columns:
        CASall = [cas.strip() for cas in CASallpd['CAS'].dropna().tolist()]  # Also remove white spaces per CAS
    else:
        st.success("The 'CAS' column was not found in the Excel file.")

    # CASall =["37872-24-5", "8028-89-5"]
    formatted_cas = [{"casNumber": cas, "ecNumber": ""} for cas in CASall]
    formatted_ec = [{"casNumber": "", "ecNumber": ec} for ec in CASall]

    st.success(f"Uploaded excel file with {len(CASall)} CAS numbers: {', '.join(CASall)}")

#starting with no path and then uploading it
db_path = 0
# Uploading the database location
if database_location is not None:
    db_path = database_location.read().decode("utf-8").strip()
    if os.path.isfile(db_path):
        st.success(f"Uploaded database directory: {db_path}")
    else:
        st.success(f":red[Database directory does not exist!!! Run button will only appear if you put the correct database directory]")

### FUNCTIONS
def check_json(CASall, API_key, save_json_dirr):
    st.write('Checking API')

    # Load the API key from file: It's on the dropbox under Science/Data searches/ED screener/input databases/NextSDS API key.txt
    with open(API_key) as creds:
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
                st.write(f"Chunk {idx + 1}: Job submitted successfully: {job_id}")
            else:
                st.write(f"Chunk {idx + 1}: Failed to submit job")
        except Exception as e:
            st.write(f"Chunk {idx + 1}: Exception during job submission: {str(e)}")

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
                    st.write(f"Chunk {job['index']}: Job status: {job_status}")
                    if job_status not in ["STARTED", "EXECUTING"]:
                        job["done"] = True
                        job["output"] = status_data.get("output", [])
                elif status_response.status_code in [400, 404]:
                    st.write(f"Chunk {job['index']}: Job error ({status_response.status_code})")
                    job["done"] = True
            except Exception as e:
                st.write(f"Chunk {job['index']}: Exception during status check: {str(e)}")

    # Step 3: Combine all outputs
    CnL_json = []
    for job in jobs:
        if job["output"]:
            CnL_json.extend(job["output"])

    # Save to a JSON file
    exportpath = os.path.join(save_json_dirr,"output")
    st.write("Save to:",exportpath)
    if not os.path.exists(exportpath):
        os.makedirs(exportpath)
    formatted_time = datetime.now().strftime("%Y-%m-%d %H-%M")  # Customize format as needed
    exportjson = os.path.join(exportpath, "CnLscreener exportJSON " + formatted_time +".json")
    with open(exportjson, "w") as json_file:
        json.dump(CnL_json, json_file, indent=2)
    return CnL_json
def checking_if_CAS_exists(CASall, db_path):
    found = []
    not_found = []

    try:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        for cas in CASall:
            cursor.execute("SELECT 1 FROM C2C_DATABASE WHERE ID = ?", (cas,))
            row = cursor.fetchone()

            if row:
                found.append(cas)
            else:
                not_found.append(cas)

    except sqlite3.Error as e:
        st.write("SQLite error:", e)

    finally:
        if 'connection' in locals():
            connection.close()

    return found, not_found
def check_if_excel_is_in_folder(folder_excels, CAS_list):
    CAS_in_folder = []
    CAS_not_in_folder = []

    file_pattern = re.compile(r'CAS (.*?)\.(xlsx|xlsm)$')
    cas_pattern = re.compile(r'CAS (\d{2,7}[-‐-–—]\d{2,3}[-‐-–—]\d{1})(.*?)\.(xlsx|xlsm)$', re.IGNORECASE)
    ec_pattern = re.compile(r'EC (\d{2,7}[-‐-–—]\d{3}[-‐-–—]\d{1})')

    # collect all inventory numbers (CAS) found in the folder
    inv_in_folder = set()

    for filename in os.listdir(folder_excels):
        full_path = os.path.join(folder_excels, filename)
        if os.path.isfile(full_path):
            match = file_pattern.search(filename)
            if match:
                # Extract CAS inventory number (inv_number)
                match_inv = cas_pattern.search(filename)
                if match_inv:
                    inv_number = match_inv.group(1)  # this is the CAS
                    inv_in_folder.add(inv_number)
                else:
                    # if no CAS, then check for EC (kept from your code)
                    match_inv = ec_pattern.search(filename)
                    if match_inv:
                        inv_number = match_inv.group(1)
                    else:
                        st.write(f"Issue with: {filename}")

    # compare input CAS_list against what was found in folder
    for cas in CAS_list:
        if cas in inv_in_folder:
            CAS_in_folder.append(cas)
        else:
            CAS_not_in_folder.append(cas)

    return CAS_in_folder, CAS_not_in_folder
#if connceted to database you can press run
if os.path.isfile(db_path):
    if st.button(":green[Run]"):
        ### Beginning: before starting download all available json files
        # json files download (first step to get new CnL info):
        json = check_json(CASall, API_key, save_json_dirr)

        ### if the CAS exists and make a list with existing cas and the ones that have to be created
        # found => CAS exists in DB
        # not_found => CAS not in DB
        found, not_found = checking_if_CAS_exists(CASall, db_path)
        st.write(f"CAS found in database: {', '.join(found)}")
        st.write(f"CAS not in database: {', '.join(not_found)}")

        ### for CAS numbers that are NOT in the database (not_found)
        # check if there is an Excel file with the given CAS
        CAS_in_folder_1, CAS_not_in_folder_1 = check_if_excel_is_in_folder(folder_excels, not_found)
        st.write(f"CAS found as an Excel: {', '.join(CAS_in_folder_1)}")
        st.write(f"CAS not found as an Excel: {', '.join(CAS_not_in_folder_1)}")

        ### for CAS numbers that are in the database (found)
        # check if there is an Excel file with the given CAS
        CAS_in_folder, CAS_not_in_folder = check_if_excel_is_in_folder(folder_excels, found)
        st.write(f"CAS found as an Excel: {', '.join(CAS_in_folder)}")
        if CAS_not_in_folder != []:
            st.write(f":red[Those CAS are in DB but not as Excel files: {', '.join(CAS_not_in_folder)}]")






