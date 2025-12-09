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
API_key = '/Users/juliakulpa/Desktop/Test_echa/NextSDS API key.txt'

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

# Uploading the database location
if database_location is not None:
    db_path = database_location.read().decode("utf-8").strip()
    if os.path.isfile(db_path):
        st.success(f"Uploaded database directory: {db_path}")
    else:
        st.success(f":red[Database directory does not exist!!! Run button will only appear if you put the correct database directory]")

### FUNCTIONS
def check_jason(CASall, API_key):
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
    currentdir = os.getcwd() # will have to check what this is and decide how to change it
    exportpath = os.path.join(currentdir,"output")
    st.write(exportpath)
    if not os.path.exists(exportpath):
        os.makedirs(exportpath)
    formatted_time = datetime.now().strftime("%Y-%m-%d %H-%M")  # Customize format as needed
    exportjson = os.path.join(exportpath, "CnLscreener exportJSON " + formatted_time +".json")
    with open(exportjson, "w") as json_file:
        json.dump(CnL_json, json_file, indent=2)
    return CnL_json
def checing_if_CAS_exists(CASall, db_path):
    try:
        connection = sqlite3.connect(db_path)
        #st.success(f"Connected to SQLite database at: {db_path}")
        cursor = connection.cursor()
        found = []  # CAS numbers that exist in the DB
        not_found = []  # CAS numbers that do NOT exist
        for cas in CASall:
            cursor.execute("SELECT 1 FROM C2C_DATABASE WHERE ID = ?", (cas,))
            row = cursor.fetchone()

            if row:
                found.append(cas)
            else:
                not_found.append(cas)

        st.success(f"CAS found in database: {found}")
        st.success(f"CAS not in database: {not_found}")
    except sqlite3.Error as e:
        st.write("SQLite error:", e)

#if connceted to database you can press run
if os.path.isfile(db_path):
    if st.button(":green[Run]"):
        try:
            # jason files download:
            #check_jason(CASall, API_key)

            ### if the CAS exists
            checing_if_CAS_exists(CASall,db_path)

            ### for CAS numbers that are not in the database



            #cursor.execute("SELECT 1 FROM C2C_DATABASE WHERE ID = ?", (sqlinfo["ID"]))



        except sqlite3.Error as e:
            print("SQLite error", e)

