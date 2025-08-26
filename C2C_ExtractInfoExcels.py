import sqlite3
import os
import re
import pandas as pd
import openpyxl
from datetime import datetime

# Define the path to your SQLite database file
db_path = os.path.join("/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling/Database/C2Cdatabase.db")
C2Cfiles_path = "/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling"

#### Loop through Excel files and extract information




#### Add info from Excel to C2C database  ####

# try:
#     connection = sqlite3.connect(db_path)
#     cursor = connection.cursor()
#     print("Connected to SQLite database at:", db_path)
#
#     # Check if CAS number already exists & if new entry is more recent than existing
#     cursor.execute('SELECT 1 FROM MainOverview WHERE CAS = ? AND LastUpdate > ?', (cas_number,last_update))
#     exists = cursor.fetchone()
#     if not exists:
#         cursor.execute('INSERT OR IGNORE INTO MainOverview (CAS,LastUpdate,FileName) VALUES (?,?,?)', (cas_number,last_update,filename))
#
#     connection.commit()
#     print("Table created or already exists.")
#
# except sqlite3.Error as e:
#     print("SQLite error:", e, cas_number)
#
# finally:
#     if connection:
#         connection.close()
#         print("Connection closed.")

