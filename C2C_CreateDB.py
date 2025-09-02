### This script goes to the CAS directory, checks all CAS files present and add them to the SQLite database.

import sqlite3
import os
import re
import pandas as pd
import openpyxl
from datetime import datetime

# Define the path to your SQLite database file
C2Cpath = "/Users/arche/Documents/Python/C2Cautomatisation/Testing"
db_path = os.path.join(C2Cpath,"Database/C2Cdatabase.db")
C2Cfiles_path = os.path.join(C2Cpath,"CPS")
# db_path = os.path.join("/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling/Database/C2Cdatabase.db")
# C2Cfiles_path = "/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling"

# Custom function for extracting info from Excel: it takes all the info below a certain cell until an empty string is reached
# It then adds the info to a new SQL database connected to the main database
def add_info_CPS(sheet, search_strings, maindatabase, newdatabase, mainID):
    """
    For each row in the worksheet `sheet`, searches for cells containing
    any of the `search_strings`. If found, collects the non-empty values
    from those cells in that row and inserts them into `newdatabase`,
    linked to `maindatabase`.

    Stops processing when all searched columns in a row are empty.

    Arguments:
        sheet          -- openpyxl worksheet
        search_strings -- list of strings to search for in the first row (headers)
        maindatabase   -- name of main database table (for foreign key reference)
        newdatabase    -- name of new database table
        mainID         -- ID value to associate with inserted rows
    """
    # Find the header row (assume first row has headers)
    headers = [cell.value for cell in sheet[1]]
    col_indices = {}

    for s in search_strings:
        if s in headers:
            col_indices[s] = headers.index(s) + 1  # +1 because openpyxl is 1-based
        else:
            raise ValueError(f"'{s}' not found in header row.")

    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                   (newdatabase,))
    table_exists = cursor.fetchone()

    if not table_exists:
        # Create the table with one column per search string
        cols_def = ", ".join([f"{s} TEXT" for s in search_strings])
        cursor.execute(f'''
            CREATE TABLE {newdatabase} (
                ID TEXT,
                {cols_def},
                FOREIGN KEY (ID) REFERENCES {maindatabase}(ID)
            )
        ''')
    else:
        # Check if all needed columns exist
        cursor.execute(f"PRAGMA table_info({newdatabase})")
        existing_cols = [col[1] for col in cursor.fetchall()]
        for s in search_strings:
            if s not in existing_cols:
                cursor.execute(f"ALTER TABLE {newdatabase} ADD COLUMN {s} TEXT")

    # Iterate over rows below the header
    for row in sheet.iter_rows(min_row=2, values_only=True):
        row_data = {}
        for s, col_idx in col_indices.items():
            value = row[col_idx - 1]
            if value is not None and str(value).strip() != "":
                row_data[s] = value

        if not row_data:
            # Stop processing entirely if all searched columns are empty
            break

            # Insert into database
        placeholders = ", ".join(["?"] * (len(row_data) + 1))
        cols = ", ".join(["ID"] + list(row_data.keys()))
        cursor.execute(
            f"INSERT INTO {newdatabase} ({cols}) VALUES ({placeholders})",
            [mainID] + list(row_data.values())
        )

def add_info_CPS(sheet, search_strings, maindatabase, newdatabase, mainID):
    """
    Finds each search string somewhere in the sheet (not necessarily in the first row),
    then reads the values directly below it column-wise.
    Collects one "row" of data across all search strings,
    and inserts into the SQLite database.

    Stops when *all* searched columns are empty in the same row.

    search_strings:
        - list: Excel labels are used as SQL column names.
        - dict: {Excel label: SQL column name}
    """

    # Handle both list and dict
    if isinstance(search_strings, str):
        search_strings = [search_strings]
    if isinstance(search_strings, list):
        mapping = {s: s for s in search_strings}  # Excel label -> same SQL name
    elif isinstance(search_strings, dict):
        mapping = search_strings  # Excel label -> custom SQL name
    else:
        raise TypeError("search_strings must be a str, list, or dict")

    # Locate the target cells (positions of the labels in Excel)
    col_positions = {}
    for excel_label in mapping.keys():
        found = None
        for row in sheet.iter_rows():
            for cell in row:
                if cell.value == excel_label:
                    found = cell
                    break
            if found:
                break
        if not found:
            raise ValueError(f"'{excel_label}' not found in the worksheet.")
        col_positions[excel_label] = (found.column, found.row + 1)  # start below label

    # Quote identifiers for SQL safety
    def q(name: str) -> str:
        return f'"{name}"'

    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                   (newdatabase,))
    table_exists = cursor.fetchone()

    if not table_exists:
        # Create the table with one column per SQL name
        cols_def = ", ".join([f"{q(sql_col)} TEXT" for sql_col in mapping.values()])
        cursor.execute(f'''
            CREATE TABLE {q(newdatabase)} (
                ID TEXT,
                {cols_def},
                FOREIGN KEY (ID) REFERENCES {q(maindatabase)}(ID)
            )
        ''')
    else:
        # Add missing columns if needed
        cursor.execute(f"PRAGMA table_info({q(newdatabase)})")
        existing_cols = [col[1] for col in cursor.fetchall()]
        for sql_col in mapping.values():
            if sql_col not in existing_cols:
                cursor.execute(f"ALTER TABLE {q(newdatabase)} ADD COLUMN {q(sql_col)} TEXT")

    # Iterate row by row until all searched columns are empty
    row_offset = 0
    while True:
        row_data = {}
        all_empty = True
        for excel_label, (col, start_row) in col_positions.items():
            cell = sheet.cell(row=start_row + row_offset, column=col)
            if cell.value is not None and str(cell.value).strip() != "":
                sql_col = mapping[excel_label]
                row_data[sql_col] = cell.value
                all_empty = False
        if all_empty:
            break  # stop when all searched columns are empty in the same row

        # Insert row into database
        placeholders = ", ".join(["?"] * (len(row_data) + 1))
        cols = ", ".join([q("ID")] + [q(c) for c in row_data.keys()])
        cursor.execute(
            f"INSERT INTO {q(newdatabase)} ({cols}) VALUES ({placeholders})",
            [mainID] + list(row_data.values())
        )

        row_offset += 1

#### Create/update C2C database with CAS numbers from Excel files ####

try:
    connection = sqlite3.connect(db_path)
    print("Connected to SQLite database at:", db_path)

    # Create table if not existing
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS C2C_DATABASE (
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
                #### Update database: general info #####
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


                #### Update database: extract info on CAS from file if new/more recent #####
                cursor.execute('SELECT 1 FROM C2C_DATABASE WHERE ID = ? AND LastUpdate > ?', (inv_number, last_update))
                exists = cursor.fetchone()
                if not exists:  # If there is no info or there is more recent info
                    # Update general entry in the database
                    cursor.execute('INSERT OR IGNORE INTO C2C_DATABASE (ID, LastUpdate, FileName , Comments) VALUES (?,?,?,?)',
                                   (inv_number,last_update,filename,comments))

                    # Create empty dictionary to save the info in
                    cheminfo = []
                    # Open the Excel file
                    CPS_wb_obj = openpyxl.load_workbook(full_path)
                    CPSsheet = CPS_wb_obj.active

                    # add_info_CPS(sheet, search_string, maindatabase, newdatabase, mainID)

                    # CHEMICAL NAME
                    add_info_CPS(CPSsheet, ["Chemical name"],"C2C_DATABASE","CHEMICALNAMES",inv_number)

                    # ASSESSOR
                    add_info_CPS(CPSsheet, {"Name assessor":"Name assessor","Date" : "Date assessed"},"C2C_DATABASE","ASSESSORS",inv_number)

                    # CHECKED status
                    add_info_CPS(CPSsheet, ["Checked"],"C2C_DATABASE","Checked",inv_number)

    connection.commit()
    print("SQL updated")

except sqlite3.Error as e:
    print("SQLite error:", e, inv_number)

finally:
    if connection:
        connection.close()
        print("Connection closed.")


#### Save Database as Excel ####

# # Get current date in YYYY-MM-DD format
# current_date = datetime.now().strftime("%Y-%m-%d")
# # Connect to the SQLite database
# connection = sqlite3.connect(db_path)
# # Get a list of all tables in the database
# tables = pd.read_sql_query("SELECT name FROM sqlite_master  WHERE type='table';", connection)
#
# # Create a Pandas Excel writer using openpyxl
# with pd.ExcelWriter(C2Cpath + '/Database/C2Cdatabase ' + current_date + '.xlsx', engine='openpyxl') as writer:
#     for table_name in tables['name']:
#         # Read each table into a DataFrame
#         df = pd.read_sql_query(f"SELECT * FROM {table_name}", connection)
#         # Write the DataFrame to a sheet named after the table
#         df.to_excel(writer, sheet_name=table_name, index=False)
#
# # Close the database connection
# connection.close()




