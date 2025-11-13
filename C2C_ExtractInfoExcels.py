### This script goes to the CPS directory, checks all CAS files present and adds the info to the SQLite database.

#### SET UP ####
import sqlite3
import os
import re
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook

# Define the path to your SQLite database file
C2Cpath = "/Users/juliakulpa/Desktop/Test_excel_imports"
C2Cfiles_path = os.path.join(C2Cpath,"CPS")
# C2Cpath = "/Users/arche/Arche Dropbox/C2C/08_Chemical Profiling/"
# C2Cfiles_path = os.path.join(C2Cpath)
db_path = os.path.join(C2Cpath,"Database/C2Cdatabase.db")
# /Users/juliakulpa/Desktop/Test_excel_imports/Database/C2Cdatabase.db
print(db_path)
# # LOAD EXCEL CPS TEMPLATE
template_path = "/Users/juliakulpa/Desktop/Test_excel_imports/Template/CPS_CAS TEMPLATE_V1.xlsm"
template_wb = load_workbook(template_path, read_only=False, keep_vba=True)
ws_template = template_wb["C2Coverview"]


### CUSTOM FUNCTIONS ###

def db_to_excel_one_below(table_name, column_to_get,lookup_column,lookup_value,label_excel):
    # Query the database
    query = f"SELECT [{column_to_get}] FROM {table_name} WHERE {lookup_column} = ?"
    cursor.execute(query, (lookup_value,))
    result = cursor.fetchone()
    if not result:
        print(f"No result found for {lookup_column} = {lookup_value}")
        return
    value_to_insert = result[0]
    # Find the label in the worksheet and insert the value below it
    for row in ws_template.iter_rows():
        for cell in row:
            if cell.value and isinstance(cell.value, str) and label_excel.lower() in cell.value.lower():
                ws_template.cell(row=cell.row + 1, column=cell.column).value = value_to_insert
                print(f"Inserted '{value_to_insert}' below '{label_excel}' in cell {cell.coordinate}")
                return
    print(f"Label '{label_excel}' not found in worksheet.")

def db_to_excel_x_right(table_name, columns_to_get, lookup_column, lookup_value, labels_excel,offset):
    # Ensure both inputs are lists
    if isinstance(columns_to_get, str):
        columns_to_get = [columns_to_get]
    if isinstance(labels_excel, str):
        labels_excel = [labels_excel]

    # Query the database for multiple columns
    query = f"SELECT {', '.join(f'[{col}]' for col in columns_to_get)} FROM {table_name} WHERE {lookup_column} = ?"
    cursor.execute(query, (lookup_value,))
    result = cursor.fetchone()
    if not result:
        print(f"No result found for {lookup_column} = {lookup_value}")
        return
    # Map column -> value from database
    col_value_map = dict(zip(columns_to_get, result))

    # Loop over each label and insert corresponding column value
    for col_name, label_excel in zip(columns_to_get, labels_excel):
        value_to_insert = col_value_map[col_name]
        inserted = False
        for row in ws_template.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str) and label_excel.lower() in cell.value.lower():
                    # Add the value in the column x (= offset) columns to the right
                    ws_template.cell(row=cell.row , column=cell.column + offset).value = value_to_insert
                    print(f"Inserted '{value_to_insert}' to the right of '{label_excel}' in cell {cell.coordinate}")
                    inserted = True
                    break
            if inserted:
                break
        if not inserted:
            print(f"Label '{label_excel}' not found in worksheet.")

def db_to_excel_multiple_below(maindb, main_ref, linked_db, link_ref, column_to_get, lookup_column, lookup_value, label_excel):

    # Query the database for all matching values
    try:
        query = f"""
             SELECT a.[{column_to_get}]
             FROM {linked_db} a
             JOIN {maindb} c ON a.{link_ref} = c.{main_ref}
             WHERE c.{lookup_column} = ?
         """
        cursor.execute(query, (lookup_value,))
        results = cursor.fetchall()
        if not results:
            print(f"No results found for {lookup_column} = {lookup_value}")
            return

        # Find the label in the worksheet
        for row in ws_template.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str) and label_excel.lower() in cell.value.lower():
                    start_row = cell.row + 1
                    col = cell.column

                    # Insert each value below the label, adding rows if needed
                    for i, result in enumerate(results):
                        target_row = start_row + i
                        row_cells = ws_template[target_row+1]

                        # Check if all cells in the row are empty
                        if all(cell.value in (None, '') for cell in row_cells):
                            # Row exists and is empty — reuse it
                            ws_template.cell(row=target_row, column=col).value = result[0]
                        else:
                            # Row has data — insert a new row
                            ws_template.insert_rows(target_row)
                            ws_template.cell(row=target_row, column=col).value = result[0]

                        print(f"Inserted '{result[0]}' into cell {ws_template.cell(row=start_row + i, column=col).coordinate}")
                    return

        print(f"Label '{label_excel}' not found in worksheet.")
    except sqlite3.Error as e:
        print("SQLite error:", e)

def refdb_to_excel_source_right(maindb, main_ref, linked_db, link_ref, column_to_get, lookup_column, lookup_value, label_excel,offset):

    # Query the database for all matching values
    try:
        query = f"""
             SELECT a.[{column_to_get}]
             FROM {linked_db} a
             JOIN {maindb} c ON a.{link_ref} = c.{main_ref}
             WHERE c.{lookup_column} = ?
         """
        cursor.execute(query, (lookup_value,))
        results = cursor.fetchall()
        if not results:
            print(f"No results found for {lookup_column} = {lookup_value}")
            return

        # Find the label in the worksheet
        for row in ws_template.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str) and label_excel.lower() in cell.value.lower():
                    start_row = cell.row
                    col = cell.column + offset
                    for i,result in enumerate(results):
                        target_row = start_row  # insert to the right
                        if result[0] != None:
                            ws_template.cell(row=target_row, column=col).value = result[0]
                            print(f"Inserted '{result[0]}' into cell {ws_template.cell(row=start_row, column=col).coordinate}")
                    return

        print(f"Label '{label_excel}' not found in worksheet.")
    except sqlite3.Error as e:
        print("SQLite error:", e)



try:
    ### SQL SET-UP
    connection = sqlite3.connect('/Users/juliakulpa/Desktop/Test_excel_imports/Database /C2Cdatabase.db')
    cursor = connection.cursor()

    print("Connected to SQLite database at:", db_path)

    CAS = "10-00-0"

    # GENERAL INFO

    # db_to_excel_one_below(table_name="C2C_DATABASE",column_to_get="Common name",lookup_column="ID",lookup_value=CAS,
    # label_excel="Common name")

    # GENERAL INFO: MULTIPLE CELLS
    # namesDBcols = ["","","","",
    #                "","","","Name assessor","Date assessed"]
    # namesExcel = ["Molecular Formula or chemical picture","Chemical name","Common name","CAS number",
    #                "Linked CAS Read across","Linked CAS Monomers","Linked CAS Degradation Products","Name assessor","Date created/updated"]
    # for namesDBcol, nameExcel in zip(namesDBcols,namesExcel):
    #     db_to_excel_multiple_below(maindb="C2C_DATABASE", main_ref="ID", linked_db="ASSESSORS", link_ref="ref",
    #                            column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel)

    # CHEMICAL CLASS
    # colsdatabase = ["Organohalogens","Toxic metal","Colourant", "Geological", "Biological"]
    # labelsexcel = ["Organohalogen","Toxic metal","Colourant", "Geological", "Biological"]
    # db_to_excel_x_right(table_name="C2C_DATABASE", columns_to_get=colsdatabase, lookup_column="ID", lookup_value=CAS,
    #                       labels_excel=labelsexcel,offset=2)

    # CARCINOGENICITY
    namesDBcols = ["Carcinogenicity Classified CLP", "Carcinogenicity Classified MAK", "Carcinogenicity Classified IARC",
                    "Carcinogenicity Classified TLV","Carcinogenicity experimental evidence","Carcinogenicity Comments"]
    namesExcel = ["Carcinogenicity Classified CLP", "Carcinogenicity Classified MAK", "Carcinogenicity Classified IARC",
                    "Carcinogenicity Classified TLV","Carcinogenicity experimental evidence","Carcinogenicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcols,namesExcel):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="CARCINOGENICITY", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

    # ED
    # namesDBcols = ["Endocrine Classified CLP", "Endocrine evidence", "Endocrine Disruption Comments"]
    # namesExcel = ["Endocrine Classified CLP", "Endocrine evidence", "Endocrine Disruption Comments"]
    # for namesDBcol, nameExcel in zip(namesDBcols,namesExcel):
    #     refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="ENDOCRINE", link_ref="ref",
    #                            column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

    # MUTAGENICITY
    # namesDBcols = ["Mutagenicity Classified CLP", "Mutagenicity Classified MAK","Mutagenicity Comments"]
    # namesExcel = ["Mutagenicity Classified CLP", "Mutagenicity Classified MAK","Mutagenicity Comments"]
    # for namesDBcol, nameExcel in zip(namesDBcols,namesExcel):
    #     refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="MUTAGENICITY", link_ref="ref",
    #                            column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

    # REPROTOX

    # DEVELOPMENTAL TOX

    # NEUROTOX

    # ORAL TOX

    # INHALE TOX

    # DERMAL TOX

    # SKIN/EYE IRRIT/COR

    # SENSITISATION

    # SPECIFIC CONCENTRATION LIMITS

    # OTHER

    # FISH TOX

    # INVERTEBRATE TOX

    # ALGAE TOX

    # TERRESTRIAL TOX

    # OTHER SPECIES TOX

    # PERSISTENCE

    # BIOACCUMULATION

    # COMBINED PB RISK FLAG

    # COMBINED AQ RISK FLAG

    # CLIMATIC RELEVANCE

    # OTHER INFORMATION: PHYS CHEM



    #### SAVE THE FILLED IN CPS EXCEL ####
    template_wb.save('/Users/juliakulpa/Desktop/Test_excel_imports/Testing/Testexport.xlsm')

except sqlite3.Error as e:
    print("SQLite error:", e)


