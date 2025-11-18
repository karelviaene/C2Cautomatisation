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
template_path = "/Users/juliakulpa/Desktop/Test_excel_imports/Template/CPS_CAS TEMPLATE V2.xlsm"
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
                    #print(f"First test on{start_row, col}")

                    # Place each value in the first empty cell below the starting row
                    for result in results:
                        # Start searching from start_row downward
                        target_row = start_row

                        # Keep moving down until we find an empty cell in the target column
                        while ws_template.cell(row=target_row, column=col).value not in (None, ''):
                            target_row += 1

                        #print(f"target row{target_row}")

                        # Write the value in the first empty cell found
                        ws_template.cell(row=target_row, column=col).value = result[0]

                        print(
                            f"Inserted '{result[0]}' into cell {ws_template.cell(row=target_row, column=col).coordinate}")

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
    #Add general info
    namesDBcol_gen = ["Chemical name", "Common name", "CAS number", "EC number", "Linked CAS Read across",
                   "Linked CAS Monomers", "Linked CAS Degradation Products"]
    namesExcel_gen = ["Chemical name", "Common name", "CAS number", "EC number", "Linked CAS Read across",
                   "Linked CAS Monomers", "Linked CAS Degradation Products"]
    for namesDBcol_gen, namesExcel_gen in zip(namesDBcol_gen,namesExcel_gen):
        db_to_excel_multiple_below(maindb="C2C_DATABASE", main_ref="ID", linked_db="GENERALINFO", link_ref="ref",
                               column_to_get=namesDBcol_gen, lookup_column="ID",lookup_value =CAS, label_excel=namesExcel_gen)
    # ADD ASSESSORS
    db_to_excel_multiple_below(maindb="C2C_DATABASE", main_ref="ID", linked_db="ASSESSORS", link_ref="ref",
                               column_to_get="Name assessor", lookup_column="ID", lookup_value=CAS,
                               label_excel="Name assessor")
    db_to_excel_multiple_below(maindb="C2C_DATABASE", main_ref="ID", linked_db="ASSESSORS", link_ref="ref",
                               column_to_get="Date assessed", lookup_column="ID", lookup_value=CAS,
                               label_excel="Date created/updated")
    ## Add various info CHEMICAL CLASS
    namesDBcol_CC = ["Organohalogen","Toxic metal", "Colourant", "Geological", "Biological", "Polymer", "SVHC", "VOC"]
    namesExcel_CC = ["Organohalogen","Toxic metal", "Colourant", "Geological", "Biological", "Polymer", "SVHC", "VOC"]
    for names_DB, name_EX in zip(namesDBcol_CC, namesExcel_CC):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="CHEMICALCLASS", link_ref="ref",
                                    column_to_get=names_DB, lookup_column="ID", lookup_value=CAS,
                                    label_excel=name_EX, offset=2)

    # Adding other info
    namesDBcol_OTHER = ["Molecular weight","Boiling point", "Log kow (octanol-water partition coefficient)", "Vapor pressure", "Water solubility", "pH", "SMILES"]
    namesExcel_OTHER = ["Molecular weight","Boiling point", "Log kow (octanol-water partition coefficient)", "Vapor pressure", "Water solubility", "pH", "SMILES"]
    for names_DB_OTHER, name_EX_OTHER in zip(namesDBcol_OTHER, namesExcel_OTHER):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="OTHERINFO", link_ref="ref",
                                    column_to_get=names_DB_OTHER, lookup_column="ID", lookup_value=CAS,
                                    label_excel=name_EX_OTHER, offset=2)
    #  OTHER CRITERIA
    refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="OCRIT", link_ref="ref",
                                column_to_get="Other comments", lookup_column="ID", lookup_value=CAS,
                                label_excel="Other comments", offset=1)
    # CARCINOGENICITY
    namesDBcols = ["Carcinogenicity Classified CLP", "Carcinogenicity Classified MAK", "Carcinogenicity Classified IARC",
                    "Carcinogenicity Classified TLV","Carcinogenicity experimental evidence","Carcinogenicity Comments"]
    namesExcel = ["Carcinogenicity Classified CLP", "Carcinogenicity Classified MAK", "Carcinogenicity Classified IARC",
                    "Carcinogenicity Classified TLV","Carcinogenicity experimental evidence","Carcinogenicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcols,namesExcel):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="CARCINOGENICITY", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

    # ED
    namesDBcols = ["Endocrine Classified CLP", "Endocrine evidence", "Endocrine Disruption Comments"]
    namesExcel = ["Endocrine Classified CLP", "Endocrine evidence", "Endocrine Disruption Comments"]
    for namesDBcol, nameExcel in zip(namesDBcols,namesExcel):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="ENDOCRINE", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

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
    template_wb.save('/Users/juliakulpa/Desktop/Test_excel_imports/Testing/Test-export.xlsm')

except sqlite3.Error as e:
    print("SQLite error:", e)


