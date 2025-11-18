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
    namesDBcol_MUT = ["Mutagenicity Classified CLP", "Mutagenicity Classified MAK","Mutagenicity Comments"]
    namesExcel_MUT = ["Mutagenicity Classified CLP", "Mutagenicity Classified MAK","Mutagenicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_MUT,namesExcel_MUT):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="MUTAGENICITY", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

    # REPROTOX
    namesDBcol_REP = ["Reprotox Classified CLP", "Reprotox Classified MAK", "Reprotox Oral NOAEL =",
                                           "Reprotox Inhalation NOAEL =", "Reproductive Toxicity Comments"]
    namesExcel_REP = ["Reprotox Classified CLP", "Reprotox Classified MAK", "Reprotox Oral NOAEL =",
                                           "Reprotox Inhalation NOAEL =", "Reproductive Toxicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_REP,namesExcel_REP):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="REPROTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # DEVELOPMENTAL TOX
    namesDBcol_DEV = ["Developmental Classified CLP", "Developmental Classified MAK", "Developmental Oral NOAEL =",
                                           "Developmental Inhalation NOAEL =", "Developmental Toxicity Comments"]
    namesExcel_DEV = ["Developmental Classified CLP", "Developmental Classified MAK", "Developmental Oral NOAEL =",
                                           "Developmental Inhalation NOAEL =", "Developmental Toxicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_DEV,namesExcel_DEV):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="DEVELOPTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # NEUROTOX
    namesDBcol_NETOX = ["Neurotox Classified CLP", "Neurotox on a list", "Neurotox scientific evidence?",
                            "Neurotox chronic LOAEL", "Neurtox STOT LOAEL", "Neurotox Comments"]
    namesExcel_NETOX = ["Neurotox Classified CLP", "Neurotox on a list", "Neurotox scientific evidence?",
                            "Neurotox chronic LOAEL", "Neurtox STOT LOAEL", "Neurotox Comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_NETOX,namesExcel_NETOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="NEUROTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # ORAL TOX
    namesDBcol_ORTOX = ["Oral toxicity Acute Tox classified","Oral toxicity Asp Tox classified", "Oral toxicity STOT classified", "Oral Acute: LD50 =",
                            "Oral Chronic: LOAEL =", "Oral Toxicity Comments"]
    namesExcel_ORTOX = ["Oral toxicity Acute Tox classified:","Oral toxicity Asp Tox classified", "Oral toxicity STOT classified", "Oral Acute: LD50 =",
                            "Oral Chronic: LOAEL =", "Oral Toxicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_ORTOX,namesExcel_ORTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="ORALTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # INHALE TOX

    namesDBcol_INHTOX = ["Inhalative toxicity Acute Tox classification", "Inhalative toxicity STOT classified",
                            "Inhalative toxicity Acute: LC50 (gas) =", "Inhalative toxicity Acute: LC50 (vapor) =", "Inhalative toxicity Acute: LC50 (dust/mist/aerosol) =", "Inhalative toxicity Chronic: LOAEL (gas) =",
                            "Inhalative toxicity Chronic: LOAEL (vapor) =", "Inhalative toxicity Chronic: LOAEL (dust/mist/aerosol) =", "Inhalative Toxicity Comments"]
    namesExcel_INHTOX = ["Inhalative toxicity Acute Tox classification", "Inhalative toxicity STOT classified",
                            "Inhalative toxicity Acute: LC50 (gas) =", "Inhalative toxicity Acute: LC50 (vapor) =", "Inhalative toxicity Acute: LC50 (dust/mist/aerosol) =", "Inhalative toxicity Chronic: LOAEL (gas) =",
                            "Inhalative toxicity Chronic: LOAEL (vapor) =", "Inhalative toxicity Chronic: LOAEL (dust/mist/aerosol) =", "Inhalative Toxicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_INHTOX,namesExcel_INHTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="INHALTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # DERMAL TOX
    namesDBcol_DERMTOX = ["Dermal toxicity Acute Tox classified", "Dermal toxicity STOT classified",
                                            "Dermal Acute: LD50 =", "Dermal Chronic: LOAEL =", "Dermal Toxicity Comments"]
    namesExcel_DERMTOX = ["Dermal toxicity Acute Tox classified", "Dermal toxicity STOT classified",
                                            "Dermal Acute: LD50 =", "Dermal Chronic: LOAEL =", "Dermal Toxicity Comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_DERMTOX,namesExcel_DERMTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="DERMALTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # SKIN/EYE IRRIT/COR
    namesDBcol_IRR = ["Skin irritation classification", "Skin testing: conclusion", "Eye irritation classification",
                            "Eye testing conclusion", "Respiratory irritation classification", "Respiratory testing conclusion", "Corrosion/irritation comments"]
    namesExcel_IRR = ["Skin irritation classification", "Skin testing: conclusion", "Eye irritation classification",
                            "Eye testing conclusion", "Respiratory irritation classification", "Respiratory testing conclusion", "Corrosion/irritation comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_IRR,namesExcel_IRR):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="IRRITCOR", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # SENSITISATION
    namesDBcol_SENS = ["Skin sensitization CLP classification", "Skin sensitization MAK classification",
                            "Skin sensitization testing conclusion", "Respiratory sensitization CLP classification",
                            "Respiratory sensitization MAK classification", "Respiratory sensitization testing conclusion", "Sensitization comments"]
    namesExcel_SENS = ["Skin sensitization CLP classification", "Skin sensitization MAK classification",
                            "Skin sensitization testing conclusion", "Respiratory sensitization CLP classification",
                            "Respiratory sensitization MAK classification", "Respiratory sensitization testing conclusion", "Sensitization comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_SENS,namesExcel_SENS):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="SENSITISATION", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # SPECIFIC CONCENTRATION LIMITS
    # CODE TO BE ADDED: PROBABLY A NEW FUNCTION NEEDED:)

    # AQUATIC TOXICITY

    namesDBcol_AQTOX = ["Aquatic toxicity Acute Tox classified", "Aquatic toxicity Chronic Tox classified","Water solubility", "M factor"]
    namesExcel_AQTOX = ["Aquatic toxicity Acute Tox classified", "Aquatic toxicity Chronic Tox classified","Water solubility", "M factor"]
    for namesDBcol, nameExcel in zip(namesDBcol_AQTOX,namesExcel_AQTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="AQUATOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)
    # VERTEBRATE FISH
    namesDBcol_FISHTOX = ["Fish toxicity Acute: LC50 (96h) =", "Fish toxicity Chronic: NOEC =", "Fish toxicity Acute QSAR: LC50 =", "Fish toxicity Chronic QSAR: NOEC =", "Fish toxicity comments"]
    namesExcel_FISHTOX = ["Fish toxicity Acute: LC50 (96h) =", "Fish toxicity Chronic: NOEC =", "Fish toxicity Acute QSAR: LC50 =", "Fish toxicity Chronic QSAR: NOEC =", "Fish toxicity comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_FISHTOX,namesExcel_FISHTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="FISHTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

    # INVERTEBRATE TOX
    namesDBcol_INVTOX = ["Invertebrate toxicity Acute: L(E)C50 (48h) =", "Invertebrae toxicity Chronic: NOEC =", "Invertebrae toxicity Acute QSAR: LC50 =", "Invertebrae toxicity Chronic QSAR: NOEC =", "Invertebrate toxicity comments"]
    namesExcel_INVTOX = ["Invertebrate toxicity Acute: L(E)C50 (48h) =", "Invertebrae toxicity Chronic: NOEC =", "Invertebrae toxicity Acute QSAR: LC50 =", "Invertebrae toxicity Chronic QSAR: NOEC =", "Invertebrate toxicity comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_INVTOX,namesExcel_INVTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="INVTOX", link_ref="ref",
                               column_to_get=namesDBcol, lookup_column="ID",lookup_value =CAS, label_excel=nameExcel,offset=1)

    # ALGAE TOX
    namesDBcol_ALGTOX = ["Algae toxicity Acute: L(E)C50 (72/96h) =", "Algae toxicity Chronic: NOEC =", "Algae toxicity Acute QSAR: LC50 =", "Algae toxicity Chronic QSAR: NOEC =", "Algae toxicity comments:"]
    namesExcel_ALGTOX = ["Algae toxicity Acute: L(E)C50 (72/96h) =", "Algae toxicity Chronic: NOEC =", "Algae toxicity Acute QSAR: LC50 =", "Algae toxicity Chronic QSAR: NOEC =", "Algae toxicity comments:"]
    for namesDBcol, nameExcel in zip(namesDBcol_ALGTOX, namesExcel_ALGTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="ALGAETOX", link_ref="ref",
                                    column_to_get=namesDBcol, lookup_column="ID", lookup_value=CAS,
                                    label_excel=nameExcel, offset=1)

    # TERRESTRIAL TOX
    namesDBcol_TERTOX =  ["Terrestial toxicity CLP classification", "Terrestial toxicity Acute (Chicken): LD50=", "Terrestial toxicity Acute (Duck): LD50=",
                                            "Terrestial toxicity Acute (Worm): EC50=", "Terrestial toxicity Chronic (Chicken): NOEC=", "Terrestial toxicity Chronic (Duck): NOEC=",
                                            "Terrestial toxicity Chronic (Worm): NOEC=", "Terrestial toxicity comments"]
    namesExcel_TERTOX =  ["Terrestial toxicity CLP classification", "Terrestial toxicity Acute (Chicken): LD50=", "Terrestial toxicity Acute (Duck): LD50=",
                                            "Terrestial toxicity Acute (Worm): EC50=", "Terrestial toxicity Chronic (Chicken): NOEC=", "Terrestial toxicity Chronic (Duck): NOEC=",
                                            "Terrestial toxicity Chronic (Worm): NOEC=", "Terrestial toxicity comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_TERTOX, namesExcel_TERTOX):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="TERTOX", link_ref="ref",
                                    column_to_get=namesDBcol, lookup_column="ID", lookup_value=CAS,
                                    label_excel=nameExcel, offset=1)

    # OTHER SPECIES TOX
    refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="SPECTOX", link_ref="ref",
                                column_to_get="Any other CLP species classification", lookup_column="ID", lookup_value=CAS,
                                label_excel="Any other CLP species classification", offset=1)
    # PERSISTENCE
    namesDBcol_PERS =  ["OECD 301: % DOC biodegradation after 28 days", "OECD 301: % ThOD biodegradation after 28 days",
                            "OECD 302 or OECD 304A: % inherent biodegradation: ", "OECD 311","QSAR prediction", "Half-life (T1/2) Air", "Half-life (T1/2) Water", "Half-life (T1/2) Soil or sediment", "Persistence comments"]
    namesExcel_PERS =  ["OECD 301: % DOC biodegradation after 28 days", "OECD 301: % ThOD biodegradation after 28 days",
                            "OECD 302 or OECD 304A: % inherent biodegradation: ", "OECD 311","QSAR prediction", "Half-life (T1/2) Air", "Half-life (T1/2) Water", "Half-life (T1/2) Soil or sediment", "Persistence comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_PERS, namesExcel_PERS):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="PERSISTENCE", link_ref="ref",
                                    column_to_get=namesDBcol, lookup_column="ID", lookup_value=CAS,
                                    label_excel=nameExcel, offset=1)
    # BIOACCUMULATION
    namesDBcol_BIOAC =  ["BCF/BAF (experimental)", "BCF/BAF (QSAR)", "Bioaccumulation comments"]
    namesExcel_BIOAC =  ["BCF/BAF (experimental)", "BCF/BAF (QSAR)", "Bioaccumulation comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_BIOAC, namesExcel_BIOAC):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="BIOACCUMULATION", link_ref="ref",
                                    column_to_get=namesDBcol, lookup_column="ID", lookup_value=CAS,
                                    label_excel=nameExcel, offset=1)
    # CLIMATIC RELEVANCE
    namesDBcol_CLIMREL =  ["Climatic listed?", "100 year GWP", "ODP", "Climatic relevance comments"]
    namesExcel_CLIMREL =  ["Climatic listed?", "100 year GWP", "ODP", "Climatic relevance comments"]
    for namesDBcol, nameExcel in zip(namesDBcol_CLIMREL, namesExcel_CLIMREL):
        refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="CLIMATICRELEVANCE", link_ref="ref",
                                    column_to_get=namesDBcol, lookup_column="ID", lookup_value=CAS,
                                    label_excel=nameExcel, offset=1)
    #  ADDITIONAL SOURCES
    refdb_to_excel_source_right(maindb="C2C_DATABASE", main_ref="ID", linked_db="ADDSOURCE", link_ref="ref",
                                column_to_get="Additional sources", lookup_column="ID", lookup_value=CAS,
                                label_excel="Additional sources", offset=1)
    #### SAVE THE FILLED IN CPS EXCEL ####
    template_wb.save('/Users/juliakulpa/Desktop/Test_excel_imports/Testing/Test-export.xlsm')

except sqlite3.Error as e:
    print("SQLite error:", e)


