import sqlite3
import pandas as pd

def create_dictionary_of_cas_and_hazards(excel_path, db_path, cas_list):
    """Creates a dictionary of CAS: Hazards as per template name with hazards from CnL:
    1. Excel file with CnL names and dictionary: CnL Name: Template name
    2. cas list that corresponds to CAS in DB from the columnn Hazard in the ECHACHEM_CL database"""
    def read_cnl_template_to_dictionary(excel_path, sheet_name=0):
        """
        Reads an Excel file where:
          - Column A header = 'CnL Name'
          - Column B header = 'Template name'

        Returns:
          dict {CnL Name: Template name}
        """
        df = pd.read_excel(excel_path, sheet_name=sheet_name)

        # Normalize column names (strip spaces)
        df.columns = df.columns.str.strip()

        if "CnL Name" not in df.columns or "Template name" not in df.columns:
            raise ValueError("Excel must contain columns 'CnL Name' and 'Template name'")

        mapping = {}

        for _, row in df.iterrows():
            cnl = row["CnL Name"]
            template = row["Template name"]

            if pd.isna(cnl) or pd.isna(template):
                continue  # skip empty rows

            mapping[str(cnl).strip()] = str(template).strip()

        return mapping
    def get_hazards_for_cas_list(db_path, cas_list):
        """
        For each CAS in cas_list, fetch hazards from ECHACHEM_CL and return:
          { "CAS": ["Hxxx", "Hyyy", ...], ... }

        Notes:
        - Uses the 'cas' column.
        - Handles empty/missing hazards.
        - De-duplicates hazards while preserving order.
        """
        cas_hazards = {}
        connection = None

        try:
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()
            print("Connected to SQLite database:", db_path)

            # Optional: normalize CAS strings
            cas_list_clean = [str(c).strip() for c in cas_list if c is not None and str(c).strip() != ""]

            for target_cas in cas_list_clean:
                cursor.execute("SELECT hazards FROM ECHACHEM_CL WHERE cas = ?", (target_cas,))
                row = cursor.fetchone()

                if not row or row[0] is None or str(row[0]).strip() == "":
                    cas_hazards[target_cas] = []
                    print(f"{target_cas}: no hazards found in ECHACHEM_CL")
                    continue

                # Hazards stored as comma-separated string -> list
                hazards_raw = str(row[0])
                hazards_list = [h.strip() for h in hazards_raw.split(",") if h.strip()]

                # De-duplicate while preserving order
                seen = set()
                hazards_list_unique = []
                for h in hazards_list:
                    if h not in seen:
                        hazards_list_unique.append(h)
                        seen.add(h)

                cas_hazards[target_cas] = hazards_list_unique
                print(f"{target_cas}: {hazards_list_unique}")

            print("Hazards dictionary:", cas_hazards)
            return cas_hazards

        finally:
            if connection:
                connection.close()
                print("Connection closed.")
    def map_cas_to_templates(cas_to_cnls, cnl_to_template):
        """
        Combines:
          {CAS: [CnL Name, ...]}
          {CnL Name: Template name}

        Returns:
          {CAS: [Template name, ...]}
        """
        cas_to_templates = {}

        for cas, cnl_list in cas_to_cnls.items():
            templates = []

            for cnl_name in cnl_list:
                template = cnl_to_template.get(cnl_name)
                if template:
                    templates.append(template)

            # remove duplicates, keep order
            seen = set()
            unique_templates = []
            for t in templates:
                if t not in seen:
                    unique_templates.append(t)
                    seen.add(t)

            cas_to_templates[cas] = unique_templates

        return cas_to_templates

    cnl_template_dict = read_cnl_template_to_dictionary(excel_path)
    print(cnl_template_dict)
    hazards_dict = get_hazards_for_cas_list(db_path, cas_list)
    result = map_cas_to_templates(hazards_dict, cnl_template_dict)
    print(result)
    return result


excel_path = "/Users/juliakulpa/Library/CloudStorage/Dropbox-Arche/Julia Kulpa/automation/CnL - info for dictionary/CnL - information.xlsx"
db_path = '/Users/juliakulpa/Library/CloudStorage/Dropbox-Arche/Julia Kulpa/automation/CnL - info for dictionary/C2Cdatabase.db'
cas_list = ["50-00-0", "64-17-5", "71-43-2"]
function = create_dictionary_of_cas_and_hazards(excel_path, db_path, cas_list)
print(function)