import sqlite3
import pandas as pd
import re
# Functions to extract info from excel and DB
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
    # print used for checking but ## when needed
    print(cnl_template_dict)
    hazards_dict = get_hazards_for_cas_list(db_path, cas_list)
    result = map_cas_to_templates(hazards_dict, cnl_template_dict)
    # print used for checking but ## when needed
    print(result)
    return result
# ###Not used as its better with Template location: Template name
# def make_dictionary_of_hazards_and_location_in_template_1(excel_path, sheet_name=0):
#     """
#     Reads an Excel file where:
#       - Column B header = 'Template name'
#       - Column C header = 'Template location'
#
#     Returns:
#       dict {Template name: Template location}
#     """
#     df = pd.read_excel(excel_path, sheet_name=sheet_name)
#
#     # Normalize column names (strip spaces)
#     df.columns = df.columns.str.strip()
#
#     if "Template name" not in df.columns or "Template location" not in df.columns:
#         raise ValueError(
#             "Excel must contain columns 'Template name' and 'Template location'"
#         )
#
#     mapping = {}
#
#     for _, row in df.iterrows():
#         template_name = row["Template name"]
#         template_location = row["Template location"]
#
#         if pd.isna(template_name) or pd.isna(template_location):
#             continue  # skip empty rows
#
#         mapping[str(template_name).strip()] = str(template_location).strip()
#
#     return mapping
#
# Extracting location (specific name of the table) for the SQL
def make_dictionary_of_hazards_and_database_name(excel_path, sheet_name=0):
    """
    Reads an Excel file where:
      - Column B header = 'Template name'
      - Column D header = 'Database name'

    Returns:
      dict {Database name: [Template name A, Template name B, Template name C, ...]}
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Normalize column names
    df.columns = df.columns.str.strip()

    if "Template name" not in df.columns or "Database name" not in df.columns:
        raise ValueError(
            "Excel must contain columns 'Template name' and 'Database name'"
        )

    mapping = {}

    for _, row in df.iterrows():
        template_name = row["Template name"]
        database_name = row["Database name"]

        if pd.isna(template_name) or pd.isna(database_name):
            continue  # skip empty rows

        template_name = str(template_name).strip()
        database_name = str(database_name).strip()

        # Append template name under the same database name
        mapping.setdefault(database_name, []).append(template_name)

    return mapping
# Extracting location (specific row in the SQL table) for the SQL
def make_dictionary_of_hazards_and_location_in_template(excel_path, sheet_name=0):
    """
    Reads an Excel file where:
      - Column B header = 'Template name'
      - Column C header = 'Template location (Column in SQL Table name)'

    Returns:
      dict {Template location: [Template name A, Template name B, Template name C, ...]}
    """
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Normalize column names
    df.columns = df.columns.str.strip()

    if "Template name" not in df.columns or "Template location" not in df.columns:
        raise ValueError(
            "Excel must contain columns 'Template name' and 'Template location'"
        )

    mapping = {}

    for _, row in df.iterrows():
        template_name = row["Template name"]
        template_location = row["Template location"]

        if pd.isna(template_name) or pd.isna(template_location):
            continue  # skip empty rows

        template_name = str(template_name).strip()
        template_location = str(template_location).strip()

        # Append template name under the same location
        mapping.setdefault(template_location, []).append(template_name)

    return mapping
# making a big dictionary with CAS: hazard name as in Excel, Location of the corresponding name, Name of the SQL table
def build_cas_template_location_dict(cas_to_templates, location_to_templates, DB_names):
    """
    Inputs:
      cas_to_templates:
        - {CAS: [template_name, ...]}  OR  {CAS: template_name}

      location_to_templates:
        - {template_location: [template_name, template_name2, ...]}

      DB_names:
        - {Database name: [Template name A, Template name B, ...]}

    Returns:
      {CAS: [
          {
            "template_name": str,
            "database_name": str or None,
            "template_location": str or None,
          }, ...
      ]}
    """

    # 1) Build template_name -> template_location lookup
    template_to_location = {}
    for location, template_names in location_to_templates.items():
        for name in template_names:
            template_to_location[str(name).strip()] = str(location).strip()

    # 2) Build template_name -> database_name lookup
    template_to_database = {}
    for db_name, template_names in DB_names.items():
        for name in template_names:
            template_to_database[str(name).strip()] = str(db_name).strip()

    # 3) Normalize cas_to_templates to always be {CAS: [templates]}
    normalized = {}
    for cas, tmpl in cas_to_templates.items():
        cas_str = str(cas).strip()
        if tmpl is None:
            normalized[cas_str] = []
        elif isinstance(tmpl, list):
            normalized[cas_str] = [str(x).strip() for x in tmpl if str(x).strip()]
        else:
            normalized[cas_str] = [str(tmpl).strip()]

    # 4) Build output structure
    result = {}
    for cas, template_list in normalized.items():
        entries = []
        seen = set()

        for tname in template_list:
            if tname in seen:
                continue
            seen.add(tname)

            entries.append({
                "template_name": tname,
                "database_name": template_to_database.get(tname),   # None if missing
                "template_location": template_to_location.get(tname)  # None if missing
            })

        # Optional: stable ordering
        entries.sort(
            key=lambda x: (
                x["database_name"] or "",
                x["template_location"] or "",
                x["template_name"]
            )
        )
        result[cas] = entries

    return result
# function to insert the values in to specific columns of the DB
def fill_templates_into_db(db_path,cas_entries_dict,*, preferred_key_cols=("cas", "CAS", "ref", "ID", "code"), verbose=True):
    """
    For each CAS, loops through its list of entries and (when database_name is not None):
      - opens the SQL table named by entry["database_name"]
      - finds the column whose name matches entry["template_location"]
        (match is tolerant to trailing ':' and extra spaces)
      - updates the row for that CAS (row key) by writing entry["template_name"] into that column

    Expected cas_entries_dict structure:
      {
        "50-00-0": [
          {
            "template_name": "Carc. 1B: H350: May cause cancer",
            "database_name": "CANCEROGENICY",
            "template_location": "Carcinogenicity Classified CLP:"
          }
        ]
      }
    """

    def norm(s: str) -> str:
        """Normalize names for tolerant matching.
        This is important as sometimes names have : at the end and sometimes not"""
        s = (s or "").strip()
        s = re.sub(r"\s+", " ", s)
        s = s.rstrip(":").strip()
        return s.casefold()

    def q_ident(name: str) -> str:
        """Quote SQL identifiers safely for SQLite."""
        return f'[{name.replace("]", "]]")}]'

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        if verbose:
            print("Connected to SQLite database:", db_path)

        for cas, entries in cas_entries_dict.items():
            cas_str = str(cas).strip()
            if not entries:
                continue

            for entry in entries:
                template_value = entry.get("template_name")
                table_name = entry.get("database_name")
                requested_col = entry.get("template_location")

                if not table_name:
                    continue

                if template_value is None or requested_col is None:
                    if verbose:
                        print(f"{cas_str}: skipped (missing template_name or template_location)")
                    continue

                # 1) Confirm table exists
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                    (table_name,)
                )
                if cur.fetchone() is None:
                    if verbose:
                        print(f"{cas_str}: table not found -> {table_name}")
                    continue

                # 2) Read columns
                cur.execute(f"PRAGMA table_info({q_ident(table_name)})")
                cols_info = cur.fetchall()

                if not cols_info:
                    if verbose:
                        print(f"{cas_str}: table has no columns -> {table_name}")
                    continue

                actual_cols = [c[1] for c in cols_info]
                col_by_norm = {norm(c): c for c in actual_cols}

                target_col = col_by_norm.get(norm(requested_col))

                # Fallback: ignore all colons
                if target_col is None:
                    def norm_no_colon(x):
                        return norm(x).replace(":", "")
                    col_by_norm2 = {norm_no_colon(c): c for c in actual_cols}
                    target_col = col_by_norm2.get(norm_no_colon(requested_col))

                if target_col is None:
                    if verbose:
                        print(f"{cas_str}: column not found in {table_name} for '{requested_col}'")
                    continue

                # 3) Detect CAS key column
                available = {c.casefold(): c for c in actual_cols}
                key_col = None
                for cand in preferred_key_cols:
                    if cand.casefold() in available:
                        key_col = available[cand.casefold()]
                        break

                if key_col is None:
                    if verbose:
                        print(f"{cas_str}: no key column found in {table_name}")
                    continue

                # 4) UPDATE or INSERT
                update_sql = (
                    f"UPDATE {q_ident(table_name)} "
                    f"SET {q_ident(target_col)} = ? "
                    f"WHERE {q_ident(key_col)} = ?"
                )
                cur.execute(update_sql, (str(template_value), cas_str))

                if cur.rowcount == 0:
                    insert_sql = (
                        f"INSERT INTO {q_ident(table_name)} "
                        f"({q_ident(key_col)}, {q_ident(target_col)}) "
                        f"VALUES (?, ?)"
                    )
                    cur.execute(insert_sql, (cas_str, str(template_value)))
                    if verbose:
                        print(f"{cas_str}: INSERTED into: {table_name} -> {target_col} -> {template_value}")
                else:
                    if verbose:
                        print(f"{cas_str}: UPDATED info: {table_name} -> {target_col} -> {template_value}")

        conn.commit()

        if verbose:
            print("All template values written successfully.")

        return True

    finally:
        if conn is not None:
            conn.close()
            if verbose:
                print("Connection closed.")

excel_path = "/Users/juliakulpa/Library/CloudStorage/Dropbox-Arche/Julia Kulpa/automation/CnL - info for dictionary/CnL - information.xlsx"
db_path = '/Users/juliakulpa/Library/CloudStorage/Dropbox-Arche/Julia Kulpa/automation/CnL - info for dictionary/C2Cdatabase.db'
# for tests I just put random CAS
cas_list = ["50-00-0", "64-17-5", "71-43-2"]
function = create_dictionary_of_cas_and_hazards(excel_path, db_path, cas_list)
print(function)
hazards_location = make_dictionary_of_hazards_and_location_in_template(excel_path)
print(hazards_location)
DB_names = make_dictionary_of_hazards_and_database_name(excel_path)
print(DB_names)
output = build_cas_template_location_dict(function, hazards_location, DB_names)
print(output)
fill_templates_into_db(db_path, output)