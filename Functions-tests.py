import pandas as pd
# sheet = pd.read_excel("/Users/juliakulpa/Desktop/function-test.xlsx")
# print(sheet)

from openpyxl import load_workbook

testing_excel = '/Users/juliakulpa/Desktop/test/CPS/CPS_CAS 10-00-0.xlsx'
#testing_excel = "/Users/juliakulpa/Desktop/function-test.xlsx"

wb = load_workbook(testing_excel, data_only=True)
sheet = wb.active  # or wb["SheetName"]

def extract_notifiers_resources_wide(sheet):
    """
    Find 'Notifiers' and 'Resources' headers (case-insensitive).
    For each header, read up to 250 rows below (skip blanks).
      - For 'Notifiers':   name = value 6 columns left of the header (col-6) (sensitive, you need to change it inside the function for it to work)
      - For 'Resources':   name = value 7 columns left of the header (col-7)
      - Section value = the cell under the header
    Returns list of dicts, merged wide by name:
      {'name': 'Canc', 'Notifiers': '54', 'Resources': 'ECHA'}
    """

    TARGETS = {"notifiers": "Notifiers", "resources": "Resources"}
    NAME_OFFSETS = {"Notifiers": 6, "Resources": 7}  # excel sensitive, if columns change needs adjusting

    max_row = sheet.max_row
    max_col = sheet.max_column
    rows_by_name = {}

    def coerce(v):
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            try:
                if "." in s:
                    return float(s)
                return int(s)
            except ValueError:
                return s
        return v

    def clean_name(raw):
        if raw is None:
            return ""
        name = str(raw).strip()
        if name.endswith(":"):
            name = name[:-1].strip()
        return name

    def process_column(header_row, header_col, section_label):
        name_offset = NAME_OFFSETS.get(section_label, 6)
        for r in range(header_row + 1, min(header_row + 251, max_row + 1)):
            val = sheet.cell(row=r, column=header_col).value
            if val is None or (isinstance(val, str) and val.strip() == ""):
                continue  # skip blanks, don't stop

            name_col = max(1, header_col - name_offset)
            raw_name = sheet.cell(row=r, column=name_col).value
            name = clean_name(raw_name)
            if not name:
                continue

            if name not in rows_by_name:
                rows_by_name[name] = {"name": name}
            rows_by_name[name][section_label] = coerce(val)

    # Find headers and process
    for row in sheet.iter_rows():
        for cell in row:
            v = cell.value
            if v is None:
                continue
            txt = str(v).strip().lower()
            if txt in TARGETS:
                section = TARGETS[txt]
                header_row = cell.row
                header_col = getattr(cell, "col_idx", getattr(cell, "column", None))
                if isinstance(header_col, int) and 1 <= header_col <= max_col:
                    process_column(header_row, header_col, section)

    return [rows_by_name[k] for k in rows_by_name]



data = extract_notifiers_resources_wide(sheet)

#print(data)

df = pd.DataFrame(data)
print(df)