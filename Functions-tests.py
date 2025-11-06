import os
import zipfile

#import Pillow

# Change depending on where is the excel and where you want to save the images
#output_dir = r"/Users/juliakulpa/Desktop/Imag_test/Photos"  # <-- put your directory here
#excel_path = "/Users/juliakulpa/Desktop/Imag_test/Image.xlsx"

C2Cpath = "/Users/juliakulpa/Desktop/test"
C2Cfiles_path = os.path.join(C2Cpath,"CPS")
images_output = "/Users/juliakulpa/Desktop/test/Chem_image"

import os
import zipfile

import os
import zipfile

def extract_all_images_from_excel(excel_path, output_dir):
    """
    Extract all embedded images from an Excel .xlsx or .xlsm file and save them to output_dir.
    Images are renamed to "<excel_filename>-01.<ext>", "<excel_filename>-02.<ext>", etc.
    Returns a list of saved file paths.
    """
    # Basic validations
    # Skip non-files
    if not os.path.isfile(excel_path):
        print(f"Skipped (not a file): {excel_path}")
        return []

    # Get extension safely
    _, ext_in = os.path.splitext(excel_path)
    ext_in = ext_in.lower()

    # Skip unsupported or extensionless files
    if ext_in not in (".xlsx", ".xlsm"):
        print(f"Skipped (unsupported or missing extension): {excel_path}")
        return []
    if not zipfile.is_zipfile(excel_path):
        raise ValueError(f"The file doesn't look like a valid Excel Open XML package: {excel_path}")

    os.makedirs(output_dir, exist_ok=True)

    excel_name = os.path.splitext(os.path.basename(excel_path))[0]
    saved_paths = []

    with zipfile.ZipFile(excel_path, 'r') as z:
        # Images live under xl/media in OOXML workbooks (both .xlsx and .xlsm)
        image_files = [f for f in z.namelist() if f.startswith('xl/media/')]

        if not image_files:
            print(f"No images found in {excel_path}.")
            return saved_paths

        # Sort for deterministic ordering
        image_files.sort()

        for idx, img_name in enumerate(image_files, start=1):
            img_data = z.read(img_name)
            img_ext = os.path.splitext(img_name)[1]  # keep original extension from the package

            #saves each time a new image
            if idx == 1:
                filename = f"{excel_name}{img_ext}"
            else:
                filename = f"{excel_name}-{idx - 1}{img_ext}"
            output_path = os.path.join(output_dir, filename)

            # If the same name exists, bump a counter
            # if os.path.exists(output_path):
            #     bump = 1
            #     base, ext = os.path.splitext(filename)
            #     while os.path.exists(output_path):
            #         output_path = os.path.join(output_dir, f"{base}({bump}){ext}")
            #         bump += 1
            # Skip if file already exists
            # if os.path.exists(output_path):
            #     print(f"Skipped (already exists): {output_path}")
            #     continue

            with open(output_path, 'wb') as f:
                f.write(img_data)

            saved_paths.append(output_path)
            #print(f"Saved: {output_path}") # check-point

    #print(f"\nExtracted {len(image_files)} image(s) from '{os.path.basename(excel_path)}' to: {output_dir}") # check-point
    return saved_paths



for filename in os.listdir(C2Cfiles_path):
    full_path = os.path.join(C2Cfiles_path, filename)
    #print([full_path]) # check-point
    extract_all_images_from_excel(full_path, images_output)

#extract_all_images_from_excel("/Users/juliakulpa/Desktop/test/CPS/CPS_CAS 10-00-1.xlsx", images_output)

# def extract_all_images_from_excel(excel_path, output_dir):
#     """
#     Extracts all embedded images from an Excel (.xlsx) file
#     and saves them into a chosen folder.
#     """
#     # Create the folder if it doesn’t exist
#     os.makedirs(output_dir, exist_ok=True)
#
#     # Extract images from excel
#     with zipfile.ZipFile(excel_path, 'r') as z:
#         image_files = [f for f in z.namelist() if f.startswith('xl/media/')]
#
#         if not image_files:
#             print(f"No images found in {excel_path}.")
#             return
#
#         for img_name in image_files:
#             img_data = z.read(img_name)
#             filename = os.path.basename(img_name)
#             output_path = os.path.join(output_dir, filename)
#             with open(output_path, 'wb') as f:
#                 f.write(img_data)
#             print(f"Saved: {output_path}")
#
#     print(f"\nExtracted {len(image_files)} image(s) to: {output_dir}")

#extract_all_images_from_excel(excel_path, output_dir)
#
# EXTRACTING IMAGE TO EXCEL FILE
# from openpyxl import load_workbook
# from openpyxl.drawing.image import Image as XLImage
# from openpyxl.utils import get_column_letter
# import os
#
# def insert_image_below_label(excel_path, image_path, sheet_name=None, label_text="Image"):
#     """
#     Finds the cell with text `label_text` (case-insensitive) in an Excel file,
#     and inserts the given image directly below it (same column, next row).
#     """
#
#     if not os.path.exists(excel_path):
#         raise FileNotFoundError(f"Excel file not found: {excel_path}")
#     if not os.path.exists(image_path):
#         raise FileNotFoundError(f"Image file not found: {image_path}")
#
#     # Load workbook (must NOT be read_only to add images)
#     wb = load_workbook(excel_path)
#     ws = wb[sheet_name] if sheet_name else wb.active
#
#     # Find the cell labeled 'Image' (case-insensitive)
#     image_label_row = None
#     image_label_col = None
#     wanted = label_text.strip().lower()
#
#     for row in ws.iter_rows():
#         for cell in row:
#             val = cell.value
#             if isinstance(val, str) and val.strip().lower() == wanted:
#                 image_label_row = cell.row
#                 image_label_col = cell.column  # 1-based index
#                 break
#         if image_label_row is not None:
#             break
#
#     if image_label_row is None:
#         raise ValueError(f"No cell labeled '{label_text}' found.")
#
#     # Cell below the label
#     target_row = image_label_row + 1
#     target_col_letter = get_column_letter(image_label_col)
#     target_cell = f"{target_col_letter}{target_row}"
#
#     # Create and add the image
#     img = XLImage(image_path)
#     # Optional: resize the image
#     img.width = 100
#     img.height = 100
#
#     ws.add_image(img, target_cell)
#
#     # Optional: adjust row height to avoid clipping
#     ws.row_dimensions[target_row].height = max(ws.row_dimensions[target_row].height or 15, 80)
#
#     wb.save(excel_path)
#     print(f"Image inserted below '{label_text}' at {target_cell} in {excel_path}")
#
#
# # Example
#
# insert_image_below_label(excel_path=r"/Users/juliakulpa/Desktop/Imag_test/Image copy.xlsx", image_path=r"/Users/juliakulpa/Desktop/Imag_test/image2.png", sheet_name=None, label_text="Image")
#



# string = ["a", "c", "d"]
#
# name = []
# for i in string:
#     name.append(i + "-a")
# print(name)


# import pandas as pd
# # sheet = pd.read_excel("/Users/juliakulpa/Desktop/function-test.xlsx")
# # print(sheet)
#
# from openpyxl import load_workbook
#
# testing_excel = '/Users/juliakulpa/Desktop/test/CPS/CPS_CAS 10-00-0.xlsx'
# #testing_excel = "/Users/juliakulpa/Desktop/function-test.xlsx"
#
# wb = load_workbook(testing_excel, data_only=True)
# sheet = wb.active  # or wb["SheetName"]
#
# def extract_notifiers_resources_wide(sheet):
#     """
#     Find 'Notifiers' and 'Resources' headers (case-insensitive).
#     For each header, read up to 250 rows below (skip blanks).
#       - For 'Notifiers':   name = value 6 columns left of the header (col-6) (sensitive, you need to change it inside the function for it to work)
#       - For 'Resources':   name = value 7 columns left of the header (col-7)
#       - Section value = the cell under the header
#     Returns list of dicts, merged wide by name:
#       {'name': 'Canc', 'Notifiers': '54', 'Resources': 'ECHA'}
#     """
#
#     TARGETS = {"notifiers": "Notifiers", "resources": "Resources"}
#     NAME_OFFSETS = {"Notifiers": 6, "Resources": 7}  # excel sensitive, if columns change needs adjusting
#
#     max_row = sheet.max_row
#     max_col = sheet.max_column
#     rows_by_name = {}
#
#     def coerce(v):
#         if v is None:
#             return None
#         if isinstance(v, str):
#             s = v.strip()
#             try:
#                 if "." in s:
#                     return float(s)
#                 return int(s)
#             except ValueError:
#                 return s
#         return v
#
#     def clean_name(raw):
#         if raw is None:
#             return ""
#         name = str(raw).strip()
#         if name.endswith(":"):
#             name = name[:-1].strip()
#         return name
#
#     def process_column(header_row, header_col, section_label):
#         name_offset = NAME_OFFSETS.get(section_label, 6)
#         for r in range(header_row + 1, min(header_row + 251, max_row + 1)):
#             val = sheet.cell(row=r, column=header_col).value
#             if val is None or (isinstance(val, str) and val.strip() == ""):
#                 continue  # skip blanks, don't stop
#
#             name_col = max(1, header_col - name_offset)
#             raw_name = sheet.cell(row=r, column=name_col).value
#             name = clean_name(raw_name)
#             if not name:
#                 continue
#
#             if name not in rows_by_name:
#                 rows_by_name[name] = {"name": name}
#             rows_by_name[name][section_label] = coerce(val)
#
#     # Find headers and process
#     for row in sheet.iter_rows():
#         for cell in row:
#             v = cell.value
#             if v is None:
#                 continue
#             txt = str(v).strip().lower()
#             if txt in TARGETS:
#                 section = TARGETS[txt]
#                 header_row = cell.row
#                 header_col = getattr(cell, "col_idx", getattr(cell, "column", None))
#                 if isinstance(header_col, int) and 1 <= header_col <= max_col:
#                     process_column(header_row, header_col, section)
#
#     return [rows_by_name[k] for k in rows_by_name]
#
#
#
# data = extract_notifiers_resources_wide(sheet)
#
# #print(data)
#
# df = pd.DataFrame(data)
# print(df)
#
#
# ### Test functions:
#
# # def add_info_CPS_right_until_empty_res(sheet, rowlabel, column_offsets, column_names, maindatabase, newdatabase, mainID):
# #     """
# #     Like add_info_CPS_one_cell_right, but starting from column_offsets[0] to the right,
# #     keeps reading consecutive cells until it finds the first empty cell.
# #     Column naming:
# #       - first value uses column_names[0] (base)
# #       - next values use column_names[1:], if present
# #       - beyond that, auto-name as base-1, base-2, ...
# #       - also captures the sheet's 'Resource' column value (if present on that row)
# #         into SQL column 'resource-<sanitized rowlabel>'.
# #     """
# #     if len(column_offsets) != len(column_names):
# #         raise ValueError("column_offsets and column_names must have the same length")
# #     if not column_offsets:
# #         return
# #
# #     # Quote identifiers for SQL safety
# #     def q(name: str) -> str:
# #         return f'"{name}"'
# #
# #     # Sanitize rowlabel for safe SQL column naming
# #     def sanitize_label(s: str) -> str:
# #         s = (s or "").strip().lower()
# #         s = s.replace(" ", "-")
# #         s = re.sub(r"[^a-z0-9_\-]", "", s)
# #         s = re.sub(r"-{2,}", "-", s)
# #         return s or "unnamed"
# #
# #     safe_rowlabel = sanitize_label(rowlabel)
# #     resource_colname = f"resource-{safe_rowlabel}"
# #
# #     # --- locate the cell containing rowlabel ---
# #     match_row_idx = None
# #     match_col_idx = None
# #     for row in sheet.iter_rows():
# #         for cell in row:
# #             if cell.value is not None and rowlabel.lower() in str(cell.value).lower():
# #                 match_row_idx = cell.row
# #                 # prefer numeric index (openpyxl)
# #                 match_col_idx = getattr(cell, "col_idx", cell.column)
# #                 break
# #         if match_row_idx is not None:
# #             break
# #
# #     if match_row_idx is None:
# #         return  # nothing to insert
# #
# #     # Find the column index for "Resource" (sheet header named 'Resource')
# #     resource_col = None
# #     for row in sheet.iter_rows():
# #         for cell in row:
# #             if cell.value and str(cell.value).strip().lower() == "resource":
# #                 resource_col = getattr(cell, "col_idx", cell.column)
# #                 break
# #         if resource_col:
# #             break
# #
# #     # Determine start offset and base name
# #     start_offset = column_offsets[0]
# #     base_name = column_names[0]
# #
# #     # --- read to the right until the first empty cell ---
# #     extracted_data = {}
# #     k = 0
# #     max_col = sheet.max_column
# #     while (match_col_idx + start_offset + k) <= max_col:
# #         target = sheet.cell(row=match_row_idx, column=match_col_idx + start_offset + k)
# #         tv = target.value
# #         # stop at first empty/blank
# #         if tv is None or (isinstance(tv, str) and tv.strip() == ""):
# #             break
# #
# #         # choose column name
# #         if k < len(column_names):
# #             col_name = column_names[k]
# #         else:
# #             col_name = f"{base_name}-{k - (len(column_names) - 1)}" if len(column_names) > 0 else f"col-{k}"
# #
# #         extracted_data[col_name] = tv
# #         k += 1
# #
# #     # Add the Resource value (may be None if column not found); always include the column
# #     extracted_data[resource_colname] = sheet.cell(row=match_row_idx, column=resource_col).value if resource_col else None
# #
# #     if not extracted_data:
# #         return  # nothing to insert
# #
# #     # --- ensure table exists and has needed columns ---
# #     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (newdatabase,))
# #     table_exists = cursor.fetchone()
# #
# #     # set of all columns we might write this time
# #     needed_columns = list(extracted_data.keys())
# #
# #     if not table_exists:
# #         cols_def = ", ".join([f"{q(col)} TEXT" for col in needed_columns])
# #         fk_clause = f", FOREIGN KEY (ref) REFERENCES {q(maindatabase)}(ID)" if newdatabase != maindatabase else ""
# #         cursor.execute(f'''
# #             CREATE TABLE {q(newdatabase)} (
# #                 ID INTEGER PRIMARY KEY AUTOINCREMENT,
# #                 ref TEXT
# #                 {"," if cols_def else ""} {cols_def}
# #                 {fk_clause}
# #             )
# #         ''')
# #     else:
# #         cursor.execute(f"PRAGMA table_info({q(newdatabase)})")
# #         existing_cols = [col[1] for col in cursor.fetchall()]
# #         if "ref" not in existing_cols and newdatabase != maindatabase:
# #             cursor.execute(f"ALTER TABLE {q(newdatabase)} ADD COLUMN ref TEXT")
# #         for col in needed_columns:
# #             if col not in existing_cols:
# #                 cursor.execute(f"ALTER TABLE {q(newdatabase)} ADD COLUMN {q(col)} TEXT")
# #
# #     # --- upsert (same keying rules as your working function) ---
# #     if newdatabase != maindatabase:
# #         cursor.execute(f"SELECT 1 FROM {q(newdatabase)} WHERE ref = ?", (mainID,))
# #         exists = cursor.fetchone()
# #         if exists:
# #             update_clause = ", ".join([f"{q(col)} = ?" for col in needed_columns])
# #             cursor.execute(
# #                 f"UPDATE {q(newdatabase)} SET {update_clause} WHERE ref = ?",
# #                 [extracted_data[col] for col in needed_columns] + [mainID]
# #             )
# #         else:
# #             all_cols = ['ref'] + needed_columns
# #             placeholders = ", ".join(["?"] * len(all_cols))
# #             cursor.execute(
# #                 f"INSERT INTO {q(newdatabase)} ({', '.join(q(col) for col in all_cols)}) VALUES ({placeholders})",
# #                 [mainID] + [extracted_data[col] for col in needed_columns]
# #             )
# #     else:
# #         cursor.execute(f"SELECT 1 FROM {q(newdatabase)} WHERE ID = ?", (mainID,))
# #         exists = cursor.fetchone()
# #         if exists:
# #             update_clause = ", ".join([f"{q(col)} = ?" for col in needed_columns])
# #             cursor.execute(
# #                 f"UPDATE {q(newdatabase)} SET {update_clause} WHERE ID = ?",
# #                 [extracted_data[col] for col in needed_columns] + [mainID]
# #             )
# #         else:
# #             all_cols = ['ID'] + needed_columns
# #             placeholders = ", ".join(["?"] * len(all_cols))
# #             cursor.execute(
# #                 f"INSERT INTO {q(newdatabase)} ({', '.join(q(col) for col in all_cols)}) VALUES ({placeholders})",
# #                 [mainID] + [extracted_data[col] for col in needed_columns]
# #             )
#
#
# # This function works but puts the resource column for all the sheets, so even if its not given (=empty row), the resource column will appear
# # def add_info_CPS_right_until_empty(sheet, rowlabel, column_offsets, column_names, maindatabase, newdatabase, mainID, include_resource=True):
# #     """
# #     Like add_info_CPS_one_cell_right, but starting from column_offsets[0] to the right,
# #     keeps reading consecutive cells until it finds the first empty cell.
# #     Column naming:
# #       - first value uses column_names[0] (base)
# #       - next values use column_names[1:], if present
# #       - beyond that, auto-name as base-1, base-2, ...
# #       - If include_resource=True, also captures the sheet's 'Resource' column value (if present on that row)
# #         into SQL column 'resource-<sanitized rowlabel>'.
# #     """
# #     if len(column_offsets) != len(column_names):
# #         raise ValueError("column_offsets and column_names must have the same length")
# #     if not column_offsets:
# #         return
# #
# #     # Quote identifiers for SQL safety
# #     def q(name: str) -> str:
# #         return f'"{name}"'
# #
# #     # Sanitize rowlabel for safe SQL column naming
# #     def sanitize_label(s: str) -> str:
# #         s = (s or "").strip().lower()
# #         s = s.replace(" ", "-")
# #         s = re.sub(r"[^a-z0-9_\-]", "", s)
# #         s = re.sub(r"-{2,}", "-", s)
# #         return s or "unnamed"
# #
# #     safe_rowlabel = sanitize_label(rowlabel)
# #     resource_colname = f"resource-{safe_rowlabel}"
# #
# #     # --- locate the cell containing rowlabel ---
# #     match_row_idx = None
# #     match_col_idx = None
# #     for row in sheet.iter_rows():
# #         for cell in row:
# #             if cell.value is not None and rowlabel.lower() in str(cell.value).lower():
# #                 match_row_idx = cell.row
# #                 # prefer numeric index (openpyxl)
# #                 match_col_idx = getattr(cell, "col_idx", cell.column)
# #                 break
# #         if match_row_idx is not None:
# #             break
# #
# #     if match_row_idx is None:
# #         return  # nothing to insert
# #
# #     # Optionally find the column index for "Resource"
# #     resource_col = None
# #     if include_resource:
# #         for row in sheet.iter_rows():
# #             for cell in row:
# #                 if cell.value and str(cell.value).strip().lower() == "resource":
# #                     resource_col = getattr(cell, "col_idx", cell.column)
# #                     break
# #             if resource_col:
# #                 break
# #
# #     # Determine start offset and base name
# #     start_offset = column_offsets[0]
# #     base_name = column_names[0]
# #
# #     # --- read to the right until the first empty cell ---
# #     extracted_data = {}
# #     k = 0
# #     max_col = sheet.max_column
# #     while (match_col_idx + start_offset + k) <= max_col:
# #         target = sheet.cell(row=match_row_idx, column=match_col_idx + start_offset + k)
# #         tv = target.value
# #         # stop at first empty/blank
# #         if tv is None or (isinstance(tv, str) and tv.strip() == ""):
# #             break
# #
# #         # choose column name
# #         if k < len(column_names):
# #             col_name = column_names[k]
# #         else:
# #             col_name = f"{base_name}-{k - (len(column_names) - 1)}" if len(column_names) > 0 else f"col-{k}"
# #
# #         extracted_data[col_name] = tv
# #         k += 1
# #
# #     # Optionally add the Resource value
# #     if include_resource:
# #         extracted_data[resource_colname] = (
# #             sheet.cell(row=match_row_idx, column=resource_col).value if resource_col else None
# #         )
# #
# #     if not extracted_data:
# #         return  # nothing to insert
# #
# #     # --- ensure table exists and has needed columns ---
# #     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (newdatabase,))
# #     table_exists = cursor.fetchone()
# #
# #     needed_columns = list(extracted_data.keys())
# #
# #     if not table_exists:
# #         cols_def = ", ".join([f"{q(col)} TEXT" for col in needed_columns])
# #         fk_clause = f", FOREIGN KEY (ref) REFERENCES {q(maindatabase)}(ID)" if newdatabase != maindatabase else ""
# #         cursor.execute(f'''
# #             CREATE TABLE {q(newdatabase)} (
# #                 ID INTEGER PRIMARY KEY AUTOINCREMENT,
# #                 ref TEXT
# #                 {"," if cols_def else ""} {cols_def}
# #                 {fk_clause}
# #             )
# #         ''')
# #     else:
# #         cursor.execute(f"PRAGMA table_info({q(newdatabase)})")
# #         existing_cols = [col[1] for col in cursor.fetchall()]
# #         if "ref" not in existing_cols and newdatabase != maindatabase:
# #             cursor.execute(f"ALTER TABLE {q(newdatabase)} ADD COLUMN ref TEXT")
# #         for col in needed_columns:
# #             if col not in existing_cols:
# #                 cursor.execute(f"ALTER TABLE {q(newdatabase)} ADD COLUMN {q(col)} TEXT")
# #
# #     # --- upsert logic (same keying as your previous working function) ---
# #     if newdatabase != maindatabase:
# #         cursor.execute(f"SELECT 1 FROM {q(newdatabase)} WHERE ref = ?", (mainID,))
# #         exists = cursor.fetchone()
# #         if exists:
# #             update_clause = ", ".join([f"{q(col)} = ?" for col in needed_columns])
# #             cursor.execute(
# #                 f"UPDATE {q(newdatabase)} SET {update_clause} WHERE ref = ?",
# #                 [extracted_data[col] for col in needed_columns] + [mainID]
# #             )
# #         else:
# #             all_cols = ['ref'] + needed_columns
# #             placeholders = ", ".join(["?"] * len(all_cols))
# #             cursor.execute(
# #                 f"INSERT INTO {q(newdatabase)} ({', '.join(q(col) for col in all_cols)}) VALUES ({placeholders})",
# #                 [mainID] + [extracted_data[col] for col in needed_columns]
# #             )
# #     else:
# #         cursor.execute(f"SELECT 1 FROM {q(newdatabase)} WHERE ID = ?", (mainID,))
# #         exists = cursor.fetchone()
# #         if exists:
# #             update_clause = ", ".join([f"{q(col)} = ?" for col in needed_columns])
# #             cursor.execute(
# #                 f"UPDATE {q(newdatabase)} SET {update_clause} WHERE ID = ?",
# #                 [extracted_data[col] for col in needed_columns] + [mainID]
# #             )
# #         else:
# #             all_cols = ['ID'] + needed_columns
# #             placeholders = ", ".join(["?"] * len(all_cols))
# #             cursor.execute(
# #                 f"INSERT INTO {q(newdatabase)} ({', '.join(q(col) for col in all_cols)}) VALUES ({placeholders})",
# #                 [mainID] + [extracted_data[col] for col in needed_columns]
# #             )
