import pandas as pd
import itertools
import threading
import time
import uuid
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import os
from datetime import datetime

### Select excel file
def open_excel_file():
    messagebox.showinfo("Selection of the excel MAS", "In the next step please select the MAS that will be merged together.")
    root = tk.Tk()
    root.withdraw()
    try:
        file_path = filedialog.askopenfilename(
            title="Select an Excel file",
            filetypes=[("Excel files", "*.xlsx *.xlsm"),("All files", "*.*")])
        if file_path:
            if file_path.lower().endswith(('.xlsx', '.xlsm')):
                return file_path
            else:
                print("Selected file is not an Excel")
                return None
        else:
            print("No file selected")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
### Select folder to save data:
def select_output_file():
    messagebox.showinfo("Save location", "In the next step please select how to save the file.")

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    # Default to Downloads folder
    default_path = os.path.join(os.path.expanduser("~"), "Downloads")

    # Create default filename with date
    date_str = datetime.now().strftime("%Y-%m-%d")
    default_filename = f"MAS_merged_{date_str}.xlsx"

    file_path = filedialog.asksaveasfilename(
        title="Select where to save the file",
        initialdir=default_path,
        initialfile=default_filename,
        defaultextension=".xlsx",
        filetypes=[("Excel files", "*.xlsx")]
    )

    root.destroy()

    if file_path:
        return file_path
    else:
        print("No file selected.")
        return None
### spinn the spinner
def start_spinner(message="Running"):
    stop_event = threading.Event()

    def spinner():
        for char in itertools.cycle("|/-\\"):
            if stop_event.is_set():
                break
            print(f"\r{message}... {char}", end="", flush=True)
            time.sleep(0.1)

        print("\rDone.          ")

    thread = threading.Thread(target=spinner, daemon=True)
    thread.start()

    return stop_event
### Get max tier
def get_max_tier():
    while True:
        value = input("Final Tier ")
        try:
            max_tier = int(value)
            if max_tier > 0:
                return max_tier
            else:
                print("Please enter a positive number.")
        except ValueError:
            print("Invalid input. Please enter a number.")
### choosing options:
def get_choice():
    while True:
        value = input("Choose option (a/b): ").strip().lower()

        if value in ["a", "b"]:
            return value
        else:
            print("Invalid input. Please enter 'a' or 'b'.")
### normalize a join-key column safely, regardless of its original dtype
def normalize_key_column(series):
    """
    Turns a column into normalized (stripped, lowercased) string keys for joining,
    without corrupting the values along the way:
    - blanks/NaN each get their own unique, unmatchable sentinel instead of
      becoming the literal string "nan" (or plain NaN/None, which pandas'
      merge treats as equal to other NaNs). Without this, every blank row in
      one sheet would silently join to every blank row in the other.
    - whole-number floats (e.g. 12345.0, which is how pandas reads a numeric
      code column with any blank cells) are rendered as "12345", not
      "12345.0", so they still match the same code stored as a plain int or
      string elsewhere.
    Applied uniformly to every tier, including Tier 1, so the start sheet
    isn't treated differently from the rest.
    """
    def _key(value):
        if pd.isna(value):
            return f"__blank_{uuid.uuid4().hex}__"
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        return str(value).strip().lower()

    return series.map(_key)
### join the tiers
def join_tier_sheets(input_file, max_tier):
    """
    Reads an Excel file with sheets:
    PR-HM-T1, T1-T2, T2-T3, ...

    Asks user how many tiers exist.
    If user enters 5, last expected sheet is T4-T5.

    Performs sequential left joins:
    PR-HM-T1 left join T1-T2 on "Tier 1 Material"
    result left join T2-T3 on "Tier 2 Material"
    result left join T3-T4 on "Tier 3 Material"
    etc.

    Exports final joined dataframe to Excel.

    Returns (final_df, reached_tier): reached_tier is the highest tier that
    was actually merged in, which can be lower than max_tier if a tier
    transition sheet is missing (the merge stops there rather than failing).
    """
    max_tier = int(max_tier)
    # Get all sheet names
    print("Processing Excel file...")
    xls = pd.ExcelFile(input_file)
    sheet_names = xls.sheet_names

    # Determine starting sheet (flexible)
    print("Detecting start sheet...")
    if "PR-HM-T1" in sheet_names:
        start_sheet = "PR-HM-T1"
    elif "P-HM-T1" in sheet_names:
        start_sheet = "P-HM-T1"
    else:
        raise ValueError("Neither 'PR-HM-T1' nor 'P-HM-T1' found in file")

    print(f"Using start sheet: {start_sheet}")

    # Read first sheet
    final_df = pd.read_excel(input_file, sheet_name=start_sheet)

    final_df['Normalized Material 1'] = normalize_key_column(final_df['Tier 1 Material'])

    reached_tier = 1
    # Loop through tier transition sheets
    for i in range(1, max_tier):
        # Handle both naming styles: T2-T3 and T2_T3
        print(f"Processing Tier {i}")
        possible_names = [f"T{i}-T{i + 1}", f"T{i}_T{i + 1}"]
        sheet_name = next((s for s in possible_names if s in sheet_names), None)

        if sheet_name is None:
            print(f"No sheet found for Tier {i}-{i + 1}. Stopping at Tier {i}.")
            break
        join_column_name = f"Tier {i} Material"
        next_tier_column_name = f"Tier {i + 1} Material"
        print(f"Joining sheet '{sheet_name}' on column '{join_column_name}'")

        # Read next sheet
        next_df = pd.read_excel(input_file, sheet_name=sheet_name)

        # Normalize the names so they are not with spaces and not case-sensitive
        normalized_material = f"Normalized Material {i}"
        next_df[normalized_material] = normalize_key_column(next_df[join_column_name])
        normalized_next_tier_column_name = f"Normalized Material {i+1}"
        next_df[normalized_next_tier_column_name] = normalize_key_column(next_df[next_tier_column_name])

        join_column = normalized_material

        # Check join column exists
        if join_column not in final_df.columns:
            raise KeyError(f"Column '{normalized_material}' not found in joined dataframe")

        if join_column not in next_df.columns:
            raise KeyError(f"Column '{normalized_material}' not found in sheet '{sheet_name}'")

        # Left join
        final_df = final_df.merge(
            next_df,
            how="left",
            on=join_column,
            suffixes=("", f"_T{i + 1}")
        )
        reached_tier = i + 1
    for i in range(1, max_tier+1):
        # Define columns to drop and drop them
        cols_to_drop = [f"Normalized Material {i}", f"Tier {i} Material_T{i + 1}", f"Tier {i} Supplier_T{i + 1}"]

        cols_to_drop_existing = [col for col in cols_to_drop if col in final_df.columns]

        if cols_to_drop_existing:
            final_df.drop(columns=cols_to_drop_existing, inplace=True)

    final_df = final_df.dropna(how="all")

    return final_df, reached_tier
### if we want to merge on suppliers too:
def join_tier_sheets_with_suppliers(input_file, max_tier):
    max_tier = int(max_tier)

    print("Processing Excel file...")
    xls = pd.ExcelFile(input_file)
    sheet_names = xls.sheet_names

    print("Detecting start sheet...")
    if "PR-HM-T1" in sheet_names:
        start_sheet = "PR-HM-T1"
    elif "P-HM-T1" in sheet_names:
        start_sheet = "P-HM-T1"
    else:
        raise ValueError("Neither 'PR-HM-T1' nor 'P-HM-T1' found in file")

    print(f"Using start sheet: {start_sheet}")

    final_df = pd.read_excel(input_file, sheet_name=start_sheet)
    final_df['Normalized Material 1'] = normalize_key_column(final_df['Tier 1 Material'])
    final_df['Normalized Supplier 1'] = normalize_key_column(final_df['Tier 1 Supplier'])

    reached_tier = 1
    for i in range(1, max_tier):
        print(f"Processing Tier {i}")

        possible_names = [f"T{i}-T{i + 1}", f"T{i}_T{i + 1}"]
        sheet_name = next((s for s in possible_names if s in sheet_names), None)

        if sheet_name is None:
            print(f"No sheet found for Tier {i}-{i + 1}. Stopping at Tier {i}.")
            break

        material_col_to_norm = f"Tier {i} Material"
        supplier_col_to_norm = f"Tier {i} Supplier"
        next_material_col = f"Tier {i + 1} Material"
        next_supp_col = f"Tier {i + 1} Supplier"
        print(f"Joining '{sheet_name}' on '{material_col_to_norm}' and '{supplier_col_to_norm}'")

        next_df = pd.read_excel(input_file, sheet_name=sheet_name)

        material_col = f"Normalized Material {i}"
        next_df[material_col] = normalize_key_column(next_df[material_col_to_norm])
        supplier_col = f"Normalized Supplier {i}"
        next_df[supplier_col] = normalize_key_column(next_df[supplier_col_to_norm])

        normalized_next_tier_column_name = f"Normalized Material {i+1}"
        next_df[normalized_next_tier_column_name] = normalize_key_column(next_df[next_material_col])

        normalized_next_tier_column_name = f"Normalized Supplier {i+1}"
        next_df[normalized_next_tier_column_name] = normalize_key_column(next_df[next_supp_col])

        # Check columns exist - a data problem (not just "no more tiers"), so
        # raise just like join_tier_sheets does, instead of silently
        # returning a partial merge that looks identical to a successful run.
        for col in [material_col, supplier_col]:
            if col not in final_df.columns:
                raise KeyError(f"Column '{col}' not found in joined dataframe")
            if col not in next_df.columns:
                raise KeyError(f"Column '{col}' not found in sheet '{sheet_name}'")

        # Merge on TWO columns
        final_df = final_df.merge(
            next_df,
            how="left",
            on=[material_col, supplier_col],
            suffixes=("", f"_T{i + 1}")
        )
        reached_tier = i + 1
    for i in range(1, max_tier+1):
        # Define columns to drop and drop them
        cols_to_drop = [f"Normalized Material {i}",f"Normalized Supplier {i}", f"Tier {i} Material_T{i + 1}", f"Tier {i} Supplier_T{i + 1}"]

        cols_to_drop_existing = [col for col in cols_to_drop if col in final_df.columns]

        if cols_to_drop_existing:
            final_df.drop(columns=cols_to_drop_existing, inplace=True)

    final_df = final_df.dropna(how="all")

    return final_df, reached_tier
### export as excel
def join_to_excel(final_df, output_file):

    # Export with table formatting
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        final_df.to_excel(writer, sheet_name="Result", index=False)

        workbook = writer.book
        worksheet = writer.sheets["Result"]

        # Get dimensions
        (max_row, max_col) = final_df.shape

        # Create column headers for table
        column_settings = [{"header": col} for col in final_df.columns]

        # Add Excel table
        worksheet.add_table(0, 0, max_row, max_col - 1, {
            "columns": column_settings,
            "style": "Table Style Medium 2"  # you can change style
        })

        # set column width nicely
        for i, col in enumerate(final_df.columns):
            worksheet.set_column(i, i, 20)

    # print(f"File saved to: {output_file}")

### CLI entry point - only runs when this file is executed directly, not when
### it is imported (e.g. by MAS_generator_app.py, which reuses the functions
### above with its own GUI-driven inputs instead of these console prompts).
if __name__ == "__main__":
    print("--------------------------------------------------------------")
    print("Select the Excel file (MAS) to analyse.")
    input_file = open_excel_file()
    print("--------------------------------------------------------------")
    output_file = select_output_file()
    print("Choose the name of the file to save.")
    print("--------------------------------------------------------------")
    print("How many Tiers are there?")
    # Ask user how many tiers there are
    max_tier = get_max_tier()
    print("")
    print("--------------------------------------------------------------")
    print("Merge on:"
          " \n a. materials column (e.g. Tier 1 Material)"
          "\n or "
          "\n b. materials and suppliers (e.g. Tier 1 Material + Tier 1 Supplier) Disclaimer:be careful to have the supplier in both excel sheets as the program looks for exact match"
          "\n"
          "\n Write a or b")
    choice = get_choice()
    print("--------------------------------------------------------------")
    print("Merging...")
    final_df = None
    reached_tier = None
    if choice == "a":
        final_df, reached_tier = join_tier_sheets(input_file, max_tier)
    if choice == "b":
        final_df, reached_tier = join_tier_sheets_with_suppliers(input_file, max_tier)
    if reached_tier is not None and reached_tier < max_tier:
        print(f"WARNING: merge stopped at Tier {reached_tier} (requested Tier {max_tier}) - a tier sheet was missing.")
    print("--------------------------------------------------------------")
    if final_df is not None:
        spinner_stop = start_spinner("Saving")
        try:
            join_to_excel(final_df, output_file)
        finally:
            spinner_stop.set()
    print("--------------------------------------------------------------")
    print(f"File saved to: {output_file}")
