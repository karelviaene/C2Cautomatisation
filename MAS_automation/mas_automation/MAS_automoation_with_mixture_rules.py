### Version 2 takes into account different products

### Files to import
import pandas as pd
import numpy as np
import itertools
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import re
import os
from datetime import datetime
from tqdm import tqdm
import sqlite3
from collections import Counter

### Adjust cols names if the template changes
#############################################
col_mat = "Tier {i} Material"
col_sup = "Tier {i} Supplier"
col_CAS = "CAS Tier {i}"
col_tier_depth = "Tier {i} Material"
product = "Product"
hom_mat = "Homogenous Material"
col_is_alternative_y_n = "Is alternative of tier {i} material"
col_is_alternative_material = "Is alternative of tier {i} material (of what?)"
col_coupling_y_n = "Coupled to tier {i} material (only present if coupled material is present)"
col_coupling_material = "Coupled to tier {i} material (only present if coupled material is present) (of what?)"
min_percent_in_product = "Min % Homogenous material in Product"
max_percent_in_product = "Max % Homogenous material in Product"
min_weight_in_product = "Min weight Homogenous material in Product"
max_weight_in_product ="Max weight Homogenous material in Product"
min_percent_in_hom_mat = "Tier 1 Material Weight% Min"
max_percent_in_hom_mat = "Tier 1 Material Weight% Max"
min_weight_in_hom_mat = "Tier 1 Material Weight Min"
max_weight_in_hom_mat = "Tier 1 Material Weight Max"
col_mat_tier_1 = "Tier 1 Material"
col_min_perc = "Tier {i} Material Weight% Min"
col_max_perc = "Tier {i} Material Weight% Max"
#############################################
##### FUNCTIONS ####
### Read the file from the selected excel:
def open_excel_file():
    messagebox.showinfo(
        "Selection of the excel MAS",
        "In the next step please select the MAS, make sure the data for the analysis is in the first sheet."
    )

    root = tk.Tk()
    root.withdraw()

    try:
        file_path = filedialog.askopenfilename(
            title="Select an Excel file",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )

        if file_path:
            if file_path.lower().endswith(('.xlsx', '.xls')):
                file_name = os.path.basename(file_path)
                df = pd.read_excel(file_path)

                # 👉 derive folder from file location
                folder_path = os.path.dirname(file_path)

                return df, file_name, folder_path

            else:
                print("Selected file is not an Excel")
                return None, None, None
        else:
            print("No file selected")
            return None, None, None

    except Exception as e:
        print(f"Error: {e}")
        return None, None, None
### Select folder to save data:
def select_folder(default_path=None):

    messagebox.showinfo(
        "Save location",
        "In the next step please select where to save the file."
    )

    root = tk.Tk()
    root.withdraw()

    if not default_path or not os.path.exists(default_path):
        default_path = os.path.expanduser("~")

    folder_path = filedialog.askdirectory(
        title="Select where to save the file",
        initialdir=default_path   # 👈 key line
    )

    root.destroy()

    if folder_path:
        return folder_path
    else:
        print("No folder selected.")
        return None
### Open SQL file
def open_sql_file():
    messagebox.showinfo(
        "Selection of SQL database",
        "In the next step please select the SQL database file."
    )

    root = tk.Tk()
    root.withdraw()

    try:
        file_path = filedialog.askopenfilename(
            title="Select SQL database file",
            filetypes=[
                ("Database files", "*.db *.sqlite *.sqlite3"),
                ("All files", "*.*")
            ]
        )

        if file_path:
            if file_path.lower().endswith((".db", ".sqlite", ".sqlite3")):
                file_name = os.path.basename(file_path)
                return file_path, file_name
            else:
                print("Selected file is not a supported SQL database file.")
                return None, None
        else:
            print("No file selected.")
            return None, None

    except Exception as e:
        print(f"Error: {e}")
        return None, None

    finally:
        root.destroy()
### Open excel with toxicity info:
def open_excel_file_toxicity():
    messagebox.showinfo("Selection of the excel with toxicity info", "In the next step please select the excel file with toxicity info, make sure the data for the analysis in the first excel sheet.")
    root = tk.Tk()
    root.withdraw()
    try:
        file_path = filedialog.askopenfilename(
            title="Select an Excel file",
            filetypes=[("Excel files", "*.xlsx *.xls"),("All files", "*.*")])
        if file_path:
            if file_path.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
                return df
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
        return None, None
### Clean data: add a col row_id for an identifier & normalize Y/N in capital letters etc
def clean_data(df, tier_level=10):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # normalize yes/no
    mapping = {
        "yes": "yes",
        "no": "no",
        "Yes": "yes",
        "No": "no"
    }
    df = df.apply(lambda col: col.map(mapping).fillna(col) if col.dtype == "object" else col)

    # add row id
    df["row_id"] = range(1, len(df) + 1)

    # clean spaces in text columns
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.strip()

    # helper to clean numeric columns
    def clean_numeric_series(series, col_name=None):
        # Step 1: normalize basic formatting
        s = series.astype(str).str.strip().str.replace(",", ".", regex=False)

        # Step 2: detect non-numeric BEFORE coercion
        numeric_check = pd.to_numeric(s, errors="coerce")
        mask_bad = numeric_check.isna() & s.notna() & (s != "")

        if mask_bad.any():
            print(f"Non-numeric values found in column: {col_name}")
            print(s[mask_bad].unique())

        # Step 3: clean problematic characters (light cleaning only)
        s_clean = (
            s.str.replace("%", "", regex=False)
            .str.replace("<", "", regex=False)
            .str.replace(">", "", regex=False)
        )

        # Step 4: convert to numeric
        result = pd.to_numeric(s_clean, errors="coerce")

        # Step 5: enforce float64
        return result.astype("float64")
        # columns that must be numeric


    # columns that must be numeric
    numeric_cols = [
        min_percent_in_product,
        max_percent_in_product,
        min_weight_in_product,
        max_weight_in_product,
        min_percent_in_hom_mat,
        max_percent_in_hom_mat,
        min_weight_in_hom_mat,
        max_weight_in_hom_mat,
    ]

    # flatten in case some of these are lists
    final_numeric_cols = []
    for item in numeric_cols:
        if isinstance(item, list):
            final_numeric_cols.extend(item)
        else:
            final_numeric_cols.append(item)

    # clean only columns that actually exist
    for col in final_numeric_cols:
        if col in df.columns:
            df[col] = clean_numeric_series(df[col])

    for i in range(1, tier_level + 1):
        min_col = col_min_perc.format(i=i)
        if min_col in df.columns:
            df[min_col] = clean_numeric_series(df[min_col])
        max_col = col_max_perc.format(i=i)
        if max_col in df.columns:
            df[max_col] = clean_numeric_series(df[max_col])

    return df
## getting the highest tier available
def get_highest_tier(df, col_pattern):
    numbers = []

    # Convert pattern into regex
    regex_pattern = col_pattern.replace("{i}", r"(\d+)")
    regex_pattern = f"{regex_pattern}"

    for col in df.columns:
        match = re.match(regex_pattern, col)
        if match:
            numbers.append(int(match.group(1)))

    if numbers:
        return max(numbers)
    else:
        print("Not determined max tier from the file, max tier is set to 10")
        return 10
### Get rows of the final material, final supplier, final CAS & the final tier depth
def get_final_material(row, tier_level=10):
    for i in range(tier_level, 0, -1):
        col = col_mat.format(i=i)
        if pd.notna(row.get(col)):
            return row[col]
    return None
def get_final_supplier(row, tier_level=10):
    for i in range(tier_level, 0, -1):
        col = col_sup.format(i=i)
        if pd.notna(row.get(col)):
            return row[col]
    return None
def get_final_CAS(row, tier_level=10):
    for i in range(tier_level, 0, -1):
        col = col_CAS.format(i=i)
        if pd.notna(row.get(col)):
            return row[col]
    return "not assessed"
def get_tier_depth(row, tier_level=10):
    for i in range(tier_level, 0, -1):
        col = col_tier_depth.format(i=i)
        if pd.notna(row.get(col)):
            return i
    return None
def add_helper_columns(df, max_tier):
    df = df.copy()
    df["CAS"] = df.apply(get_final_CAS,args=(max_tier,),  axis=1)
    df["final_material"] = df.apply(get_final_material, args=(max_tier,), axis=1)
    df["final_supplier"] = df.apply(get_final_supplier, args=(max_tier,), axis=1)
    df["tier_depth"] = df.apply(get_tier_depth,args=(max_tier,),  axis=1)
    return df
### Build location: Map all materials to their product Prod -> Hom mat -> Tier 1 (supp 1) -> Tier 2 (Sup 2) -> etc.
def build_location(row, tier_level=10):
    path = [row.get(product), row.get(hom_mat)]

    for i in range(1, tier_level + 1):
        col1 = col_mat.format(i=i)
        col_2 = col_sup.format(i=i)
        val1 = row.get(col1)
        val2 = row.get(col_2)
        val = f"{val1} ({val2})"

        if pd.notna(val):
            path.append(str(val))

        # stop once we reach the final tier depth
        if i == row.get("tier_depth"):
            break

    return " → ".join(str(item) for item in path if item is not None) if path else None
def add_final_map(df,max_tier):
    df = df.copy()
    df["final_material_map"] = df.apply(lambda r: build_location(r, max_tier), axis=1)
    return df
### Identify all the alternatives in the group
def identify_alternative_groups(df, tier_level=10):
    df = df.copy()

    def make_group(row, i):
        col_flag = col_is_alternative_y_n.format(i=i)
        col_anchor = col_is_alternative_material.format(i=i)

        if str(row.get(col_flag, "")).lower() == "yes":
            anchor = row.get(col_anchor)

            if i == 1:
                ref = row.get(hom_mat)
            else:
                i = i - 1
                col_mat_for_ref = col_mat.format(i=i)
                ref = row.get(col_mat_for_ref)
                i = i + 1

            return f"T{i}; {row.get(product)}; {ref}; {anchor}"

        return np.nan

    # Generate alternative group columns for each tier
    for i in range(1, tier_level + 1):
        df[f"t{i}_alt_group"] = df.apply(lambda row: make_group(row, i), axis=1)

    return df
### Make scenarios
def generate_scenarios(df, tier_level=10):
    scenarios = []

    # build scenarios separately for each product
    for product, product_df in df.groupby("Product", dropna=True):
        alt_choices = {}

        for i in range(1, tier_level + 1):
            group_col = f"t{i}_alt_group"
            material_col = col_mat.format(i=i)

            if group_col not in product_df.columns or material_col not in product_df.columns:
                continue

            subset = product_df.dropna(subset=[group_col])

            for group, grp in subset.groupby(group_col):
                choices = grp[material_col].dropna().unique().tolist()
                if choices:
                    alt_choices[group] = sorted(choices)

        # no alternatives for this product
        if not alt_choices:
            scenarios.append({
                "scenario_id": f"{product}_base",
                "product": product,
                "choices": {}
            })
            continue

        group_names = list(alt_choices.keys())

        for i, combo in enumerate(
            itertools.product(*(alt_choices[g] for g in group_names)),
            start=1
        ):
            choices = dict(zip(group_names, combo))
            scenarios.append({
                "scenario_id": f"{product}_scenario_{i}",
                "product": product,
                "choices": choices
            })

    return scenarios
### Check if the row is active (if the materials are to be included in the scenario or not)
def row_is_active(row, scenario, selected_materials, tier_level=10):

    # Product filtering
    scenario_product = scenario.get("product")
    row_product = row.get("Product")

    if pd.notna(scenario_product) and row_product != scenario_product:
        return False, "Excluded by product"

    # Alternative + coupling filtering
    for i in range(1, tier_level + 1):
        alt_group_col = f"t{i}_alt_group"
        material_col = col_mat.format(i=i)

        # Alternative filtering
        if pd.notna(row.get(alt_group_col)):
            chosen = scenario["choices"].get(row[alt_group_col])
            if chosen is not None and row.get(material_col) != chosen:
                return False, f"Excluded by Tier {i} alternative"

        # Coupling rule
        coupling_col1 = col_coupling_y_n.format(i=i)
        coupling_col2 = col_coupling_material.format(i=i)

        coupled_material = row.get(coupling_col2)
        coupling_yes_no = row.get(coupling_col1)

        if pd.notna(coupling_yes_no) and str(coupling_yes_no).strip().lower() == "yes":
            if coupled_material not in selected_materials:
                return False, f"Excluded by Tier {i} coupling"

    return True, "Active"
### Calculate the % contribution
def calc_row_contribution(row, tier_level=10):
    min_val_prod = row[min_percent_in_product] * row[min_percent_in_hom_mat]
    max_val_prod = row[max_percent_in_product] * row[max_percent_in_hom_mat]

    min_val_hom_mat = row[min_percent_in_hom_mat]
    max_val_hom_mat = row[max_percent_in_hom_mat]
    # Loop over tiers > 1
    for i in range(2, tier_level + 1):
        material_col = col_mat.format(i=i)
        min_col = col_min_perc.format(i=i)
        max_col = col_max_perc.format(i=i)

        if pd.notna(row.get(material_col)):
            min_val_prod *= row.get(min_col, 1)
            max_val_prod *= row.get(max_col, 1)
            min_val_hom_mat *= row.get(min_col, 1)
            max_val_hom_mat *= row.get(max_col, 1)


    return min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat
### Evaluate each scenario
def evaluate_row_activity(df, scenario, tier_level=10):
    df = df.copy()

    scenario_product = scenario.get("product")
    if scenario_product is not None:
        df = df[
            df["Product"].astype(str).str.strip().str.lower()
            == str(scenario_product).strip().lower()
        ].copy()

    selected_materials = set(scenario["choices"].values())

    active_flags = []
    reasons = []

    for _, row in df.iterrows():
        active, reason = row_is_active(
            row,
            scenario,
            selected_materials,
            tier_level=tier_level
        )
        active_flags.append(active)
        reasons.append(reason)

    df["scenario_id"] = scenario["scenario_id"]
    df["active"] = active_flags
    df["status_reason"] = reasons

    return df
### Calculate the % contribution per product
def calculate_material_percentages_product(df):
    df = df.copy()
    df_mass_calc = df.copy()
    keys = [product, min_weight_in_product, max_weight_in_product, hom_mat]
    only_active = df_mass_calc["active"] == True
    df_mass_calc_unique = df.loc[only_active, keys].drop_duplicates()

    def calculations_for_material_percentages_product(df):
        """  Calculate the percentage of material based on mass given (worst & best case scenarios)"""
        df = df.copy()
        min_col = min_weight_in_product
        max_col = max_weight_in_product
        group_cols = product
        df["total_min_product"] = df.groupby(group_cols)[min_col].transform("sum")
        df["total_max_product"] = df.groupby(group_cols)[max_col].transform("sum")

        df["rest_min"] = df["total_min_product"] - df[min_col]
        df["rest_max"] = df["total_max_product"] - df[max_col]

        df[min_percent_in_product] = df[min_col] / (df[min_col] + df["rest_max"])
        df[max_percent_in_product] = df[max_col] / (df[max_col] + df["rest_min"])

        return df
    #calculate_material_percentages_product(df_mass_calc_unique)
    df_mass_calc_unique = calculations_for_material_percentages_product(df_mass_calc_unique)
    #
    df_mass_calc_unique["key"] = list(zip(*(df_mass_calc_unique[k] for k in keys)))
    df["key"] = list(zip(*(df[k] for k in keys)))
    #
    min_map = df_mass_calc_unique.set_index("key")[min_percent_in_product]
    max_map = df_mass_calc_unique.set_index("key")[max_percent_in_product]

    df[min_percent_in_product] = df[min_percent_in_product].fillna(df["key"].map(min_map))
    df[max_percent_in_product] = df[max_percent_in_product].fillna(df["key"].map(max_map))
    df.drop(["key"], axis=1, inplace=True)
    return df
### Calculate the % contribution per homogenous material
def calculate_material_percentages_hom_mat(df):
    df = df.copy()
    df_mass_calc = df.copy()
    keys = [ hom_mat, min_weight_in_hom_mat, max_weight_in_hom_mat, col_mat_tier_1]
    only_active = df_mass_calc["active"] == True
    df_mass_calc_unique = df.loc[only_active, keys].drop_duplicates()

    def calculations_for_material_percentages_hom_mat(df):
        """  Calculate the percentage of material based on mass given (worst & best case scenarios)"""
        df = df.copy()
        min_col = min_weight_in_hom_mat
        max_col = max_weight_in_hom_mat
        group_cols = hom_mat
        df["total_min_product"] = df.groupby(group_cols)[min_col].transform("sum")
        df["total_max_product"] = df.groupby(group_cols)[max_col].transform("sum")

        df["rest_min"] = df["total_min_product"] - df[min_col]
        df["rest_max"] = df["total_max_product"] - df[max_col]

        df[min_percent_in_hom_mat] = df[min_col] / (df[min_col] + df["rest_max"])
        df[max_percent_in_hom_mat] = df[max_col] / (df[max_col] + df["rest_min"])

        return df
    #calculate_material_percentages_product(df_mass_calc_unique)
    df_mass_calc_unique = calculations_for_material_percentages_hom_mat(df_mass_calc_unique)

    df_mass_calc_unique["key"] = list(zip(*(df_mass_calc_unique[k] for k in keys)))
    df["key"] = list(zip(*(df[k] for k in keys)))

    min_map = df_mass_calc_unique.set_index("key")[min_percent_in_hom_mat]
    max_map = df_mass_calc_unique.set_index("key")[max_percent_in_hom_mat]

    df[min_percent_in_hom_mat] = df[min_percent_in_hom_mat].fillna(df["key"].map(min_map))
    df[max_percent_in_hom_mat] = df[max_percent_in_hom_mat].fillna(df["key"].map(max_map))
    df.drop(["key"], axis=1, inplace=True)
    return df
### calculating the % in product and hom mat
def calculate_row_contributions(df):
    df = df.copy()

    min_val_prod_contibutions = []
    max_val_prod_contibutions = []
    min_val_hom_mat_contibutions = []
    max_val_hom_mat_contibutions = []
    for _, row in df.iterrows():
        if row.get("active") is True:
            min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat = calc_row_contribution(row)
        else:
            min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat = np.nan, np.nan, np.nan, np.nan

        min_val_prod_contibutions.append(min_val_prod)
        max_val_prod_contibutions.append(max_val_prod)
        min_val_hom_mat_contibutions.append(min_val_hom_mat)
        max_val_hom_mat_contibutions.append(max_val_hom_mat)


    df["min_contribution_prod"] = min_val_prod_contibutions
    df["max_contribution_prod"] = max_val_prod_contibutions
    df["min_contribution_hom_mat"] = min_val_hom_mat_contibutions
    df["max_contribution_hom_mat"] = max_val_hom_mat_contibutions

    calc_df = df.copy()
    return calc_df
def update_low(record, key, value, scenario_id):
    if pd.isna(value):
        return
    value_col = f"{key}_value"
    scenario_col = f"{key}_scenario"

    if value_col not in record or pd.isna(record[value_col]) or value < record[value_col]:
        record[value_col] = value
        record[scenario_col] = scenario_id
def update_high(record, key, value, scenario_id):
    if pd.isna(value):
        return
    value_col = f"{key}_value"
    scenario_col = f"{key}_scenario"

    if value_col not in record or pd.isna(record[value_col]) or value > record[value_col]:
        record[value_col] = value
        record[scenario_col] = scenario_id
def build_selected_scenarios_df(df, scenarios, selected_scenario_ids):
    results = []

    selected_set = set(selected_scenario_ids)

    for scenario in scenarios:
        if scenario["scenario_id"] not in selected_set:
            continue

        scenario_df = evaluate_row_activity(df, scenario)
        product_percent_df = calculate_material_percentages_product(scenario_df)
        hom_mat_percent_df = calculate_material_percentages_hom_mat(product_percent_df)
        scenario_evaluated = calculate_row_contributions(hom_mat_percent_df).copy()
        results.append(scenario_evaluated)

    if results:
        return pd.concat(results, ignore_index=True)

    return pd.DataFrame()
def analyse_the_dataset_with_mixture_rules(df, scenarios, df_toxicity_info):
    metrics = [
        "min_contribution_prod",
        "max_contribution_prod",
        "min_contribution_hom_mat",
        "max_contribution_hom_mat"
    ]
    colour_rank = {
        "GREEN": 1,
        "YELLOW": 2,
        "GREY": 3,
        "RED": 4,
        "!!! SENS 1 OR 1A PRESENT !!!": 5,
    }

    def clean_colour(value):
        if pd.isna(value):
            return None
        value = str(value).strip().upper()
        return value if value in colour_rank else None

    def update_worst_colour(rec, endpoint, colour, scenario_id):
        colour = clean_colour(colour)
        if colour is None:
            return

        value_col = endpoint
        scenario_col = f"{endpoint}_scenario"

        if value_col not in rec:
            rec[value_col] = colour
            rec[scenario_col] = scenario_id
        elif colour_rank[colour] > colour_rank[rec[value_col]]:
            rec[value_col] = colour
            rec[scenario_col] = scenario_id

    summary = {}
    scenario_extremes = {}
    all_invalid_material_info = []
    c2c_scenario_extremes = {}
    c2c_by_hom_mat = {}
    all_c2c_scenario_results = []

    for scenario in tqdm(scenarios, desc="Scenarios", total=len(scenarios)):
        scenario_df = evaluate_row_activity(df, scenario)
        product_percent_df = calculate_material_percentages_product(scenario_df)
        hom_mat_percent_df = calculate_material_percentages_hom_mat(product_percent_df)
        scenario_evaluated = calculate_row_contributions(hom_mat_percent_df).copy()

        # ---------------------------------------------------------
        # % assessed calculation of scenarios per product
        # ---------------------------------------------------------

        # Keep only rows that actually have contributions
        active_mask = scenario_evaluated["active"].astype(str).str.upper().eq("TRUE")
        current = scenario_evaluated.loc[
            active_mask,
            ["row_id", "CAS", "final_material", "final_material_map", "scenario_id"] + metrics
        ].copy()

        # Update running absolute bounds
        for row in current.itertuples(index=False):
            row_id = row.row_id
            cas = row.CAS
            material = row.final_material
            material_map = row.final_material_map
            scenario_id = row.scenario_id

            key = (row_id, cas, material, material_map)

            rec = summary.setdefault(
                key,
                {
                    "row_id": row_id,
                    "CAS": cas,
                    "final_material": material,
                    "final_material_map": material_map,
                }
            )

            update_low(rec,  "abs_min_contribution_prod",    row.min_contribution_prod,    scenario_id)
            update_high(rec, "abs_max_contribution_prod",    row.max_contribution_prod,    scenario_id)
            update_low(rec,  "abs_min_contribution_hom_mat", row.min_contribution_hom_mat, scenario_id)
            update_high(rec, "abs_max_contribution_hom_mat", row.max_contribution_hom_mat, scenario_id)

        ####
        scenario_summaries = {}
        # Keep only active rows
        active_mask = scenario_evaluated["active"].astype(str).str.upper().eq("TRUE")

        current = scenario_evaluated.loc[
            active_mask,
            ["final_material","final_material_map" ,"scenario_id", "CAS", "min_contribution_prod", "max_contribution_prod"]
        ].copy()

        # Convert to numeric:
        for col in ["min_contribution_prod", "max_contribution_prod"]:
            current[col] = pd.to_numeric(
                current[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )
        # Identify if some rows do not have numerical values before summing up:
        invalid_rows = current[current["min_contribution_prod"].isna() | current["max_contribution_prod"].isna()]
        invalid_material_info = invalid_rows[["final_material", "final_material_map", "CAS"]]
        invalid_material_info = invalid_material_info.drop_duplicates(keep='first')
        all_invalid_material_info.append(invalid_material_info)

        # Sum per scenario:

        # get the name of each scenario
        scenario_id = scenario["scenario_id"]

        # Normalize CAS column once
        cas_clean = current["CAS"].str.strip().str.lower().fillna("")

        # Filter on not assessed
        not_assessed_df = current[cas_clean == "not assessed"]

        # CALC WITH NOT ASSESSED
        sum_min_not_assessed = not_assessed_df["min_contribution_prod"].sum(skipna=True)
        sum_max_not_assessed = not_assessed_df["max_contribution_prod"].sum(skipna=True)

        sum_min_calc_w_not_assessed = 1 - sum_max_not_assessed
        sum_max_calc_w_not_assessed = 1 - sum_min_not_assessed

        rec = scenario_extremes.setdefault("global", {})

        # % assessed calculation with not assessed
        if "abs_min_sum_min_prod_calc_w_not_assessed" not in rec or \
                sum_min_calc_w_not_assessed < rec["abs_min_sum_min_prod_calc_w_not_assessed"]:
            rec["abs_min_sum_min_prod_calc_w_not_assessed"] = sum_min_calc_w_not_assessed
            rec["abs_min_sum_min_prod_calc_w_not_assessed_scenario"] = scenario_id

        if "abs_max_sum_min_prod_calc_w_not_assessed" not in rec or \
                sum_min_calc_w_not_assessed > rec["abs_max_sum_min_prod_calc_w_not_assessed"]:
            rec["abs_max_sum_min_prod_calc_w_not_assessed"] = sum_min_calc_w_not_assessed
            rec["abs_max_sum_min_prod_calc_w_not_assessed_scenario"] = scenario_id

        if "abs_min_sum_max_prod_calc_w_not_assessed" not in rec or \
                sum_max_calc_w_not_assessed < rec["abs_min_sum_max_prod_calc_w_not_assessed"]:
            rec["abs_min_sum_max_prod_calc_w_not_assessed"] = sum_max_calc_w_not_assessed
            rec["abs_min_sum_max_prod_calc_w_not_assessed_scenario"] = scenario_id

        if "abs_max_sum_max_prod_calc_w_not_assessed" not in rec or \
                sum_max_calc_w_not_assessed > rec["abs_max_sum_max_prod_calc_w_not_assessed"]:
            rec["abs_max_sum_max_prod_calc_w_not_assessed"] = sum_max_calc_w_not_assessed
            rec["abs_max_sum_max_prod_calc_w_not_assessed_scenario"] = scenario_id

        # ---------------------------------------------------------
        # C2C mixture assessment for each scenario per homogeneous material
        # ---------------------------------------------------------
        scenario_id = scenario["scenario_id"]

        active_mask = scenario_evaluated["active"].astype(str).str.upper().eq("TRUE")

        active_product_df = scenario_evaluated.loc[
            active_mask,
            [
                'Product',
                "Homogenous Material",
                "CAS",
                "min_contribution_hom_mat",
                "max_contribution_hom_mat",
            ]
        ].copy()

        # Remove rows without CAS
        active_product_df = active_product_df[
            active_product_df["CAS"].notna()
        ].copy()

        # Only run if there are active rows
        if not active_product_df.empty:

            c2c_summary_df = mixture_rules_C2C_assessment(
                active_product_df,
                df_toxicity_info
            )

            # Add scenario ID for traceability
            c2c_summary_df["scenario_id"] = scenario_id
            all_c2c_scenario_results.append(c2c_summary_df)

            # These are the output columns from mixture_rules_C2C_assessment
            c2c_endpoint_cols = [
                "C2C acute toxicity",
                "C2C Skin, Eye, and Respiratory Irritation",
                "C2C Skin and Respiratory Sensitization",
                "C2C Acute and Chronic Aquatic Toxicity",
            ]

            # -----------------------------
            # Aggregate worst colour per HOMOGENEOUS MATERIAL
            # -----------------------------
            hom_col = "Homogenous Material"
            if hom_col not in c2c_summary_df.columns:
                if "hom_material" in c2c_summary_df.columns:
                    c2c_summary_df = c2c_summary_df.rename(columns={"hom_material": hom_col})
                else:
                    raise KeyError(f"No homogeneous material column found. Available: {list(c2c_summary_df.columns)}")

            for endpoint in c2c_endpoint_cols:
                if endpoint not in c2c_summary_df.columns:
                    continue

                # Group by homogeneous material
                for hom_mat, group_df in c2c_summary_df.groupby(hom_col):
                    # Clean colours and remove invalid
                    colours = [
                        clean_colour(v)
                        for v in group_df[endpoint].dropna().astype(str)
                    ]
                    colours = [c for c in colours if c is not None]

                    if not colours:
                        continue

                    # Determine worst colour
                    worst = max(colours, key=lambda x: colour_rank[x])

                    # Scenario where the worst occurred
                    match = group_df[group_df[endpoint].astype(str).str.upper() == worst]
                    worst_scenario = (
                        match["scenario_id"].iloc[0] if not match.empty else scenario_id
                    )

                    # Store in per-homogeneous-material dict
                    rec_hm = c2c_by_hom_mat.setdefault(hom_mat, {"Homogenous Material": hom_mat})

                    prev = rec_hm.get(endpoint)
                    if prev is None:
                        rec_hm[endpoint] = worst
                        rec_hm[f"{endpoint}_scenario"] = worst_scenario
                    elif colour_rank[worst] > colour_rank.get(prev, 0):
                        rec_hm[endpoint] = worst
                        rec_hm[f"{endpoint}_scenario"] = worst_scenario

        # -----------------------------
        # Convert to DataFrames for output
        # -----------------------------
        # Worst-case per homogenous material
        c2c_extremes_df = pd.DataFrame(c2c_by_hom_mat.values())

        # Optional full trace of all scenario results
        if all_c2c_scenario_results:
            all_c2c_scenario_results_df = pd.concat(all_c2c_scenario_results, ignore_index=True)
        else:
            all_c2c_scenario_results_df = pd.DataFrame()

    ##### SAVING THE % ASSESSED

    # summary per each CAS
    summary_df = (pd.DataFrame(summary.values()).sort_values("row_id").reset_index(drop=True))
    # CAS with no % in the prodcut
    final_invalid_material_info = pd.concat(all_invalid_material_info, ignore_index=True)
    final_invalid_material_info = final_invalid_material_info.drop_duplicates(keep='first')
    # best & worst case perecentage assessed:
    perecentage_assessed = pd.DataFrame([scenario_extremes["global"]])
    # find the worst case % assessed across the scenarios:

    # Find the smallest value in the numeric columns
    min_value = perecentage_assessed.iloc[:, ::2].min().min()  # Select numeric columns by slicing (even-indexed)
    # Find the column and the corresponding scenario for the smallest value
    # Get the column name for the smallest value
    min_column = perecentage_assessed.iloc[:, ::2].min().idxmin()
    # The scenario column
    min_scenario_column = min_column + '_scenario'
    # Retrieve the scenario
    min_scenario = perecentage_assessed[min_scenario_column].iloc[perecentage_assessed[min_column].idxmin()]
    # Create a dictionary to store the results
    abs_min_data_percentage_assessed = {}
    # Append the absolute min value and its scenario to a df:
    percent_assessed_df = pd.DataFrame({
        'Percentage_assessed': [min_value],
        'Scenario': [min_scenario]
    })
    perecentage_assessed_dict = {
        "Percentage Assessed": percent_assessed_df,
        "Invalid Material Info": final_invalid_material_info,
        "Storing calculations for percentage assessed": perecentage_assessed
    }


    return summary_df, perecentage_assessed_dict, c2c_extremes_df, all_c2c_scenario_results_df
def analyse_the_dataset(df, scenarios):
    metrics = [
        "min_contribution_prod",
        "max_contribution_prod",
        "min_contribution_hom_mat",
        "max_contribution_hom_mat"
    ]

    summary = {}
    scenario_extremes = {}
    all_invalid_material_info = []


    for scenario in tqdm(scenarios, desc="Scenarios", total=len(scenarios)):
        scenario_df = evaluate_row_activity(df, scenario)
        product_percent_df = calculate_material_percentages_product(scenario_df)
        hom_mat_percent_df = calculate_material_percentages_hom_mat(product_percent_df)
        scenario_evaluated = calculate_row_contributions(hom_mat_percent_df).copy()

        # Keep only rows that actually have contributions
        active_mask = scenario_evaluated["active"].astype(str).str.upper().eq("TRUE")
        current = scenario_evaluated.loc[
            active_mask,
            ["row_id", "CAS", "final_material", "final_material_map", "scenario_id"] + metrics
        ].copy()

        # Update running absolute bounds
        for row in current.itertuples(index=False):
            row_id = row.row_id
            cas = row.CAS
            material = row.final_material
            material_map = row.final_material_map
            scenario_id = row.scenario_id

            key = (row_id, cas, material, material_map)

            rec = summary.setdefault(
                key,
                {
                    "row_id": row_id,
                    "CAS": cas,
                    "final_material": material,
                    "final_material_map": material_map,
                }
            )

            update_low(rec,  "abs_min_contribution_prod",    row.min_contribution_prod,    scenario_id)
            update_high(rec, "abs_max_contribution_prod",    row.max_contribution_prod,    scenario_id)
            update_low(rec,  "abs_min_contribution_hom_mat", row.min_contribution_hom_mat, scenario_id)
            update_high(rec, "abs_max_contribution_hom_mat", row.max_contribution_hom_mat, scenario_id)

        ####
        scenario_summaries = {}
        # Keep only active rows
        active_mask = scenario_evaluated["active"].astype(str).str.upper().eq("TRUE")

        current = scenario_evaluated.loc[
            active_mask,
            ["final_material","final_material_map" ,"scenario_id", "CAS", "min_contribution_prod", "max_contribution_prod"]
        ].copy()

        # Convert to numeric:
        for col in ["min_contribution_prod", "max_contribution_prod"]:
            current[col] = pd.to_numeric(
                current[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )
        # Identify if some rows do not have numerical values before summing up:
        invalid_rows = current[current["min_contribution_prod"].isna() | current["max_contribution_prod"].isna()]
        invalid_material_info = invalid_rows[["final_material", "final_material_map", "CAS"]]
        invalid_material_info = invalid_material_info.drop_duplicates(keep='first')
        all_invalid_material_info.append(invalid_material_info)

        # Sum per scenario:

        # get the name of each scenario
        scenario_id = scenario["scenario_id"]

        # Normalize CAS column once
        cas_clean = current["CAS"].str.strip().str.lower().fillna("")

        # Filter on not assessed
        not_assessed_df = current[cas_clean == "not assessed"]

        # CALC WITH NOT ASSESSED
        sum_min_not_assessed = not_assessed_df["min_contribution_prod"].sum(skipna=True)
        sum_max_not_assessed = not_assessed_df["max_contribution_prod"].sum(skipna=True)

        sum_min_calc_w_not_assessed = 1 - sum_max_not_assessed
        sum_max_calc_w_not_assessed = 1 - sum_min_not_assessed

        rec = scenario_extremes.setdefault("global", {})

        # % assessed calculation with not assessed
        if "abs_min_sum_min_prod_calc_w_not_assessed" not in rec or \
                sum_min_calc_w_not_assessed < rec["abs_min_sum_min_prod_calc_w_not_assessed"]:
            rec["abs_min_sum_min_prod_calc_w_not_assessed"] = sum_min_calc_w_not_assessed
            rec["abs_min_sum_min_prod_calc_w_not_assessed_scenario"] = scenario_id

        if "abs_max_sum_min_prod_calc_w_not_assessed" not in rec or \
                sum_min_calc_w_not_assessed > rec["abs_max_sum_min_prod_calc_w_not_assessed"]:
            rec["abs_max_sum_min_prod_calc_w_not_assessed"] = sum_min_calc_w_not_assessed
            rec["abs_max_sum_min_prod_calc_w_not_assessed_scenario"] = scenario_id

        if "abs_min_sum_max_prod_calc_w_not_assessed" not in rec or \
                sum_max_calc_w_not_assessed < rec["abs_min_sum_max_prod_calc_w_not_assessed"]:
            rec["abs_min_sum_max_prod_calc_w_not_assessed"] = sum_max_calc_w_not_assessed
            rec["abs_min_sum_max_prod_calc_w_not_assessed_scenario"] = scenario_id

        if "abs_max_sum_max_prod_calc_w_not_assessed" not in rec or \
                sum_max_calc_w_not_assessed > rec["abs_max_sum_max_prod_calc_w_not_assessed"]:
            rec["abs_max_sum_max_prod_calc_w_not_assessed"] = sum_max_calc_w_not_assessed
            rec["abs_max_sum_max_prod_calc_w_not_assessed_scenario"] = scenario_id

        # scenario_summaries[scenario_id] = {
        #     "scenario_id": scenario_id,
        #     # Not assessed version
        #     "sum_min_contribution_prod_calc_w_not_assessed": sum_min_calc_w_not_assessed,
        #     "sum_max_contribution_prod_calc_w_not_assessed": sum_max_calc_w_not_assessed
        # }
    # summary per each CAS
    summary_df = (pd.DataFrame(summary.values()).sort_values("row_id").reset_index(drop=True))
    # CAS with no % in the prodcut
    final_invalid_material_info = pd.concat(all_invalid_material_info, ignore_index=True)
    final_invalid_material_info = final_invalid_material_info.drop_duplicates(keep='first')
    # best & worst case perecentage assessed:
    perecentage_assessed = pd.DataFrame([scenario_extremes["global"]])
    # find the worst case % assessed across the scenarios:

    # Find the smallest value in the numeric columns
    min_value = perecentage_assessed.iloc[:, ::2].min().min()  # Select numeric columns by slicing (even-indexed)
    # Find the column and the corresponding scenario for the smallest value
    # Get the column name for the smallest value
    min_column = perecentage_assessed.iloc[:, ::2].min().idxmin()
    # The scenario column
    min_scenario_column = min_column + '_scenario'
    # Retrieve the scenario
    min_scenario = perecentage_assessed[min_scenario_column].iloc[perecentage_assessed[min_column].idxmin()]
    # Create a dictionary to store the results
    abs_min_data_percentage_assessed = {}
    # Append the absolute min value and its scenario to a df:
    percent_assessed_df = pd.DataFrame({
        'Percentage_assessed': [min_value],
        'Scenario': [min_scenario]
    })
    perecentage_assessed_dict = {
        "Percentage Assessed": percent_assessed_df,
        "Invalid Material Info": final_invalid_material_info,
        "Storing calculations for percentage assessed": perecentage_assessed
    }
    return summary_df, perecentage_assessed_dict
# select scenarios (add that it prompts the user to choose which ones)
def select_scenarios(scenario_ids: list) -> list:
    print("Available Scenarios:")
    for i, scenario in enumerate(scenario_ids, 1):
        print(f"  {i}. {scenario}")

    print("Enter the numbers of the scenarios you want (e.g: 1,3,5) or 'all' to select all or X for no scenarios:")

    while True:
        user_input = input(" ").strip().lower()

        if user_input == "all":
            selected = scenario_ids[:]
            break

        if user_input == "x":
            selected = []
            break

        try:
            indices = [int(x.strip()) for x in user_input.split(",")]
            if all(1 <= i <= len(scenario_ids) for i in indices):
                selected = [scenario_ids[i - 1] for i in indices]
                break
            else:
                print(f"Please enter numbers between 1 and {len(scenario_ids)}")
        except ValueError:
            print("Invalid input. Use comma-separated numbers like: 1,3,5")
    print(f"Selected scenarios: {selected}")
    return selected
# save the unique CAS list:
def save_unique_values(df, column_name, output_file):
    """
    Takes a DataFrame and a column name, extracts unique values,
    and saves them to an Excel file.

    Parameters:
    df (pd.DataFrame): Input DataFrame
    column_name (str): Column to extract unique values from
    output_file (str): Output Excel file path (e.g., 'output.xlsx')
    """

    # Check if column exists
    if column_name not in df.columns:
        raise KeyError(f"Column '{column_name}' not found in DataFrame")

    # Get unique values
    unique_values = df[df[column_name] != "not assessed"][column_name].dropna().unique()

    # Convert to DataFrame
    unique_df = pd.DataFrame(unique_values, columns=[column_name])

    # Save to Excel
    unique_df.to_excel(output_file, index=False)

    print(f"Saved {len(unique_df)} unique values to '{output_file}'")
# calculate CAS numebrs unique:
def count_CAS_unique(df, column_name):
    # Check column exists
    if column_name not in df.columns:
        raise KeyError(f"Column '{column_name}' not found")

    # Filter out "not assessed"
    filtered_df = df[df[column_name] != "not assessed"]

    # Get unique CAS values
    unique_values = filtered_df[column_name].dropna().unique()

    # Return both count and list
    return len(unique_values), list(unique_values)
# Save the % assessed:
def save_percent_assessed(perecentage_assessed_dict, saving_percent_assessed):
    with pd.ExcelWriter(saving_percent_assessed, engine="xlsxwriter") as writer:
        # Write Percentage Assessed at the top
        perecentage_assessed_dict["Percentage Assessed"].to_excel(writer, sheet_name="percent_assessed", index=False,
                                                                  startrow=0)
        worksheet = writer.sheets["percent_assessed"]

        # If invalid_material_info is not empty, write it below the disclaimer
        if not perecentage_assessed_dict["Invalid Material Info"].empty:
            # Add Disclaimer
            red = writer.book.add_format({'color': 'red', 'bold': True})
            worksheet.write('A4',
                            "Disclaimer! For those CAS there is no info about their % in the products, so the % assessed is not accounting for them. Check the BOM:",
                            red)
            perecentage_assessed_dict["Invalid Material Info"].to_excel(writer, sheet_name="percent_assessed", index=False, startrow=4)

        perecentage_assessed_dict["Storing calculations for percentage assessed"].to_excel(writer, sheet_name="percent_assessed_calc_methods", index=False, startrow=0)
####################################################################################
# FUNCTIONS FOR C2C MIXTURE RULES #
### 1. Acute toxicity ###
## acute tox
def C2C_acute_toxicity(df_product, df_toxicity_info, ld_lc_to_assess):
    """
    Calculate acute toxicity ATE values and C2C acute toxicity ratings
    for each homogeneous material in a product.

    The function:
    1. Merges product composition data with toxicity data by CAS.
    2. Calculates the maximum homogeneous material concentration.
    3. Creates a percentage concentration column for ATE calculation.
    4. Calculates ATE values for selected LD50/LC50 endpoints.
    5. Stores chemicals with unknown ATE values together with the missing endpoint.
    6. Assigns acute toxicity ratings for oral, dermal and inhalation routes.
    7. Assigns one overall C2C acute toxicity rating per homogeneous material.

    Important concentration assumption:
    - min_contribution_hom_mat and max_contribution_hom_mat are fractions.
      Example: 0.02 means 2%.
    - ATE calculations require percentages.
      Therefore, conc_hom_mat_percent = conc_hom_mat * 100.

    Parameters
    ----------
    df_product : pd.DataFrame
        Product composition DataFrame. Must contain:
        - CAS
        - Homogenous Material
        - min_contribution_hom_mat
        - max_contribution_hom_mat

    df_toxicity_info : pd.DataFrame
        Toxicity information DataFrame. Must contain:
        - CAS
        - LD50_oral
        - LD50_dermal
        - LC50_gas
        - LC50_vapour
        - LC50_dust_mist_aerosol
        - CLP oral class
        - CLP dermal class
        - CLP inhalation class
        - oral toxicity C2C assessment
        - inhalative toxicity C2C assessment
        - dermal toxicity C2C assessment

    ld_lc_to_assess : list[str]
        List of LD50/LC50 endpoint columns to calculate ATE for.
        Allowed values:
        - "LD50_oral"
        - "LD50_dermal"
        - "LC50_gas"
        - "LC50_vapour"
        - "LC50_dust_mist_aerosol"

    Returns
    -------
    final_df : DataFrame with ATE values and C2C acute toxicity ratings per homogeneous material.

    unknown_chemicals_df : DataFrame with chemicals that have unknown ATE values but relevant classification, including the missing ATE endpoint.
    """

    # 1. Prepare the dataset
    df_calculation = pd.merge(df_product,df_toxicity_info,on="CAS",how="left")
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    # Worst-case concentration as fraction
    df_calculation["conc_hom_mat"] = (df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1))
    # Calculate the %
    df_calculation["conc_hom_mat_percent"] = (df_calculation["conc_hom_mat"] * 100)

    # 2. Save Endpoint-specific ATE settings
    ate_config = {
        "LD50_oral": {
            "route": "oral",
            "exclusion_value": 2000,
            "CLP_info": "CLP oral class",
            "tox_1": 0.5,
            "tox_2": 5,
            "tox_3": 100,
            "tox_4": 500,
            "ate_col": "ATE_based_on_LD50_oral",
        },
        "LD50_dermal": {
            "route": "dermal",
            "exclusion_value": 2000,
            "CLP_info": "CLP dermal class",
            "tox_1": 5,
            "tox_2": 50,
            "tox_3": 300,
            "tox_4": 1100,
            "ate_col": "ATE_based_on_LD50_dermal",
        },
        "LC50_gas": {
            "route": "inhalation gas",
            "exclusion_value": 20000,
            "CLP_info": "CLP inhalation class",
            "tox_1": 10,
            "tox_2": 100,
            "tox_3": 700,
            "tox_4": 4500,
            "ate_col": "ATE_based_on_LC50_gas",
        },
        "LC50_vapour": {
            "route": "inhalation vapour",
            "exclusion_value": 20,
            "CLP_info": "CLP inhalation class",
            "tox_1": 0.05,
            "tox_2": 0.5,
            "tox_3": 3,
            "tox_4": 5,
            "ate_col": "ATE_based_on_LC50_vapour",
        },
        "LC50_dust_mist_aerosol": {
            "route": "inhalation dust/mist/aerosol",
            "exclusion_value": 5,
            "CLP_info": "CLP inhalation class",
            "tox_1": 0.005,
            "tox_2": 0.05,
            "tox_3": 0.5,
            "tox_4": 1.5,
            "ate_col": "ATE_based_on_LC50_dust_mist_aerosol",
        },
    }
    # Output starts with one row per homogeneous material
    final_df = pd.DataFrame({"hom_material": hom_materials})
    # Store unknown ATE chemicals here as dicts
    all_unknown_chemicals = []

    # 3. Calculate ATE for each selected LD50/LC50 endpoint
    for ld_lc_col in ld_lc_to_assess:
        if ld_lc_col not in ate_config:
            print(f"Unknown ATE option skipped: {ld_lc_col}")
            continue
        # configure for each endpoint
        cfg = ate_config[ld_lc_col]
        clp_col = cfg["CLP_info"]

        df_ate = df_calculation.copy()

        # Force endpoint to numeric
        df_ate[ld_lc_col] = pd.to_numeric(df_ate[ld_lc_col],errors="coerce")

        # Exclude chemicals below 0.1%
        df_ate = df_ate.loc[df_ate["conc_hom_mat"] >= 0.001].copy()

        # Exclude chemicals below 1% if they are only category 4 /
        # above the route-specific exclusion value.
        # conc_hom_mat is fraction, so 0.01 = 1%
        df_ate = df_ate.loc[~((df_ate["conc_hom_mat"] < 0.01) & (df_ate[ld_lc_col] > cfg["exclusion_value"]))].copy()

        # Fill missing LD50/LC50 values based on CLP category
        df_ate.loc[df_ate[ld_lc_col].isna()& df_ate[clp_col].astype(str).str.contains("Tox. 1",na=False,regex=False),ld_lc_col] = cfg["tox_1"]
        df_ate.loc[df_ate[ld_lc_col].isna()& df_ate[clp_col].astype(str).str.contains("Tox. 2", na=False,regex=False), ld_lc_col] = cfg["tox_2"]
        df_ate.loc[ df_ate[ld_lc_col].isna() & df_ate[clp_col].astype(str).str.contains("Tox. 3", na=False,regex=False),ld_lc_col] = cfg["tox_3"]
        df_ate.loc[df_ate[ld_lc_col].isna()& df_ate[clp_col].astype(str).str.contains("Tox. 4", na=False, regex=False),ld_lc_col] = cfg["tox_4"]

        ate_rows = []

        # loop over each homogenous material
        for hom_material in hom_materials:
            df_hom = df_ate.loc[df_ate["Homogenous Material"] == hom_material].copy()

            # Unknown ATE chemicals above 10%
            condition_unknown = ((df_hom["conc_hom_mat_percent"] > 10) & df_hom[ld_lc_col].isna() & (df_hom[clp_col] != "Not classified"))

            # Save unknown chemicals with endpoint/route information
            unknown_cols = ["CAS","Homogenous Material","conc_hom_mat","conc_hom_mat_percent",clp_col]

            unknown_cols = [col for col in unknown_cols if col in df_hom.columns]

            unknown_df = df_hom.loc[condition_unknown,unknown_cols].copy()

            if not unknown_df.empty:
                unknown_df["missing_ATE_endpoint"] = ld_lc_col
                unknown_df["missing_ATE_route"] = cfg["route"]
                unknown_df["missing_ATE_output_col"] = cfg["ate_col"]

                all_unknown_chemicals.extend(unknown_df.to_dict("records"))

            sum_unknown_chemicals = df_hom.loc[condition_unknown,"conc_hom_mat_percent"].sum()

            # Adjust the 100 with the sum of unknown chemicals
            adjusted_100 = 100 - sum_unknown_chemicals

            # Calculate ATE
            df_hom["conc_divided_by_LD50"] = (df_hom["conc_hom_mat_percent"] / df_hom[ld_lc_col])

            sum_constituents = df_hom["conc_divided_by_LD50"].sum()

            if sum_constituents == 0 or pd.isna(sum_constituents):
                ate = np.nan
            else:
                ate = adjusted_100 / sum_constituents

            ate_rows.append({
                "hom_material": hom_material,
                cfg["ate_col"]: (
                    round(float(ate), 2)
                    if pd.notna(ate)
                    else np.nan
                ),
            })

        df_single_ate = pd.DataFrame(ate_rows)

        final_df = final_df.merge(df_single_ate, on="hom_material", how="left")


    # 4. GREY flags based on constituent assessments

    grey_rows = []

    for hom_material in hom_materials:
        df_hom = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material].copy()

        sum_oral_grey = df_hom.loc[df_hom["oral toxicity C2C assessment"] == "GREY","conc_hom_mat"].sum()

        sum_inhal_grey = df_hom.loc[df_hom["inhalative toxicity C2C assessment"] == "GREY","conc_hom_mat"].sum()

        sum_dermal_grey = df_hom.loc[df_hom["dermal toxicity C2C assessment"] == "GREY","conc_hom_mat"].sum()

        grey_rows.append({
            "hom_material": hom_material,
            "GREY_oral_tox": ("Yes" if sum_oral_grey >= 0.001 else "No"),
            "GREY_inhal_tox": ("Yes" if sum_inhal_grey >= 0.001 else "No"),
            "GREY_dermal_tox": ("Yes" if sum_dermal_grey >= 0.001 else "No")
        })

    grey_df = pd.DataFrame(grey_rows)

    final_df = final_df.merge(grey_df,on="hom_material",how="left")

    # 5. Classify ATE values
    # Oral
    if "ATE_based_on_LD50_oral" in final_df.columns:
        ate_col = "ATE_based_on_LD50_oral"
        out_col = "Acute toxicity oral C2C"

        final_df[out_col] = None
        final_df.loc[final_df[ate_col] <= 300, out_col] = "RED"
        final_df.loc[final_df[ate_col].between(300, 2000, inclusive="right"),out_col] = "YELLOW"
        final_df.loc[final_df[ate_col] > 2000, out_col] = "GREEN"

    # Dermal
    if "ATE_based_on_LD50_dermal" in final_df.columns:
        ate_col = "ATE_based_on_LD50_dermal"
        out_col = "Acute toxicity dermal C2C"

        final_df[out_col] = None
        final_df.loc[final_df[ate_col] <= 1000, out_col] = "RED"
        final_df.loc[ final_df[ate_col].between(1000, 2000, inclusive="right"),out_col] = "YELLOW"
        final_df.loc[final_df[ate_col] > 2000, out_col] = "GREEN"

    # Inhalation gases
    if "ATE_based_on_LC50_gas" in final_df.columns:
        ate_col = "ATE_based_on_LC50_gas"
        out_col = "Acute toxicity inhalation (gases) C2C"

        final_df[out_col] = None
        final_df.loc[final_df[ate_col] <= 2500, out_col] = "RED"
        final_df.loc[ final_df[ate_col].between(2500, 20000, inclusive="right"),out_col] = "YELLOW"
        final_df.loc[final_df[ate_col] > 20000, out_col] = "GREEN"

    # Inhalation vapour
    if "ATE_based_on_LC50_vapour" in final_df.columns:
        ate_col = "ATE_based_on_LC50_vapour"
        out_col = "Acute toxicity inhalation (vapour) C2C"

        final_df[out_col] = None
        final_df.loc[final_df[ate_col] <= 10, out_col] = "RED"
        final_df.loc[final_df[ate_col].between(10, 20, inclusive="right"),out_col] = "YELLOW"
        final_df.loc[final_df[ate_col] > 20, out_col] = "GREEN"

    # Inhalation dust/mist/aerosol
    if "ATE_based_on_LC50_dust_mist_aerosol" in final_df.columns:
        ate_col = "ATE_based_on_LC50_dust_mist_aerosol"
        out_col = "Acute toxicity inhalation (dust/mist) C2C"

        final_df[out_col] = None
        final_df.loc[final_df[ate_col] <= 1, out_col] = "RED"
        final_df.loc[ final_df[ate_col].between(1, 5, inclusive="right"),out_col] = "YELLOW"
        final_df.loc[final_df[ate_col] > 5, out_col] = "GREEN"

    # 6. Overall C2C acute toxicity rating
    classification_cols = [
        "Acute toxicity oral C2C",
        "Acute toxicity dermal C2C",
        "Acute toxicity inhalation (gases) C2C",
        "Acute toxicity inhalation (vapour) C2C",
        "Acute toxicity inhalation (dust/mist) C2C",
    ]

    for col in classification_cols:
        if col not in final_df.columns:
            final_df[col] = None

    final_df["C2C acute toxicity"] = None

    # RED first
    final_df.loc[final_df[classification_cols].eq("RED").any(axis=1), "C2C acute toxicity"] = "RED"

    # GREY second
    final_df.loc[final_df["C2C acute toxicity"].isna()& ((final_df["GREY_oral_tox"] == "Yes") | (final_df["GREY_inhal_tox"] == "Yes") | (final_df["GREY_dermal_tox"] == "Yes")), "C2C acute toxicity"] = "GREY"

    # YELLOW third
    final_df.loc[ final_df["C2C acute toxicity"].isna() & final_df[classification_cols].eq("YELLOW").any(axis=1),"C2C acute toxicity"] = "YELLOW"

    # GREEN fourth
    final_df.loc[final_df["C2C acute toxicity"].isna()& final_df[classification_cols].eq("GREEN").any(axis=1),"C2C acute toxicity"] = "GREEN"

    # 7. Build unknown chemicals DataFrame

    unknown_chemicals_df = pd.DataFrame(all_unknown_chemicals)

    if not unknown_chemicals_df.empty:
        unknown_chemicals_df = unknown_chemicals_df.drop_duplicates().sort_values(by="CAS").reset_index(drop=True)

    else:
        unknown_chemicals_df = pd.DataFrame(
            columns=[
                "CAS",
                "Homogenous Material",
                "conc_hom_mat",
                "conc_hom_mat_percent",
                "missing_ATE_endpoint",
                "missing_ATE_route",
                "missing_ATE_output_col",
            ]
        )

    return final_df, unknown_chemicals_df

### 2. Corrosion & Irritation ###
## Functions skin
def skin_corr_mixture_rule_c2c(df_product, df_toxicity_info):
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()

    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)

    def skin_irr_mixture_rating(df):
        conc_col = "conc_hom_mat"
        rating_col = "skin eye respiratory corrosion irritation C2C assessment"

        d = df.copy()

        d[conc_col] = pd.to_numeric(d[conc_col], errors="coerce").fillna(0)
        d[rating_col] = d[rating_col].astype(str).str.strip().str.upper()

        red_sum_ge_1pct = d.loc[
            (d[rating_col] == "RED") & (d[conc_col] >= 0.01),
            conc_col
        ].sum()

        red_sum_ge_0_1pct_lt_1pct = d.loc[
            (d[rating_col] == "RED") & (d[conc_col] >= 0.001) & (d[conc_col] < 0.01),
            conc_col
        ].sum()

        grey_sum_ge_0_1pct = d.loc[
            (d[rating_col] == "GREY") & (d[conc_col] >= 0.001),
            conc_col
        ].sum()

        yellow_sum_ge_1pct = d.loc[
            (d[rating_col] == "YELLOW") & (d[conc_col] >= 0.01),
            conc_col
        ].sum()

        yellow_weighted_sum = (10 * red_sum_ge_0_1pct_lt_1pct) + yellow_sum_ge_1pct

        if red_sum_ge_1pct >= 0.05:
            mixture_rating = "RED"

        elif red_sum_ge_1pct < 0.05 and (red_sum_ge_1pct + grey_sum_ge_0_1pct) >= 0.05:
            mixture_rating = "GREY"

        elif (0.01 <= red_sum_ge_1pct < 0.05) or (yellow_weighted_sum >= 0.01):
            mixture_rating = "YELLOW"

        else:
            mixture_rating = "GREEN"

        return mixture_rating

    skin_corr_for_each_material = []
    # assessment for each hom mat
    for hom_material in hom_materials:
        df_calc_hom_material = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        rating = skin_irr_mixture_rating(df_calc_hom_material)
        skin_corr_for_each_material.append({
            "hom_material": hom_material,
            f"skin_corr": rating})

    skin_results_df = pd.DataFrame(skin_corr_for_each_material)
    return skin_results_df
def eye_corr_mixture_rule_c2c(df_product, df_toxicity_info):
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()

    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(
        axis=1)

    def eye_irr_mixture_rating(df):
        conc_col = "conc_hom_mat"
        rating_col = "skin eye respiratory corrosion irritation C2C assessment"

        d = df.copy()

        d[conc_col] = pd.to_numeric(d[conc_col], errors="coerce").fillna(0)
        d[rating_col] = d[rating_col].astype(str).str.strip().str.upper()

        red_sum_ge_1pct = d.loc[
            (d[rating_col] == "RED") & (d[conc_col] >= 0.01),
            conc_col
        ].sum()

        red_sum_ge_0_1pct_lt_1pct = d.loc[
            (d[rating_col] == "RED") & (d[conc_col] >= 0.001) & (d[conc_col] < 0.01),
            conc_col
        ].sum()

        grey_sum_ge_0_1pct = d.loc[
            (d[rating_col] == "GREY") & (d[conc_col] >= 0.001),
            conc_col
        ].sum()

        yellow_sum_ge_1pct = d.loc[
            (d[rating_col] == "YELLOW") & (d[conc_col] >= 0.01),
            conc_col
        ].sum()

        yellow_weighted_sum = (10 * red_sum_ge_0_1pct_lt_1pct) + yellow_sum_ge_1pct

        if red_sum_ge_1pct >= 0.03:
            mixture_rating = "RED"

        elif red_sum_ge_1pct < 0.03 and (red_sum_ge_1pct + grey_sum_ge_0_1pct) >= 0.03:
            mixture_rating = "GREY"

        elif (0.01 <= red_sum_ge_1pct < 0.03) or (yellow_weighted_sum >= 0.10):
            mixture_rating = "YELLOW"

        else:
            mixture_rating = "GREEN"

        return mixture_rating

    eye_corr_for_each_material = []
    # assessment for each hom mat
    for hom_material in hom_materials:
        df_calc_hom_material = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        rating = eye_irr_mixture_rating(df_calc_hom_material)
        eye_corr_for_each_material.append({
            "hom_material": hom_material,
            f"eye_corr": rating})

    eye_results_df = pd.DataFrame(eye_corr_for_each_material)
    return eye_results_df
def resp_corr_rule_c2c(df_product, df_toxicity_info):
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    resp_corr_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        rating_col = "skin eye respiratory corrosion irritation C2C assessment"
        rank = {"RED": 0, "GREY": 1, "YELLOW": 2, "GREEN": 3}
        rating = min(df[rating_col], key=lambda x: rank[x])
        resp_corr_for_each_material.append({
            "hom_material": hom_material,
            f"resp_corr": rating})

    resp_results_df = pd.DataFrame(resp_corr_for_each_material)
    return resp_results_df
def corr_n_irr_mixture_rule_c2c(df_product, df_toxicity_info):
    skin_result = skin_corr_mixture_rule_c2c(df_product, df_toxicity_info)
    eye_result = eye_corr_mixture_rule_c2c(df_product, df_toxicity_info)
    resp_result = resp_corr_rule_c2c(df_product, df_toxicity_info)
    df_results = skin_result.merge(eye_result, on="hom_material", how="left").merge(resp_result, on="hom_material", how="left")
    df_results["C2C Skin, Eye, and Respiratory Irritation"] = None
    rank = {"RED": 0, "GREY": 1, "YELLOW": 2, "GREEN": 3}
    df_results["C2C Skin, Eye, and Respiratory Irritation"] = (
        df_results[["skin_corr", "eye_corr", "resp_corr"]]
        .apply(lambda row: min(row, key=lambda x: rank.get(x, float("inf"))), axis=1))
    return df_results

### 3. Skin and Respiratory Sensitization ###
def skin_and_resp_sens_c2c(df_product, df_toxicity_info):
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()

    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)

    # List of endpoints for sensitization:
    endpoints = ["Resp. Sens. 1A", "Resp. Sens. 1B", "Resp. Sens. 1",
                 "Skin Sens. 1", "Skin Sens. 1A", "Skin Sens. 1B"]
    # Step 1: Create SCL columns (lowest of Lower/Upper Limits)
    for ep in endpoints:
        lower_col = f"{ep} - Lower Limit: (%)"
        upper_col = f"{ep} - Upper Limit: (%)"

        # Check if at least one of the columns exists
        if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
            # Use min row-wise, ignoring missing columns
            df_calculation[f"SCL {ep}"] = df_calculation[[c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)

    # Step 2: Create check columns comparing concentration with SCL
    for ep in endpoints:
        scl_col = f"SCL {ep}"
        check_col = f"{scl_col} - check"

        if scl_col in df_calculation.columns:
            df_calculation[scl_col] = pd.to_numeric(df_calculation[scl_col],errors="coerce")
            df_calculation[check_col] = np.where(
                df_calculation[scl_col].isna(),
                None,  # SCL missing
                np.where(df_calculation["conc_hom_mat"] > df_calculation[scl_col], "Yes", "No")
            )

    # Step 3: assess per homogenous material
    sensitization_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        df["sensitization assessment"] = None
        # Loop over all check columns e.g. "SCL Skin Sens. 1 - check"
        for col in df.columns:
            # check SCL for each
            if col.endswith("- check"):
                #print(col)
                # For rows where check is "Yes" and assessment not set yet
                if col in ["SCL Resp. Sens. 1A - check", "SCL Resp. Sens. 1 - check", "SCL Skin Sens. 1 - check", "SCL Skin Sens. 1A - check"]:
                    df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "!!! Sens 1 or 1A present !!!"
                elif col in ["SCL Resp. Sens. 1B - check", "SCL Skin Sens. 1B - check"]:
                    df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "RED"
            # check general conc limits
            if col in ["skin_sensitisation", "resp_sensitisation"]:
                # for Sens. 1A and 1
                df.loc[((df[col] == "Skin Sens. 1: H317 May cause an allergic skin reaction")|(df[col].str.contains("Skin Sens. 1A", case=False, na=False))) & df["sensitization assessment"].isna(), "sensitization assessment"] = "!!! Sens 1 or 1A present !!!"
                # for Sens. 1B
                df.loc[((df[col].str.contains("Skin Sens. 1B", case=False, na=False)) & (df["conc_hom_mat"]>0.01)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "RED"
            # check for C2C single point assessment
            if col in ["sensitization C2C assessment"]:
                # assess the colour based on the C2C colour
                df.loc[(df[col] == "RED") & df["sensitization assessment"].isna(), "sensitization assessment"] = "RED"
                df.loc[(df[col] == "GREY") & df["sensitization assessment"].isna(), "sensitization assessment"] = "GREY"
                df.loc[(df[col] == "YELLOW") & df["sensitization assessment"].isna(), "sensitization assessment"] = "YELLOW"
                df.loc[(df[col] == "GREEN") & df["sensitization assessment"].isna(), "sensitization assessment"] = "GREEN"


        rating_col = "sensitization assessment"
        rank = { "!!! Sens 1 or 1A present !!!": 0 ,"RED": 1, "GREY": 2, "YELLOW": 3, "GREEN": 4}
        rating = min(df[rating_col], key=lambda x: rank[x])
        sensitization_for_each_material.append({
            "hom_material": hom_material,
            f"C2C Skin and Respiratory Sensitization": rating})

    return pd.DataFrame(sensitization_for_each_material)
def skin_sens_clp(df_product, df_toxicity_info):
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()

    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)

    # List of endpoints for sensitization:
    endpoints = ["Skin Sens. 1", "Skin Sens. 1A", "Skin Sens. 1B"]
    # Step 1: Create SCL columns (from the DB lowest of Lower/Upper Limits)
    for ep in endpoints:
        lower_col = f"{ep} - Lower Limit: (%)"
        upper_col = f"{ep} - Upper Limit: (%)"

        # Check if at least one of the columns exists
        if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
            # Use min row-wise, ignoring missing columns
            df_calculation[f"SCL {ep}"] = df_calculation[[c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)

    # Step 2: Create check columns comparing concentration in the mixture with SCL
    for ep in endpoints:
        scl_col = f"SCL {ep}"
        check_col = f"{scl_col} - check"

        if scl_col in df_calculation.columns:
            df_calculation[check_col] = np.where(
                df_calculation[scl_col].isna(),
                None,  # SCL missing
                np.where(df_calculation["conc_hom_mat"] > df_calculation[scl_col], "Yes", "No")
            )

    # Step 3: assess per homogenous material
    sensitization_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        df["sensitization assessment"] = None
        # Loop over all check columns e.g. "SCL Skin Sens. 1 - check"
        for col in df.columns:
            # check SCL for each
            if col.endswith("- check"):
                #print(col)
                # For rows where check is "Yes" and assessment not set yet
                if col in ["SCL Skin Sens. 1A - check"]:
                    df.loc[(df[col] == "Yes"), "sensitization assessment"] = "cat. 1A"
                elif col in ["SCL Skin Sens. 1B - check"]:
                    df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
                elif col in ["SCL Skin Sens. 1 - check"]:
                    df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"
            # check general conc limits
            if col in ["skin_sensitisation"]:
                # for Sens. 1A
                df.loc[
                    ((df[col].str.contains("Skin Sens. 1A", case=False, na=False)) & (df["conc_hom_mat"] >= 0.001)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1A"
                # for Sens. 1B
                df.loc[((df[col].str.contains("Skin Sens. 1B", case=False, na=False)) & (df["conc_hom_mat"]>=0.01)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
                # for Sens. 1
                df.loc[
                    ((df[col].str.contains("Skin Sens. 1: H317", case=False, na=False)) & (df["conc_hom_mat"] >= 0.01)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"

        rating_col = "sensitization assessment"
        rank = { "cat. 1A": 0 ,"cat. 1B": 1, "cat. 1": 2, None: 3}
        rating = min(df[rating_col], key=lambda x: rank[x])
        sensitization_for_each_material.append({
            "hom_material": hom_material,
            f"CLP Skin Sensitization": rating})

    return pd.DataFrame(sensitization_for_each_material)
def resp_sens_clp(df_product, df_toxicity_info, state = "solid/liquid" or "gas"):
    if state == "solid/liquid":
        lim_1a = 0.001
        lim_1b = 0.01
        lim_1 = 0.01
    elif state == "gas":
        lim_1a = 0.001
        lim_1b = 0.002
        lim_1 = 0.002

    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()

    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)

    # List of endpoints for sensitization:
    endpoints = ["Resp. Sens. 1A", "Resp. Sens. 1B", "Resp. Sens. 1"]
    # Step 1: Create SCL columns (from the DB lowest of Lower/Upper Limits)
    for ep in endpoints:
        lower_col = f"{ep} - Lower Limit: (%)"
        upper_col = f"{ep} - Upper Limit: (%)"

        # Check if at least one of the columns exists
        if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
            # Use min row-wise, ignoring missing columns
            df_calculation[f"SCL {ep}"] = df_calculation[[c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)

    # Step 2: Create check columns comparing concentration in the mixture with SCL
    for ep in endpoints:
        scl_col = f"SCL {ep}"
        check_col = f"{scl_col} - check"

        if scl_col in df_calculation.columns:
            df_calculation[check_col] = np.where(
                df_calculation[scl_col].isna(),
                None,  # SCL missing
                np.where(df_calculation["conc_hom_mat"] > df_calculation[scl_col], "Yes", "No")
            )

    # Step 3: assess per homogenous material
    sensitization_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        df["sensitization assessment"] = None
        # Loop over all check columns e.g. "SCL Skin Sens. 1 - check"
        for col in df.columns:
            # check SCL for each
            if col.endswith("- check"):
                #print(col)
                # For rows where check is "Yes" and assessment not set yet
                if col in ["SCL Resp. Sens. 1A - check"]:
                    df.loc[(df[col] == "Yes"), "sensitization assessment"] = "cat. 1A"
                elif col in ["SCL Resp. Sens. 1B - check"]:
                    df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
                elif col in ["SCL Resp. Sens. 1 - check"]:
                    df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"
            # check general conc limits
            if col in ["resp_sensitisation"]:
                # for Sens. 1A
                df.loc[
                    ((df[col].str.contains("Resp. Sens. 1A", case=False, na=False)) & (df["conc_hom_mat"] >= lim_1a)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1A"
                # for Sens. 1B
                df.loc[((df[col].str.contains("Resp. Sens. 1B", case=False, na=False)) & (df["conc_hom_mat"]>=lim_1b)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
                # for Sens. 1
                df.loc[
                    ((df[col].str.contains("Resp. Sens. 1: H317", case=False, na=False)) & (df["conc_hom_mat"] >= lim_1)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"

        rating_col = "sensitization assessment"
        rank = { "cat. 1A": 0 ,"cat. 1B": 1, "cat. 1": 2, None: 3}
        rating = min(df[rating_col], key=lambda x: rank[x])
        sensitization_for_each_material.append({
            "hom_material": hom_material,
            f"CLP Resp Sensitization": rating})

    return pd.DataFrame(sensitization_for_each_material)

### 4. Aquatic toxicity ###
## Acute aquatic tox
def acute_aquatic_c2c(df_product, df_toxicity_info, type = "fish" or "daph" or "algae"):

    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    lc_50 = f"lc_50_{type}"
    lc_50_exp = f"{type}_lc50"
    lc_50_qsar = f"{type}_lc50_qsar"
    hazard_class = "aquatic_tox_acute"
    m_factor = "m_factor"

    # data:
    df_calculation = df_calculation.copy()

    # take worst value from f"{type}_lc_50" and f"{type}_lc_50_qsar" take the worst value
    df_calculation[lc_50] = df_calculation[[lc_50_exp, lc_50_qsar]].min(axis=1)
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()

    # Function to determine hazard_classification
    def classify_hazard(row):
        lc50 = row[lc_50]
        hazard = row[hazard_class]

        if pd.isna(lc50) and pd.isna(hazard):
            return 'GREY', None
        elif lc50 > 100 or hazard == 'Not Classified':
            return 'GREEN', None
        elif 10 < lc50 <= 100 or hazard == 'Aqua. Acute 3: H402':
            return 'YELLOW', None
        elif 1 < lc50 <= 10 or hazard == 'Aqua. Acute 2: H401':
            return 'Acute 2', None
        elif 0.1 < lc50 <= 1 or hazard == 'Aqua. Acute 1: H400':
            return 'Acute 1', 1
        elif 0.01 < lc50 <= 0.1:
            return 'Acute 1', 10
        elif 0.001 < lc50 <= 0.01:
            return 'Acute 1', 100
        elif 0.0001 < lc50 <= 0.001:
            return 'Acute 1', 1000
        else:
            return 'Acute 1', 10000

    df_calculation[["designated_hazard_classification", "designated M factor"]] = df_calculation.apply(classify_hazard, axis=1).apply(pd.Series)

    df_calculation[m_factor] = df_calculation[m_factor].fillna(df_calculation["designated M factor"])

    hazard_col = "designated_hazard_classification"
    conc_col = "conc_hom_mat"
    m_col = m_factor

    results_for_each_material = []
    # assessment for each hom mat
    for hom_material in hom_materials:
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        # Compute the sums for each category based on concentration thresholds
        sum_acute1_x_m_factor = (df.loc[(df_calculation[hazard_col] == 'Acute 1') & (df[conc_col] >= 0.001), conc_col] *
                      df.loc[(df_calculation[hazard_col] == 'Acute 1') & (df[conc_col] >= 0.001), m_col]).sum()
        sum_acute2 = df.loc[(df_calculation[hazard_col] == 'Acute 2') & (df[conc_col] >= 0.01), conc_col].sum()
        sum_yellow = df.loc[(df_calculation[hazard_col] == 'YELLOW') & (df[conc_col] >= 0.01), conc_col].sum()
        sum_grey   = df.loc[(df_calculation[hazard_col] == 'GREY') & (df[conc_col] >= 0.001), conc_col].sum()


        # Assign hazard rating based on logic
        if (10 * sum_acute1_x_m_factor + sum_acute2) >= 0.25:
            mixture_hazard = 'RED'
        elif (10 * sum_acute1_x_m_factor + sum_acute2 + 10 * sum_grey) >= 0.25:
            mixture_hazard = 'GREY'
        elif (100 * sum_acute1_x_m_factor + 10 * sum_acute2 + sum_yellow) >= 0.25:
            mixture_hazard = 'YELLOW'
        else:
            mixture_hazard = 'GREEN'

        results_for_each_material.append({
            "hom_material": hom_material,
            f"{type} aquatic acute tox": mixture_hazard})
    return pd.DataFrame(results_for_each_material)
def final_acute_aquatic_c2c(df_product, df_toxicity_info):
    results_fish = acute_aquatic_c2c(df_product, df_toxicity_info, type = "fish")
    result_daph = acute_aquatic_c2c(df_product, df_toxicity_info, type = "daph")
    results_algae = acute_aquatic_c2c(df_product, df_toxicity_info, type = "algae")

    results_aqua_tox_acute = results_fish.merge(result_daph, on="hom_material", how="outer").merge(results_algae, on="hom_material", how="outer")
    priority = {'RED': 0, 'GREY': 1, 'YELLOW': 2, 'GREEN': 3}
    results_aqua_tox_acute['final assessment acute aquatic tox'] = results_aqua_tox_acute[['fish aquatic acute tox', 'daph aquatic acute tox', 'algae aquatic acute tox']].apply(lambda x: min(x, key=lambda y: priority[y]), axis=1)
    return results_aqua_tox_acute
## Chronic aquatic tox
def chronic_aquatic_c2c(df_product, df_toxicity_info, type = "fish" or "daph" or "algae"):

    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    noec = f"noec_{type}"
    noec_exp = f"{type}_noec"
    noec_qsar = f"{type}_noec_qsar"
    hazard_class = "aquatic_tox_chronic"
    m_factor = "m_factor"

    # data to numeric:
    df_calculation[[noec_exp, noec_qsar]] = df_calculation[[noec_exp, noec_qsar]].apply(pd.to_numeric, errors='coerce')
    # take worst value from f"{type}_noec" and f"{type}_noec" take the worst value
    df_calculation[noec] = df_calculation[[noec_exp, noec_qsar]].min(axis=1)
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()

    # Function to determine hazard_classification
    def classify_hazard(row):
        noec_value = row[noec]
        hazard = row[hazard_class]

        if pd.isna(noec_value) and pd.isna(hazard):
            return 'GREY', None
        elif noec_value > 10:
            return 'GREEN', None
        elif 1 < noec_value <= 10:
            return 'YELLOW', None
        elif hazard == 'Aqua. Chronic 4: H413':
            return 'Chronic 4', None
        elif hazard == 'Aqua. Chronic 3: H412':
            return 'Chronic 3', None
        elif 0.1 < noec_value <= 1 or hazard == 'Aqua. Chronic 2: H411':
            return 'Chronic 2', None
        elif 0.01 < noec_value <= 0.1 or hazard == 'Aqua. Chronic 1: H410':
            return 'Chronic 1', 1
        elif 0.001 < noec_value <= 0.01:
            return 'Chronic 1', 10
        elif 0.0001 < noec_value <= 0.001:
            return 'Chronic 1', 100
        elif 0.00001 < noec_value <= 0.0001:
            return 'Chronic 1', 1000
        elif 0.000001 < noec_value <= 0.00001:
            return 'Chronic 1', 10000
        else:
            return 'Chronic 1', 100000

    df_calculation[["designated_hazard_classification", "designated M factor"]] = df_calculation.apply(classify_hazard, axis=1).apply(pd.Series)

    df_calculation[m_factor] = df_calculation[m_factor].fillna(df_calculation["designated M factor"])

    hazard_col = "designated_hazard_classification"
    conc_col = "conc_hom_mat"
    m_col = m_factor

    results_for_each_material = []
    # assessment for each hom mat
    for hom_material in hom_materials:
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        # Compute the sums for each category based on concentration thresholds
        sum_chronic1_x_m_factor = (
                df.loc[(df_calculation[hazard_col] == 'Chronic 1') &(df[conc_col] >= 0.001),conc_col] *
                df.loc[(df_calculation[hazard_col] == 'Chronic 1') &(df[conc_col] >= 0.001), m_col]).sum()

        sum_chronic2 = df.loc[(df_calculation[hazard_col] == 'Chronic 2') &(df[conc_col] >= 0.01), conc_col].sum()

        sum_chronic3 = df.loc[(df_calculation[hazard_col] == 'Chronic 3') &(df[conc_col] >= 0.01),conc_col].sum()

        sum_chronic4 = df.loc[(df_calculation[hazard_col] == 'Chronic 4') &(df[conc_col] >= 0.01),conc_col].sum()

        sum_grey = df.loc[(df_calculation[hazard_col] == 'GREY')&(df[conc_col] >= 0.001),conc_col].sum()

        sum_yellow = df.loc[(df_calculation[hazard_col] == 'YELLOW') &(df[conc_col] >= 0.01),conc_col].sum()

        # compute scores
        red_score = (100 * sum_chronic1_x_m_factor
                + 10 * sum_chronic2
                + 10 * sum_chronic3
                + sum_chronic4)

        grey_score = (100 * sum_chronic1_x_m_factor
                + 10 * sum_chronic2
                + 10 * sum_chronic3
                + sum_chronic4
                + 100 * sum_grey)

        yellow_score = (1000 * sum_chronic1_x_m_factor
                + 100 * sum_chronic2
                + 100 * sum_chronic3
                + 10 * sum_chronic4
                + sum_yellow)


        # Assign hazard rating based on logic
        if red_score >= 0.25:
            mixture_hazard = 'RED'
        elif grey_score >= 0.25:
            mixture_hazard = 'GREY'
        elif yellow_score >= 0.25:
            mixture_hazard = 'YELLOW'
        else:
            mixture_hazard = 'GREEN'

        results_for_each_material.append({
            "hom_material": hom_material,
            f"{type} aquatic chronic tox": mixture_hazard})
    return pd.DataFrame(results_for_each_material)
def final_chronic_aquatic_c2c(df_product, df_toxicity_info):
    results_fish = chronic_aquatic_c2c(df_product, df_toxicity_info, type = "fish")
    result_daph = chronic_aquatic_c2c(df_product, df_toxicity_info, type = "daph")
    results_algae = chronic_aquatic_c2c(df_product, df_toxicity_info, type = "algae")

    results_aqua_tox_chronic = results_fish.merge(result_daph, on="hom_material", how="outer").merge(results_algae, on="hom_material", how="outer")
    priority = {'RED': 0, 'GREY': 1, 'YELLOW': 2, 'GREEN': 3}
    results_aqua_tox_chronic['final assessment chronic aquatic tox'] = results_aqua_tox_chronic[['fish aquatic chronic tox', 'daph aquatic chronic tox', 'algae aquatic chronic tox']].apply(lambda x: min(x, key=lambda y: priority[y]), axis=1)
    return results_aqua_tox_chronic
# final c2c aquatic assessment
def final_aquatic_c2c(df_product, df_toxicity_info):
    results_acute = final_acute_aquatic_c2c(df_product, df_toxicity_info)
    results_chronic = final_chronic_aquatic_c2c(df_product, df_toxicity_info)
    df = results_acute.merge(results_chronic, on="hom_material", how="outer")
    acute_col = "final assessment acute aquatic tox"
    chronic_col = "final assessment chronic aquatic tox"

    #
    df["C2C Acute and Chronic Aquatic Toxicity"] = None
    # If acute = green -> green
    df.loc[
        (df[acute_col] == "GREEN"),
        "C2C Acute and Chronic Aquatic Toxicity"
    ] = "GREEN"

    # If acute = RED -> RED
    df.loc[
        (df[acute_col] == "RED"),
        "C2C Acute and Chronic Aquatic Toxicity"
    ] = "RED"

    # If acute = GREY -> GREY
    df.loc[
        (df[acute_col] == "GREY"),
        "C2C Acute and Chronic Aquatic Toxicity"
    ] = "GREY"

    # If acute is YELLOW, final depends on chronic
    df.loc[
        (df[acute_col] == "YELLOW") & (df[chronic_col] == "RED"),
        "C2C Acute and Chronic Aquatic Toxicity"
    ] = "RED"

    df.loc[
        (df[acute_col] == "YELLOW") & (df[chronic_col] == "GREY"),
        "C2C Acute and Chronic Aquatic Toxicity"
    ] = "GREY"

    df.loc[
        (df[acute_col] == "YELLOW") & (df[chronic_col].isin(["YELLOW", "GREEN"])),
        "C2C Acute and Chronic Aquatic Toxicity"
    ] = "YELLOW"

    return df
### All C2C assessments at once ###
def mixture_rules_C2C_assessment(df_product, df_toxicity_info):

    def safe_run(func, name, fallback):
        """Helper: run function safely and never crash pipeline."""
        try:
            return func()
        except Exception as e:
            print(f"WARNING {name} failed: {e}")
            return fallback

    # ---- EXPECTED OUTPUT STRUCTURE (fallbacks) ----
    empty_acute = pd.DataFrame(columns=["hom_material", "C2C acute toxicity"])
    empty_corr = pd.DataFrame(columns=["hom_material", "C2C Skin, Eye, and Respiratory Irritation"])
    empty_sens = pd.DataFrame(columns=["hom_material", "C2C Skin and Respiratory Sensitization"])
    empty_aqua = pd.DataFrame(columns=["hom_material", "C2C Acute and Chronic Aquatic Toxicity"])
    empty_unknown = pd.DataFrame(columns=["hom_material"])

    # ---- ACUTE TOX ----
    all_ld_50_or_lc_50_options = [
        "LD50_oral",
        "LC50_gas",
        "LC50_vapour",
        "LC50_dust_mist_aerosol",
        "LD50_dermal"
    ]

    acute_tox_C2C_df, unknown_chemicals_df = safe_run(
        lambda: C2C_acute_toxicity(df_product, df_toxicity_info, all_ld_50_or_lc_50_options),
        "C2C_acute_toxicity",
        (empty_acute.copy(), empty_unknown.copy())
    )

    # ---- CORROSION & IRRITATION ----
    corr_n_irr_C2C_df = safe_run(
        lambda: corr_n_irr_mixture_rule_c2c(df_product, df_toxicity_info),
        "corr_n_irr_mixture_rule_c2c",
        empty_corr.copy()
    )

    # ---- SENSITIZATION ----
    sens_C2C_df = safe_run(
        lambda: skin_and_resp_sens_c2c(df_product, df_toxicity_info),
        "skin_and_resp_sens_c2c",
        empty_sens.copy()
    )

    # ---- AQUATIC ----
    final_aquatic_results = safe_run(
        lambda: final_aquatic_c2c(df_product, df_toxicity_info),
        "final_aquatic_c2c",
        empty_aqua.copy()
    )

    # ---- MERGE (also protected) ----
    try:
        final_c2c_results = (
            acute_tox_C2C_df
            .merge(corr_n_irr_C2C_df, on="hom_material", how="outer")
            .merge(sens_C2C_df, on="hom_material", how="outer")
            .merge(final_aquatic_results, on="hom_material", how="outer")
        )
    except Exception as e:
        print(f"[WARNING] Final merge failed: {e}")
        return empty_acute.copy()

    # Cleaning the summary
    try:
        final_c2c_results_summary = final_c2c_results[
            [
                "hom_material",
                "C2C acute toxicity",
                "C2C Skin, Eye, and Respiratory Irritation",
                "C2C Skin and Respiratory Sensitization",
                "C2C Acute and Chronic Aquatic Toxicity"
            ]
        ]
    except Exception as e:
        print(f"WARNING Column selection failed: {e}")
        final_c2c_results_summary = final_c2c_results[["hom_material"]]

    # Unknown chemicals
    try:
        print(unknown_chemicals_df)
    except Exception as e:
        print(f"WARNING Could not print unknown chemicals: {e}")

    return final_c2c_results_summary
#################################################################
def extract_info_from_DB(cas_list, db_path):

    def log_missing(cas, table, issue, log_list):
        log_list.append({
            "CAS": cas,
            "table": table,
            "issue": issue
        })

    # --------------------------
    # helper functions unchanged
    # --------------------------
    def clean_manual_rating(value):
        if pd.isna(value):
            return pd.NA
        text = str(value).strip().upper()
        match = re.search(r"\b(RED|GREY|YELLOW|GREEN)\b", text)
        return match.group(1) if match else pd.NA

    rating_rank = {"GREEN": 1, "YELLOW": 2, "GREY": 3, "RED": 4}

    def worst_rating(auto_value, manual_value):
        values = []
        if pd.notna(auto_value):
            values.append(str(auto_value).strip().upper())
        if pd.notna(manual_value):
            values.append(str(manual_value).strip().upper())

        values = [v for v in values if v in rating_rank]
        if not values:
            return pd.NA
        return max(values, key=lambda x: rating_rank[x])

    # --------------------------
    # connect DB (safe)
    # --------------------------
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
    except Exception as e:
        print(f"[ERROR] Cannot connect to DB: {e}")
        return None, pd.DataFrame([{"CAS": "ALL", "issue": "DB connection failed"}])

    results = []
    SCL_results = []
    assessment_dfs = []
    missing_cas_log = []

    # --------------------------
    # detect SCL columns safely
    # --------------------------
    try:
        table_name = "SCONCLIM"
        cursor.execute(f"PRAGMA table_info({table_name})")
        cols = [row[1] for row in cursor.fetchall()]
        selected_cols_SCL = [c for c in cols if c not in {"ID", "ref"}]
        selected_cols_SCL_sql = ", ".join([f'"{c}"' for c in selected_cols_SCL])
    except Exception as e:
        print(f"[WARNING] SCL schema issue: {e}")
        selected_cols_SCL_sql = '"ref"'
        selected_cols_SCL = ["ref"]

    # --------------------------
    # MAIN LOOP
    # --------------------------
    for cas in cas_list:

        # =========================
        # 1. SCL (safe)
        # =========================
        try:
            query = f'''
            SELECT {selected_cols_SCL_sql}
            FROM SCONCLIM
            WHERE ref = ?
            '''
            df_SCL = pd.read_sql_query(query, conn, params=(cas,))

            if df_SCL.empty:
                log_missing(cas, "SCONCLIM", "missing record", missing_cas_log)

            df_SCL.insert(0, "CAS", cas)
            SCL_results.append(df_SCL)

        except Exception as e:
            log_missing(cas, "SCONCLIM", str(e), missing_cas_log)

        # =========================
        # 2. ORAL TOX
        # =========================
        try:
            cursor.execute("""
                SELECT "Oral Acute: LD50 =",
                       "Oral toxicity Acute Tox classified"
                FROM ORALTOX WHERE ref = ?
            """, (cas,))
            oral_data = cursor.fetchone()

            if not oral_data:
                log_missing(cas, "ORALTOX", "missing record", missing_cas_log)
                oral_ld50 = oral_CLP_class = None
            else:
                oral_ld50, oral_CLP_class = oral_data

        except Exception as e:
            log_missing(cas, "ORALTOX", str(e), missing_cas_log)
            oral_ld50 = oral_CLP_class = None

        # =========================
        # 3. INHALATION TOX
        # =========================
        try:
            cursor.execute("""
                SELECT "Inhalative toxicity Acute: LC50 (gas) =",
                       "Inhalative toxicity Acute: LC50 (vapor) =",
                       "Inhalative toxicity Acute: LC50 (dust/mist/aerosol) =",
                       "Inhalative toxicity Acute Tox classification"
                FROM INHALTOX WHERE ref = ?
            """, (cas,))
            inhalation_data = cursor.fetchone()

            if not inhalation_data:
                log_missing(cas, "INHALTOX", "missing record", missing_cas_log)
                lc50_gas = lc50_vapour = lc50_dust_mist_aerosol = inhal_CLP_class = None
            else:
                lc50_gas, lc50_vapour, lc50_dust_mist_aerosol, inhal_CLP_class = inhalation_data

        except Exception as e:
            log_missing(cas, "INHALTOX", str(e), missing_cas_log)

        # =========================
        # 4. DERMAL TOX
        # =========================
        try:
            cursor.execute("""
                SELECT "Dermal Acute: LD50 =",
                       "Dermal toxicity Acute Tox classified"
                FROM DERMALTOX WHERE ref = ?
            """, (cas,))
            dermal_data = cursor.fetchone()

            if not dermal_data:
                log_missing(cas, "DERMALTOX", "missing record", missing_cas_log)
                dermal_ld50 = dermal_CLP_class = None
            else:
                dermal_ld50, dermal_CLP_class = dermal_data

        except Exception as e:
            log_missing(cas, "DERMALTOX", str(e), missing_cas_log)

        # =========================
        # 5. CORROSION / IRRITATION
        # =========================
        try:
            cursor.execute("""
                SELECT "Skin irritation classification",
                       "Eye irritation classification",
                       "Respiratory irritation classification"
                FROM IRRITCOR WHERE ref = ?
            """, (cas,))
            irritation_data = cursor.fetchone()

            if not irritation_data:
                log_missing(cas, "IRRITCOR", "missing record", missing_cas_log)
                skin_irr = eye_irr = reps_irr = None
            else:
                skin_irr, eye_irr, reps_irr = irritation_data

        except Exception as e:
            log_missing(cas, "IRRITCOR", str(e), missing_cas_log)

        # =========================
        # 6. SENSITISATION
        # =========================
        try:
            cursor.execute("""
                SELECT "Skin sensitization CLP classification",
                       "Respiratory sensitization CLP classification"
                FROM SENSITISATION WHERE ref = ?
            """, (cas,))
            sensitisation_data = cursor.fetchone()

            if not sensitisation_data:
                log_missing(cas, "SENSITISATION", "missing record", missing_cas_log)
                skin_sensitisation = resp_sensitisation = None
            else:
                skin_sensitisation, resp_sensitisation = sensitisation_data

        except Exception as e:
            log_missing(cas, "SENSITISATION", str(e), missing_cas_log)

        # =========================
        # 7. AQUATIC TOX (pattern same idea)
        # =========================
        try:
            cursor.execute("""
                SELECT "Aquatic toxicity Acute Tox classified",
                       "Aquatic toxicity Chronic Tox classified",
                       "M factor"
                FROM AQUATOX WHERE ref = ?
            """, (cas,))
            aquatic_tox_data = cursor.fetchone()

            if not aquatic_tox_data:
                log_missing(cas, "AQUATOX", "missing record", missing_cas_log)
                aquatic_tox_acute = aquatic_tox_chronic = m_factor = None
            else:
                aquatic_tox_acute, aquatic_tox_chronic, m_factor = aquatic_tox_data

        except Exception as e:
            log_missing(cas, "AQUATOX", str(e), missing_cas_log)

        # =========================
        # STORE RESULTS
        # =========================
        results.append({
            "CAS": cas,
            "LD50_oral": oral_ld50,
            "LD50_dermal": dermal_ld50,
            "CLP oral class": oral_CLP_class,
            "CLP dermal class": dermal_CLP_class,
            "CLP inhalation class": inhal_CLP_class,
            "skin_corr_irr": skin_irr,
            "eye_corr_irr": eye_irr,
            "reps_corr_irr": reps_irr,
            "skin_sensitisation": skin_sensitisation,
            "resp_sensitisation": resp_sensitisation,
            "aquatic_tox_acute": aquatic_tox_acute,
            "aquatic_tox_chronic": aquatic_tox_chronic,
            "m_factor": m_factor,
        })

    # --------------------------
    # FINAL ASSEMBLY
    # --------------------------
    df_info = pd.DataFrame(results)

    try:
        df_final_SCL = pd.concat(SCL_results, ignore_index=True)
    except Exception:
        df_final_SCL = pd.DataFrame()

    df = df_info.merge(df_final_SCL, on="CAS", how="outer")

    conn.close()

    # --------------------------
    # MISSING CAS OUTPUT
    # --------------------------
    df_missing = pd.DataFrame(missing_cas_log)

    return df, df_missing

#################################################################
### Calculating with mixture rules
def run_wint_C2C_mixture_rules():
    print("--------------------------------------------------------------")
    print("Select the Excel file (MAS) to analyse.")
    # open the program
    df, file_name, default_folder = open_excel_file()
    print("--------------------------------------------------------------")
    # Select folder for saving:
    print("Select a folder you want to save your files in.")
    saving = select_folder(default_folder)
    saving_dir = os.path.abspath(saving)
    print("--------------------------------------------------------------")
    db_path, db_name = open_sql_file()
    print("--------------------------------------------------------------")
    print("Initiating...")
    # calculate the maximum tier
    max_tier = get_highest_tier(df,col_CAS)
    print("Max Tier found: ", max_tier)
    # standardize & clean the df
    df = clean_data(df, max_tier)
    # add columns for analysis
    df = add_helper_columns(df, max_tier)
    df = add_final_map(df,max_tier)
    # how many CAS:
    CAS_count, cas_list = count_CAS_unique(df, "CAS")
    print("Total unique CAS found: ", CAS_count)
    print("--------------------------------------------------------------")
    toxicity_info_df, missing_cas_df = extract_info_from_DB(cas_list, db_path)
    if not missing_cas_df.empty:
        print("CAS missing from the DB", missing_cas_df)
        print("To continue choose an Excel with toxicity info:")
        toxicity_info_updated_with_excel_df = open_excel_file_toxicity()
        CAS_in_excel = toxicity_info_updated_with_excel_df["CAS"].dropna().unique()
        if Counter(CAS_in_excel) == Counter(cas_list):
            toxicity_info_df = toxicity_info_updated_with_excel_df
        else:
            print("The toxicity info provided is not sufficient for the whole anlaysis, probably some C2C endpoints will not be computed.")
    print("Proceeding with toxicity info.")
    print("--------------------------------------------------------------")
    # identify alternatives & make scenarios
    print("Generating scenarios...")
    df = identify_alternative_groups(df, max_tier)
    scenarios = generate_scenarios(df, max_tier)
    scenario_ids = [x['scenario_id'] for x in scenarios]
    print("Scenarios generated. Total number of scenarios: ", len(scenarios))
    print("Calculating... This might take a while...")
    # analyse the dataset: summary for each CAS & product assessed
    summary_df, perecentage_assessed_dict, C2C_mixture_results, all_c2c_scenario_results_df = analyse_the_dataset_with_mixture_rules(df, scenarios, toxicity_info_df)
    ### Saving:
    now = datetime.now()
    time = now.strftime("%Y%m%d")
    saving_summary = os.path.join(saving_dir, f"summary_{time}_{file_name}.xlsx")
    saving_percent_assessed = os.path.join(saving_dir, f"percent_assessed_{time}_{file_name}.xlsx")
    saving_selected = os.path.join(saving_dir, f"selected_scenarios_{time}_{file_name}.xlsx")
    saving_all_scenarios = os.path.join(saving_dir, f"all_scenarios_{time}_{file_name}.xlsx")
    saving_CAS = os.path.join(saving_dir, f"CAS_{time}_{file_name}.xlsx")
    C2C_mixture_rules_saving = os.path.join(saving_dir, f"Mixture_rules_{time}_{file_name}.xlsx")
    all_c2c_scenario_results_df.to_excel('/Users/juliakulpa/Desktop/c2c_mixture_rules_tests/Miture_rules_all.xlsx.xlsx')
    print("--------------------------------------------------------------")
    summary_df.to_excel(saving_summary, index=False)
    C2C_mixture_results.to_excel(C2C_mixture_rules_saving, index=False)
    print("Saved summary per each CAS to file: ", saving_summary)
    save_percent_assessed(perecentage_assessed_dict, saving_percent_assessed)
    print("Saved percentage assessed to file: ", saving_percent_assessed)
    print("--------------------------------------------------------------")
    print("Scanning for unique CAS...")
    save_unique_values(df, "CAS", saving_CAS)
    print("Saved unique values to file: ", saving_CAS)
    print("--------------------------------------------------------------")
    print("Do you want to save all scenarios? (y/n)")
    user_input = input("").strip().lower()
    if user_input == "y":
        print("Saving...")
        all_scenarios_df = build_selected_scenarios_df(df, scenarios, scenario_ids)
        all_scenarios_df.to_excel(saving_all_scenarios, index=False)
        print("Saved all scenarios to file: ", saving_all_scenarios)
    print("--------------------------------------------------------------")
    print("Do you want to save selected scenarios? (y/n)")
    user_input = input("").strip().lower()
    if user_input == "y":
        chosen = select_scenarios(scenario_ids)
        selected_df = build_selected_scenarios_df(df, scenarios, chosen)
        selected_df.to_excel(saving_selected, index=False)
        print("Saved the selected scenarios to file: ", saving_selected)
    print("--------------------------------------------------------------")
    print("Calculations finished. Have a nice day!")

def run_with_percentage_assessed():
    ### Start the program:
    print("--------------------------------------------------------------")
    print("Select the Excel file (MAS) to analyse.")
    # open the program
    df, file_name, default_folder = open_excel_file()
    print("--------------------------------------------------------------")
    # Select folder for saving:
    print("Select a folder you want to save your files in.")
    saving = select_folder(default_folder)
    saving_dir = os.path.abspath(saving)
    print("--------------------------------------------------------------")
    print("Calculating...")
    # calculate the maximum tier
    max_tier = get_highest_tier(df, col_CAS)
    print("Max Tier found: ", max_tier)
    # standardize & clean the df
    df = clean_data(df, max_tier)
    # add columns for analysis
    df = add_helper_columns(df, max_tier)
    df = add_final_map(df, max_tier)
    # how many CAS:
    CAS_count = count_CAS_unique(df, "CAS")
    print("Total unique CAS found: ", CAS_count)
    # identify alternatives & make scenarios
    print("Generating scenarios...")
    df = identify_alternative_groups(df, max_tier)
    scenarios = generate_scenarios(df, max_tier)
    scenario_ids = [x['scenario_id'] for x in scenarios]
    print("Scenarios generated. Total number of scenarios: ", len(scenarios))
    print("Calculating... This might take a while...")
    # analyse the dataset: summary for each CAS & product assessed
    summary_df, perecentage_assessed_dict = analyse_the_dataset(df, scenarios)
    ### Saving:
    now = datetime.now()
    time = now.strftime("%Y%m%d")
    saving_summary = os.path.join(saving_dir, f"summary_{time}_{file_name}.xlsx")
    saving_percent_assessed = os.path.join(saving_dir, f"percent_assessed_{time}_{file_name}.xlsx")
    saving_selected = os.path.join(saving_dir, f"selected_scenarios_{time}_{file_name}.xlsx")
    saving_all_scenarios = os.path.join(saving_dir, f"all_scenarios_{time}_{file_name}.xlsx")
    saving_CAS = os.path.join(saving_dir, f"CAS_{time}_{file_name}.xlsx")
    print("--------------------------------------------------------------")
    summary_df.to_excel(saving_summary, index=False)
    print("Saved summary per each CAS to file: ", saving_summary)
    save_percent_assessed(perecentage_assessed_dict, saving_percent_assessed)
    print("Saved percentage assessed to file: ", saving_percent_assessed)
    print("--------------------------------------------------------------")
    print("Scanning for unique CASs...")
    save_unique_values(df, "CAS", saving_CAS)
    print("Saved unique values to file: ", saving_CAS)
    print("--------------------------------------------------------------")
    print("Do you want to save all scenarios? (y/n)")
    user_input = input("").strip().lower()
    if user_input == "y":
        print("Saving...")
        all_scenarios_df = build_selected_scenarios_df(df, scenarios, scenario_ids)
        all_scenarios_df.to_excel(saving_all_scenarios, index=False)
        print("Saved all scenarios to file: ", saving_all_scenarios)
    print("--------------------------------------------------------------")
    print("Do you want to save selected scenarios? (y/n)")
    user_input = input("").strip().lower()
    if user_input == "y":
        chosen = select_scenarios(scenario_ids)
        selected_df = build_selected_scenarios_df(df, scenarios, chosen)
        selected_df.to_excel(saving_selected, index=False)
        print("Saved the selected scenarios to file: ", saving_selected)
    print("--------------------------------------------------------------")
    print("Calculations finished. Have a nice day!")


### Start the program:
# ---- Ask user ----
choice = ""
while choice not in ["A", "B"]:
    choice = input("Which calculation do you want to run? \n"
                   "A: just % assessed \n"
                   "B: % assessed and mixture rules \n"
                   "Type A or B").strip().upper()
    if choice not in ["A", "B"]:
        print("Please type A or B.")

# ---- Execute ----
if choice == "A":
    result = run_with_percentage_assessed()
else:
    result = run_wint_C2C_mixture_rules()

print(result)