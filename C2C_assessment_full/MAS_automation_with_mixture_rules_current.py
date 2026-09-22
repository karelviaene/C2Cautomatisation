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
import shutil
from datetime import datetime
from tqdm import tqdm
import sqlite3
from collections import Counter
import openpyxl
from openpyxl.formula.translate import Translator
from openpyxl.worksheet.formula import ArrayFormula
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils import get_column_letter
from openpyxl.styles import PatternFill, Font, Alignment

########################################################################
### C2C ASSESSMENT EXCEL TEMPLATE

C2C_ASSESSMENT_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "templates", "C2C_assessment_template.xlsx"
)
### The "overview"/"percentage_assessed"/"risk_assessed" sheets ship with
### only ONE formula row (row 2) in the template. save_c2c_assessment_workbook()
### generates however many extra formula rows this project needs (matched
### to the number of rows written to "detailed_overview", capped at
### C2C_ASSESSMENT_TEMPLATE_MAX_ROWS below), AND shrinks each formula's
### detailed_overview scan range (the template's row 2 hardcodes
### $2:$50000 / $2:$100000) down to just cover that many rows plus
### C2C_ASSESSMENT_SCAN_RANGE_BUFFER of headroom. Both matter for speed:
### a real-world 30,000-row project with the old fixed 50000/100000 scan
### range made Excel crash on open, so keep this cap conservative even
### though it is technically possible to go higher.
C2C_ASSESSMENT_TEMPLATE_MAX_ROWS = 5000
C2C_ASSESSMENT_SCAN_RANGE_BUFFER = 100
########################################################################

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
    # Defensive strip regardless of how clean the source "CAS Tier N" columns were - a
    # trailing space here (e.g. "100-00-0 ") would otherwise silently fail every DB lookup
    # keyed on this exact string (COLOUR_ASSESSMENT_C2C/SCONCLIM/toxicity table matching,
    # the missing-CAS check, etc.) despite looking identical to the eye.
    df["CAS"] = df["CAS"].astype(str).str.strip()
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

    # Running % after each tier is folded in, keyed by tier number, so the caller can show
    # the step-by-step trace (after % of hom mat -> after Tier 1 -> after Tier 2 -> ...) not
    # just the final value. Tier 1's own entry is the same number as min/max_val_hom_mat /
    # the starting min/max_val_prod above - included anyway so the trace is complete and
    # every tier is addressable the same way.
    tier_track = {1: (min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat)}

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
            tier_track[i] = (min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat)


    return min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat, tier_track
### Whether a row's OWN alternative-group picks (ignoring coupling) match this scenario
def _row_matches_alternative_choices(row, scenario, tier_level=10):
    for i in range(1, tier_level + 1):
        alt_group_col = f"t{i}_alt_group"
        material_col = col_mat.format(i=i)
        if pd.notna(row.get(alt_group_col)):
            chosen = scenario["choices"].get(row[alt_group_col])
            if chosen is not None and row.get(material_col) != chosen:
                return False
    return True
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
    # also treat every FIXED (non-alternative) Tier-i Material as "selected", so a coupling
    # rule pointed at a plain/base material (not itself an alternative choice) can be
    # satisfied - it previously never could be, since selected_materials only ever held
    # alternative-group choices. Restricted to rows that are themselves consistent with
    # this scenario's alternative choices (ignoring coupling) - otherwise a material that
    # only exists under a DIFFERENT, unchosen alternative branch could leak in and wrongly
    # satisfy a coupling check in a scenario where that branch was never picked.
    if not df.empty:
        alt_consistent_mask = df.apply(
            lambda r: _row_matches_alternative_choices(r, scenario, tier_level), axis=1
        )
        consistent_df = df[alt_consistent_mask]
        for i in range(1, tier_level + 1):
            alt_group_col = f"t{i}_alt_group"
            material_col = col_mat.format(i=i)
            if alt_group_col in consistent_df.columns and material_col in consistent_df.columns:
                fixed_mask = consistent_df[alt_group_col].isna()
                selected_materials |= set(consistent_df.loc[fixed_mask, material_col].dropna().unique())

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
### Derive a hom mat's weight-in-product from its Tier 1 materials' weights, when neither
### the hom mat's own weight-in-product NOR its %-in-product were given directly.
def calculate_hom_mat_weight_from_tier1(df):
    """Some MAS files only give the Tier 1 material weight (min_weight_in_hom_mat /
    max_weight_in_hom_mat) and never the hom mat's own weight-in-product or %-in-product.
    In that case, derive the hom mat's weight-in-product by summing the Tier 1 weights of
    its own ACTIVE rows for this scenario - so an alternative branch that was NOT chosen in
    this scenario never contributes its weight. Must run after evaluate_row_activity (needs
    "active") and before calculate_material_percentages_product, whose existing mass-based
    %-of-product fallback then picks up the derived weight automatically.
    Min and max are derived independently: if even one active row's Tier 1 weight is missing
    for a given side (min or max), that side is left as NaN rather than silently summing only
    the rows that DO have a value - understating a hom mat's true weight would be an unsafe,
    non-conservative % of hom mat in product."""
    df = df.copy()
    needs_derivation = (
        df[min_weight_in_product].isna()
        & df[max_weight_in_product].isna()
        & df[min_percent_in_product].isna()
        & df[max_percent_in_product].isna()
    )
    only_active = df["active"] == True
    candidates = df.loc[only_active & needs_derivation]
    if candidates.empty:
        return df

    id_keys = [product, hom_mat]

    def _sum_or_nan(s):
        return np.nan if s.isna().any() or s.empty else s.sum()

    grouped = candidates.groupby(id_keys)
    min_sums = grouped[min_weight_in_hom_mat].apply(_sum_or_nan)
    max_sums = grouped[max_weight_in_hom_mat].apply(_sum_or_nan)

    df["key"] = list(zip(*(df[k] for k in id_keys)))
    fill_mask = needs_derivation & only_active
    df.loc[fill_mask, min_weight_in_product] = df.loc[fill_mask, "key"].map(min_sums)
    df.loc[fill_mask, max_weight_in_product] = df.loc[fill_mask, "key"].map(max_sums)
    df.drop(columns=["key"], inplace=True)
    return df
### Calculate the % contribution per product
def calculate_material_percentages_product(df):
    df = df.copy()
    df_mass_calc = df.copy()
    # Identity of "one homogeneous material in this product" is (product, hom_mat) alone.
    # A hom mat's own weight-in-product is a HOM-MAT-LEVEL attribute that is repeated across
    # every one of its child rows (one row per Tier 1 ingredient/CAS underneath it, or per
    # active alternative). Deduplicating on a key that ALSO includes the weight columns
    # (as before) breaks whenever those child rows don't carry an identical weight value -
    # e.g. a real MAS row filled with a stale/mismatched number, or an alternative swap that
    # legitimately changes the hom mat's declared total - two rows of the SAME hom mat then
    # look like two DIFFERENT hom mats to drop_duplicates(), and its weight gets summed once
    # per distinct value instead of once per hom mat, inflating the product-level total and
    # skewing every %-of-product this fallback produces for that product.
    id_keys = [product, hom_mat]
    keys = id_keys + [min_weight_in_product, max_weight_in_product]
    only_active = df_mass_calc["active"] == True
    df_mass_calc_unique = df.loc[only_active, keys].groupby(id_keys, as_index=False).first()

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
    # Map the computed percentage back onto every row of that (product, hom_mat) BY IDENTITY
    # only - not by each row's own (possibly inconsistent) weight value - so every child row
    # of the same hom mat gets the SAME %-of-product.
    df_mass_calc_unique["key"] = list(zip(*(df_mass_calc_unique[k] for k in id_keys)))
    df["key"] = list(zip(*(df[k] for k in id_keys)))
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
    # Same fix as calculate_material_percentages_product, one tier down: identity of "one
    # Tier 1 material within this hom mat" is (hom_mat, Tier 1 Material) alone - its own
    # weight is a Tier-1-level attribute that must not be re-treated as part of the identity
    # (see the comment in calculate_material_percentages_product for why that double-counts).
    id_keys = [hom_mat, col_mat_tier_1]
    keys = id_keys + [min_weight_in_hom_mat, max_weight_in_hom_mat]
    only_active = df_mass_calc["active"] == True
    df_mass_calc_unique = df.loc[only_active, keys].groupby(id_keys, as_index=False).first()

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

    df_mass_calc_unique["key"] = list(zip(*(df_mass_calc_unique[k] for k in id_keys)))
    df["key"] = list(zip(*(df[k] for k in id_keys)))

    min_map = df_mass_calc_unique.set_index("key")[min_percent_in_hom_mat]
    max_map = df_mass_calc_unique.set_index("key")[max_percent_in_hom_mat]

    df[min_percent_in_hom_mat] = df[min_percent_in_hom_mat].fillna(df["key"].map(min_map))
    df[max_percent_in_hom_mat] = df[max_percent_in_hom_mat].fillna(df["key"].map(max_map))
    df.drop(["key"], axis=1, inplace=True)
    return df
### calculating the % in product and hom mat
def calculate_row_contributions(df, tier_level=None):
    df = df.copy()
    # Auto-detect this dataset's own deepest tier (instead of a fixed hardcoded depth) so the
    # per-tier tracking columns added below only go as far as tiers actually present in the file.
    if tier_level is None:
        tier_level = get_highest_tier(df, col_mat)

    min_val_prod_contibutions = []
    max_val_prod_contibutions = []
    min_val_hom_mat_contibutions = []
    max_val_hom_mat_contibutions = []
    tier_tracks = []
    for _, row in df.iterrows():
        if row.get("active") is True:
            min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat, tier_track = calc_row_contribution(row, tier_level=tier_level)
        else:
            min_val_prod, max_val_prod, min_val_hom_mat, max_val_hom_mat, tier_track = np.nan, np.nan, np.nan, np.nan, {}

        min_val_prod_contibutions.append(min_val_prod)
        max_val_prod_contibutions.append(max_val_prod)
        min_val_hom_mat_contibutions.append(min_val_hom_mat)
        max_val_hom_mat_contibutions.append(max_val_hom_mat)
        tier_tracks.append(tier_track)


    df["min_contribution_prod"] = min_val_prod_contibutions
    df["max_contribution_prod"] = max_val_prod_contibutions
    df["min_contribution_hom_mat"] = min_val_hom_mat_contibutions
    df["max_contribution_hom_mat"] = max_val_hom_mat_contibutions

    # Per-tier running % trace ("after % of hom mat, then Tier 1, then Tier 2, ..."), so the
    # cumulative calculation is auditable step by step, not just visible as the final result.
    # A tier column stays NaN for rows that don't go that deep (or aren't active).
    _missing = (np.nan, np.nan, np.nan, np.nan)
    for i in range(1, tier_level + 1):
        df[f"min_contribution_prod_t{i}"] = [t.get(i, _missing)[0] for t in tier_tracks]
        df[f"max_contribution_prod_t{i}"] = [t.get(i, _missing)[1] for t in tier_tracks]
        df[f"min_contribution_hom_mat_t{i}"] = [t.get(i, _missing)[2] for t in tier_tracks]
        df[f"max_contribution_hom_mat_t{i}"] = [t.get(i, _missing)[3] for t in tier_tracks]

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
        scenario_df = calculate_hom_mat_weight_from_tier1(scenario_df)
        product_percent_df = calculate_material_percentages_product(scenario_df)
        hom_mat_percent_df = calculate_material_percentages_hom_mat(product_percent_df)
        scenario_evaluated = calculate_row_contributions(hom_mat_percent_df).copy()
        results.append(scenario_evaluated)

    if results:
        return pd.concat(results, ignore_index=True)

    return pd.DataFrame()
def analyse_the_dataset_with_mixture_rules(df, scenarios, db_path):
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
        # ranked highest so it is never silently dropped from (or lost a tie-break in) the
        # worst-case-across-scenarios aggregation below - "we don't have enough data to say"
        # must surface at least as prominently as any colour that WAS computed.
        NOT_FULL_COMPOSITION_LABEL.upper(): 6,
        NOT_ENOUGH_INFO_LABEL.upper(): 7,
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
        scenario_df = calculate_hom_mat_weight_from_tier1(scenario_df)
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

        # Round away machine-epsilon-scale floating point residue (e.g. 2.22e-16 instead
        # of an exact 0 when a product's own contributions sum to ~1.0 by construction) -
        # see the identical fix in MAS_quick_C2C_assessment_static.py's
        # _pct_assessed_by_group for the full rationale. A genuine >100% composition data
        # issue still shows up as a real, well-above-epsilon negative value.
        sum_min_calc_w_not_assessed = round(1 - sum_max_not_assessed, 10)
        sum_max_calc_w_not_assessed = round(1 - sum_min_not_assessed, 10)

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

            c2c_summary_df = mixture_rules_C2C_assessment_from_db(
                active_product_df,
                db_path
            )

            # Add scenario ID for traceability
            c2c_summary_df["scenario_id"] = scenario_id
            all_c2c_scenario_results.append(c2c_summary_df)

            # These are the output columns from mixture_rules_C2C_assessment_from_db
            c2c_endpoint_cols = [
                "C2C acute toxicity",
                "C2C Skin, Eye, and Respiratory Irritation",
                "C2C Skin and Respiratory Sensitization",
                "C2C Acute and Chronic Aquatic Toxicity",
            ] + [f"C2C {label}" for label in ASSESSMENT_C_ENDPOINTS.values()]

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
        scenario_df = calculate_hom_mat_weight_from_tier1(scenario_df)
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

        # Round away machine-epsilon-scale floating point residue (e.g. 2.22e-16 instead
        # of an exact 0 when a product's own contributions sum to ~1.0 by construction) -
        # see the identical fix in MAS_quick_C2C_assessment_static.py's
        # _pct_assessed_by_group for the full rationale. A genuine >100% composition data
        # issue still shows up as a real, well-above-epsilon negative value.
        sum_min_calc_w_not_assessed = round(1 - sum_max_not_assessed, 10)
        sum_max_calc_w_not_assessed = round(1 - sum_min_not_assessed, 10)

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

### Shown instead of a RED/YELLOW/GREEN/GREY rating whenever a homogeneous material's
### mixture-rule result can't actually be trusted: an active ingredient's composition
### (%) is unknown, its CAS/identity is unknown ("not assessed"), or its CAS is known
### but the specific hazard data that rule needs (LD50/LC50/CLP class, corrosion/
### irritation rating, sensitization data, aquatic LC50/NOEC/hazard class) is missing.
### Previously such gaps were either silently dropped from the calculation (understating
### the mixture's real hazard) or, in the aquatic-toxicity case, silently defaulted to
### the WORST possible rating - both are wrong; this makes the gap visible instead.
NOT_FULL_COMPOSITION_LABEL = "Not full comp - no mixture rules applied"


def _hom_materials_with_unknown_composition(df_product):
    """Homogeneous materials with at least one active row of unknown % (NaN) or unknown CAS ('not assessed')."""
    d = df_product.copy()
    d["conc_hom_mat"] = d[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    unknown_mask = d["conc_hom_mat"].isna() | (d["CAS"] == "not assessed")
    return set(d.loc[unknown_mask, "Homogenous Material"].unique())


def _apply_not_full_composition_label(df, incomplete_hom_materials, cols):
    """Overwrite `cols` with NOT_FULL_COMPOSITION_LABEL for every row whose hom_material is incomplete."""
    if not incomplete_hom_materials or df.empty:
        return df
    mask = df["hom_material"].isin(incomplete_hom_materials)
    existing_cols = [c for c in cols if c in df.columns]
    # these columns may currently be numeric (e.g. an ATE value) - cast to object first so
    # assigning the text label doesn't trip pandas' incompatible-dtype warning/future error
    for c in existing_cols:
        if df[c].dtype != object:
            df[c] = df[c].astype(object)
    df.loc[mask, existing_cols] = NOT_FULL_COMPOSITION_LABEL
    return df


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
            "CLP_info": "CLP oral class",
            "tox_1": 0.5,
            "tox_2": 5,
            "tox_3": 100,
            "tox_4": 500,
            "ate_col": "ATE_based_on_LD50_oral",
        },
        "LD50_dermal": {
            "route": "dermal",
            "CLP_info": "CLP dermal class",
            "tox_1": 5,
            "tox_2": 50,
            "tox_3": 300,
            "tox_4": 1100,
            "ate_col": "ATE_based_on_LD50_dermal",
        },
        "LC50_gas": {
            "route": "inhalation gas",
            "CLP_info": "CLP inhalation class",
            "tox_1": 10,
            "tox_2": 100,
            "tox_3": 700,
            "tox_4": 4500,
            "ate_col": "ATE_based_on_LC50_gas",
        },
        "LC50_vapour": {
            "route": "inhalation vapour",
            "CLP_info": "CLP inhalation class",
            "tox_1": 0.05,
            "tox_2": 0.5,
            "tox_3": 3,
            "tox_4": 5,
            "ate_col": "ATE_based_on_LC50_vapour",
        },
        "LC50_dust_mist_aerosol": {
            "route": "inhalation dust/mist/aerosol",
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
    # Homogeneous materials whose acute-toxicity rating can't be trusted: unknown
    # composition/CAS, or a known-CAS ingredient (at or above the standard 0.1% CLP
    # de-minimis threshold - the SAME cutoff the ATE math itself uses below, so nothing
    # exempt from classification consideration gets flagged) missing all the hazard data
    # relevant to it. Oral and dermal are each required independently, since those are
    # normally reported for every substance; the three inhalation columns (gas/vapour/
    # dust-mist-aerosol) are ALTERNATE representations of the same exposure route
    # depending on the substance's physical form - a real substance is only ever tested
    # under ONE of them, so a material is only flagged for "inhalation" if NONE of the
    # requested inhalation endpoints have data, not if any single one of the three is
    # missing (checking all 5 independently, as an earlier version of this fix did,
    # flagged almost every real ingredient, since virtually none have all 3 populated).
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)

    known_row_mask = (
        (df_calculation["CAS"] != "not assessed")
        & df_calculation["conc_hom_mat"].notna()
        & (df_calculation["conc_hom_mat"] >= 0.001)
    )
    route_groups = {}
    for _ld_lc_col in ld_lc_to_assess:
        if _ld_lc_col not in ate_config:
            continue
        _cfg = ate_config[_ld_lc_col]
        _clp_col = _cfg["CLP_info"]
        _filled_col = f"_filled_{_ld_lc_col}"
        df_calculation[_filled_col] = pd.to_numeric(df_calculation[_ld_lc_col], errors="coerce")
        df_calculation.loc[df_calculation[_filled_col].isna() & df_calculation[_clp_col].astype(str).str.contains("Tox. 1", na=False, regex=False), _filled_col] = _cfg["tox_1"]
        df_calculation.loc[df_calculation[_filled_col].isna() & df_calculation[_clp_col].astype(str).str.contains("Tox. 2", na=False, regex=False), _filled_col] = _cfg["tox_2"]
        df_calculation.loc[df_calculation[_filled_col].isna() & df_calculation[_clp_col].astype(str).str.contains("Tox. 3", na=False, regex=False), _filled_col] = _cfg["tox_3"]
        df_calculation.loc[df_calculation[_filled_col].isna() & df_calculation[_clp_col].astype(str).str.contains("Tox. 4", na=False, regex=False), _filled_col] = _cfg["tox_4"]
        _route = _cfg["route"]
        _group_key = "inhalation" if _route.startswith("inhalation") else _route
        route_groups.setdefault(_group_key, []).append(_filled_col)

    for _group_key, _filled_cols in route_groups.items():
        group_missing = df_calculation[_filled_cols].isna().all(axis=1) & known_row_mask
        if group_missing.any():
            incomplete_hom_materials |= set(df_calculation.loc[group_missing, "Homogenous Material"].unique())

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

        # Exclude chemicals below 1% if they are CLP Category 4 (C2C YELLOW) rated - Table 11
        # footnote 13: RED-rated (Cat 1-3) and GREY-rated chemicals count toward the mixture
        # rating at >=0.1% (already applied above), but Category 4/YELLOW-rated chemicals
        # only count at >=1%.
        # conc_hom_mat is fraction, so 0.01 = 1%
        is_category_4 = df_ate[clp_col].astype(str).str.contains("Tox. 4", na=False, regex=False)
        df_ate = df_ate.loc[~(is_category_4 & (df_ate["conc_hom_mat"] < 0.01))].copy()

        # Fill missing LD50/LC50 values based on CLP category
        df_ate.loc[df_ate[ld_lc_col].isna()& df_ate[clp_col].astype(str).str.contains("Tox. 1",na=False,regex=False),ld_lc_col] = cfg["tox_1"]
        df_ate.loc[df_ate[ld_lc_col].isna()& df_ate[clp_col].astype(str).str.contains("Tox. 2", na=False,regex=False), ld_lc_col] = cfg["tox_2"]
        df_ate.loc[ df_ate[ld_lc_col].isna() & df_ate[clp_col].astype(str).str.contains("Tox. 3", na=False,regex=False),ld_lc_col] = cfg["tox_3"]
        df_ate.loc[df_ate[ld_lc_col].isna()& df_ate[clp_col].astype(str).str.contains("Tox. 4", na=False, regex=False),ld_lc_col] = cfg["tox_4"]

        ate_rows = []

        # loop over each homogenous material
        for hom_material in hom_materials:
            df_hom = df_ate.loc[df_ate["Homogenous Material"] == hom_material].copy()

            # Chemicals with genuinely unknown acute toxicity (no usable LD50/LC50 value even
            # after the CLP-category fill above, and not "Not classified"). Per CLP section
            # 2.3.1, when the TOTAL concentration of such unknown-toxicity chemicals exceeds
            # 10%, the "100" in the ATE formula is corrected down to 100 minus that total.
            # Previously this only counted a chemical toward the correction if IT ALONE
            # exceeded 10%, so several smaller unknown-toxicity chemicals that together
            # exceeded 10% received no correction at all.
            condition_unknown = (df_hom[ld_lc_col].isna() & (df_hom[clp_col] != "Not classified"))

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

            # Only correct the "100" once the unknown chemicals' TOTAL concentration is
            # >10% (per CLP); below that they are simply excluded from the Ci/ATEi sum
            # (via NaN, already skipped by .sum()) without adjusting the numerator.
            adjusted_100 = (100 - sum_unknown_chemicals) if sum_unknown_chemicals > 10 else 100

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

    # Inhalation gases (same C2C Table 11 mg/L cutoffs as vapour - not the raw CLP
    # category-boundary ppmV values 2500/20000 used here previously)
    if "ATE_based_on_LC50_gas" in final_df.columns:
        ate_col = "ATE_based_on_LC50_gas"
        out_col = "Acute toxicity inhalation (gases) C2C"

        final_df[out_col] = None
        final_df.loc[final_df[ate_col] <= 10, out_col] = "RED"
        final_df.loc[ final_df[ate_col].between(10, 20, inclusive="right"),out_col] = "YELLOW"
        final_df.loc[final_df[ate_col] > 20, out_col] = "GREEN"

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

    # 6b. Any homogeneous material with unknown composition/CAS or missing hazard data for
    # a requested route can't get a trustworthy rating - replace whatever was computed
    # (including a possibly-wrong RED/YELLOW/GREEN/GREY) with an explicit label instead.
    ate_output_cols = [cfg["ate_col"] for cfg in ate_config.values()]
    final_df = _apply_not_full_composition_label(
        final_df, incomplete_hom_materials, classification_cols + ["C2C acute toxicity"] + ate_output_cols
    )

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
        # .get(..., worst_rank) instead of raw rank[x]: a missing/unexpected rating (e.g. NaN
        # from a CAS not found in the toxicity DB) must never raise and take down every other
        # homogeneous material's result in this same call - the completeness check in
        # corr_n_irr_mixture_rule_c2c is what actually decides whether to trust this value.
        worst_rank = max(rank.values()) + 1
        rating = min(df[rating_col], key=lambda x: rank.get(x, worst_rank))
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

    # Any homogeneous material with unknown composition/CAS, or a known-CAS ingredient
    # missing the corrosion/irritation rating altogether, can't get a trustworthy result.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    rating_col = "skin eye respiratory corrosion irritation C2C assessment"
    df_calc = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    df_calc["conc_hom_mat"] = df_calc[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    missing_rating_mask = (
        df_calc[rating_col].isna() & (df_calc["CAS"] != "not assessed") & df_calc["conc_hom_mat"].notna()
    )
    incomplete_hom_materials |= set(df_calc.loc[missing_rating_mask, "Homogenous Material"].unique())

    df_results = _apply_not_full_composition_label(
        df_results, incomplete_hom_materials,
        ["skin_corr", "eye_corr", "resp_corr", "C2C Skin, Eye, and Respiratory Irritation"],
    )
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
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material].copy()
        df["sensitization assessment"] = None

        # Pass 1: SCL-based checks first. Per the methodology, an SCL can be LOWER or
        # HIGHER than the generic %-threshold and always takes precedence when defined -
        # so any endpoint/chemical with SCL data must be judged ONLY by that comparison;
        # the generic-threshold checks in Pass 2 skip it entirely (has_scl_* below),
        # regardless of whether the SCL check itself came out "Yes" or "No".
        scl_1_1a_cols = [c for c in [
            "SCL Resp. Sens. 1A - check", "SCL Resp. Sens. 1 - check",
            "SCL Skin Sens. 1 - check", "SCL Skin Sens. 1A - check",
        ] if c in df.columns]
        scl_1b_cols = [c for c in [
            "SCL Resp. Sens. 1B - check", "SCL Skin Sens. 1B - check",
        ] if c in df.columns]
        has_scl_1_1a = df[scl_1_1a_cols].notna().any(axis=1) if scl_1_1a_cols else pd.Series(False, index=df.index)
        has_scl_1b = df[scl_1b_cols].notna().any(axis=1) if scl_1b_cols else pd.Series(False, index=df.index)

        for col in scl_1_1a_cols:
            df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "!!! Sens 1 or 1A present !!!"
        for col in scl_1b_cols:
            df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "RED"

        # Pass 2: generic concentration-threshold checks - CLP Table 6/7: Cat 1/1A >= 0.1%,
        # Cat 1B >= 1.0% (C2C section 3.2.3 uses a flat 1.0% for Resp. Sens. 1B too - no
        # gas/solid-liquid split for the mixture-rule process). Skin and respiratory each
        # read their OWN column/text (previously respiratory reused the skin literals and
        # so could never match), and only apply when no SCL is defined for that endpoint.
        if "skin_sensitisation" in df.columns:
            col = "skin_sensitisation"
            df.loc[
                (
                    ((df[col] == "Skin Sens. 1: H317 May cause an allergic skin reaction")
                     | df[col].str.contains("Skin Sens. 1A", case=False, na=False))
                    & (df["conc_hom_mat"] >= 0.001)
                    & ~has_scl_1_1a
                ) & df["sensitization assessment"].isna(),
                "sensitization assessment",
            ] = "!!! Sens 1 or 1A present !!!"
            df.loc[
                (
                    df[col].str.contains("Skin Sens. 1B", case=False, na=False)
                    & (df["conc_hom_mat"] >= 0.01)
                    & ~has_scl_1b
                ) & df["sensitization assessment"].isna(),
                "sensitization assessment",
            ] = "RED"

        if "resp_sensitisation" in df.columns:
            col = "resp_sensitisation"
            df.loc[
                (
                    (df[col].str.contains("Resp. Sens. 1:", case=False, na=False)
                     | df[col].str.contains("Resp. Sens. 1A", case=False, na=False))
                    & (df["conc_hom_mat"] >= 0.001)
                    & ~has_scl_1_1a
                ) & df["sensitization assessment"].isna(),
                "sensitization assessment",
            ] = "!!! Sens 1 or 1A present !!!"
            df.loc[
                (
                    df[col].str.contains("Resp. Sens. 1B", case=False, na=False)
                    & (df["conc_hom_mat"] >= 0.01)
                    & ~has_scl_1b
                ) & df["sensitization assessment"].isna(),
                "sensitization assessment",
            ] = "RED"

        # Pass 3: C2C single-point chemical-level assessment passthrough (upstream data;
        # e.g. "mild sensitization" -> YELLOW per section 5.1.2.4/5.1.2.5 is captured there).
        if "sensitization C2C assessment" in df.columns:
            col = "sensitization C2C assessment"
            df.loc[(df[col] == "RED") & df["sensitization assessment"].isna(), "sensitization assessment"] = "RED"
            df.loc[(df[col] == "GREY") & df["sensitization assessment"].isna(), "sensitization assessment"] = "GREY"
            df.loc[(df[col] == "YELLOW") & df["sensitization assessment"].isna(), "sensitization assessment"] = "YELLOW"
            df.loc[(df[col] == "GREEN") & df["sensitization assessment"].isna(), "sensitization assessment"] = "GREEN"


        rating_col = "sensitization assessment"
        rank = { "!!! Sens 1 or 1A present !!!": 0 ,"RED": 1, "GREY": 2, "YELLOW": 3, "GREEN": 4}
        # a row left as None means none of the checks above could classify it - almost
        # always because its CAS wasn't found in the toxicity DB at all - use .get() with
        # a worst-rank sentinel so that never crashes; the completeness check below is what
        # actually decides whether the resulting rating can be trusted.
        worst_rank = max(rank.values()) + 1
        row_missing_data = df[rating_col].isna().any()
        rating = min(df[rating_col], key=lambda x: rank.get(x, worst_rank))
        sensitization_for_each_material.append({
            "hom_material": hom_material,
            "C2C Skin and Respiratory Sensitization": rating,
            "_missing_sensitization_data": row_missing_data,
        })

    result_df = pd.DataFrame(sensitization_for_each_material)

    # Any homogeneous material with unknown composition/CAS, or a known-CAS ingredient
    # for which no sensitization data could be found at all, can't get a trustworthy result.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    if "_missing_sensitization_data" in result_df.columns:
        incomplete_hom_materials |= set(
            result_df.loc[result_df["_missing_sensitization_data"], "hom_material"]
        )
    result_df = _apply_not_full_composition_label(
        result_df, incomplete_hom_materials, ["C2C Skin and Respiratory Sensitization"]
    )
    return result_df.drop(columns=["_missing_sensitization_data"], errors="ignore")
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
def _continue_m_factor_decades(value, first_tier_upper):
    """M factor keeps scaling x10 per decade below `first_tier_upper` (Table 9/GHS Table
    4.1.5: "continue in factor 10 intervals" indefinitely) - `value` must already be
    <= first_tier_upper. Replaces a previously hardcoded chain that capped out at a flat
    M factor once `value` dropped far enough (e.g. any LC50 <=0.0001 all got M=10000, or
    any NOEC <=0.000001 all got M=100000), understating the weight of extremely potent
    substances beyond that last hardcoded tier."""
    n = 1
    upper = first_tier_upper
    # guard against value <= 0 (shouldn't occur for a real LC50/NOEC) looping forever
    while value <= upper / 10 and n < 30:
        upper /= 10
        n += 1
    return 10 ** n


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
    known_acute_hazard_literals = {
        'Not Classified', 'Aqua. Acute 3: H402', 'Aqua. Acute 2: H401', 'Aqua. Acute 1: H400',
    }

    def classify_hazard(row):
        lc50 = row[lc_50]
        hazard = row[hazard_class]

        # GREY (unknown) whenever there is no usable numeric LC50 AND no RECOGNIZED hazard
        # classification string - previously an unrecognized/unexpected hazard string (a typo,
        # a value this tool doesn't know about, etc.) combined with a missing LC50 fell through
        # to the WORST possible rating by default instead of being flagged as unknown data.
        if pd.isna(lc50) and hazard not in known_acute_hazard_literals:
            return 'GREY', None
        elif lc50 > 100 or hazard == 'Not Classified':
            return 'GREEN', None
        elif 10 < lc50 <= 100 or hazard == 'Aqua. Acute 3: H402':
            return 'YELLOW', None
        elif 1 < lc50 <= 10 or hazard == 'Aqua. Acute 2: H401':
            return 'Acute 2', None
        elif 0.1 < lc50 <= 1 or hazard == 'Aqua. Acute 1: H400':
            return 'Acute 1', 1
        else:
            # LC50 <= 0.1: M factor continues scaling x10 per decade indefinitely
            # (Table 9), not capped at a flat 10000 for anything <=0.0001.
            return 'Acute 1', _continue_m_factor_decades(lc50, 0.1)

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
    known_chronic_hazard_literals = {
        'Aqua. Chronic 4: H413', 'Aqua. Chronic 3: H412', 'Aqua. Chronic 2: H411', 'Aqua. Chronic 1: H410',
    }

    def classify_hazard(row):
        noec_value = row[noec]
        hazard = row[hazard_class]

        # GREY (unknown) whenever there is no usable numeric NOEC AND no RECOGNIZED hazard
        # classification string - previously an unrecognized/unexpected hazard string
        # combined with a missing NOEC fell through to the WORST possible rating by default
        # instead of being flagged as unknown data.
        if pd.isna(noec_value) and hazard not in known_chronic_hazard_literals:
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
        else:
            # NOEC <= 0.01: M factor continues scaling x10 per decade indefinitely
            # (Table 9), not capped at a flat 100000 for anything <=0.000001.
            return 'Chronic 1', _continue_m_factor_decades(noec_value, 0.01)

    df_calculation[["designated_hazard_classification", "designated M factor"]] = df_calculation.apply(classify_hazard, axis=1).apply(pd.Series)

    df_calculation[m_factor] = df_calculation[m_factor].fillna(df_calculation["designated M factor"])

    hazard_col = "designated_hazard_classification"
    conc_col = "conc_hom_mat"
    m_col = m_factor

    results_for_each_material = []
    # assessment for each hom mat
    for hom_material in hom_materials:
        df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
        # Compute the sums for each category based on concentration thresholds. Table 17
        # footnote 15: a highly toxic Chronic 1 chemical (NOEC <= 0.01 mg/L) still counts
        # even below the normal 0.1% cutoff - previously such a chemical was silently
        # excluded whenever its concentration fell under 0.1%.
        chronic1_relevant = (df_calculation[hazard_col] == 'Chronic 1') & (
            (df[conc_col] >= 0.001) | (df[noec] <= 0.01)
        )
        sum_chronic1_x_m_factor = (
                df.loc[chronic1_relevant, conc_col] *
                df.loc[chronic1_relevant, m_col]).sum()

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

    # Combine acute and chronic by worst-case (RED > GREY > YELLOW > GREEN), the same
    # priority order used everywhere else in this file to combine sub-ratings (per-taxon
    # combination above, sub-endpoint combination in corr_n_irr/sensitization, etc.).
    # Figure 8 in the methodology document (p.29) draws this as acute GREEN/RED/GREY locking
    # in the final rating unconditionally, with chronic only consulted when acute == YELLOW -
    # but per explicit confirmation, that gated reading is NOT the intended rule: chronic
    # data must be able to escalate the result even when acute is GREEN (a substance can be
    # acutely harmless yet chronically hazardous - e.g. persistent/bioaccumulative - and the
    # methodology's own text says chronic data "should be considered" whenever available,
    # not only when acute is YELLOW). Worst-case combination is the conservative choice here.
    priority = {'RED': 0, 'GREY': 1, 'YELLOW': 2, 'GREEN': 3}

    def _worst_of(row):
        acute, chronic = row[acute_col], row[chronic_col]
        if pd.isna(chronic):
            return acute
        if pd.isna(acute):
            return chronic
        return acute if priority.get(acute, 99) <= priority.get(chronic, 99) else chronic

    df["C2C Acute and Chronic Aquatic Toxicity"] = df.apply(_worst_of, axis=1)

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
        ].copy()
    except Exception as e:
        print(f"WARNING Column selection failed: {e}")
        final_c2c_results_summary = final_c2c_results[["hom_material"]].copy()

    # Unknown composition/CAS covers acute toxicity, corrosion/irritation and sensitization
    # already (each overrides its own column internally) - aquatic toxicity doesn't have that
    # check yet, so apply it here for that column specifically.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    final_c2c_results_summary = _apply_not_full_composition_label(
        final_c2c_results_summary, incomplete_hom_materials, ["C2C Acute and Chronic Aquatic Toxicity"]
    )

    # Unknown chemicals (previously just printed and discarded) - surface them as a
    # diagnostic column instead, listing which CAS/route caused each affected homogeneous
    # material's acute-toxicity rating to need the "Not full comp" label.
    final_c2c_results_summary["Unknown ATE chemicals (diagnostic)"] = ""
    try:
        if not unknown_chemicals_df.empty and "Homogenous Material" in unknown_chemicals_df.columns:
            diagnostic_text = (
                unknown_chemicals_df.assign(
                    _entry=lambda d: d["CAS"].astype(str) + " (" + d["missing_ATE_route"].astype(str) + ")"
                )
                .groupby("Homogenous Material")["_entry"]
                .agg(lambda s: ", ".join(sorted(set(s))))
            )
            final_c2c_results_summary["Unknown ATE chemicals (diagnostic)"] = (
                final_c2c_results_summary["hom_material"].map(diagnostic_text).fillna("")
            )
    except Exception as e:
        print(f"WARNING Could not attach unknown chemicals diagnostic: {e}")

    return final_c2c_results_summary


### Full mixture-rules assessment straight from the DB: builds df_toxicity_info from the
### real database (build_mixture_rules_toxicity_info_from_db) instead of an Excel file,
### runs the existing 4 additive endpoint groups via mixture_rules_C2C_assessment
### unchanged, and merges in the Assessment C endpoints (assessment_c_mixture_rules).
def mixture_rules_C2C_assessment_from_db(df_product, db_path):
    cas_list = df_product["CAS"].unique().tolist()
    df_toxicity_info = build_mixture_rules_toxicity_info_from_db(cas_list, db_path)

    base_result = mixture_rules_C2C_assessment(df_product, df_toxicity_info)

    # Assessment C is not individually safe_run-wrapped internally (unlike each of the 4
    # groups inside mixture_rules_C2C_assessment above) - a DB hiccup or schema surprise
    # here must not take down the otherwise-valid, already-computed base_result for this
    # scenario. Degrade to NOT_ENOUGH_INFO_LABEL for Assessment C's own columns only.
    try:
        assessment_c_result = assessment_c_mixture_rules(df_product, cas_list, db_path)
    except Exception as e:
        print(f"WARNING assessment_c_mixture_rules failed: {e}")
        hom_materials = df_product["Homogenous Material"].unique().tolist()
        assessment_c_result = pd.DataFrame({
            "hom_material": hom_materials,
            **{f"C2C {label}": NOT_ENOUGH_INFO_LABEL for label in ASSESSMENT_C_ENDPOINTS.values()},
        })

    try:
        return base_result.merge(assessment_c_result, on="hom_material", how="outer")
    except Exception as e:
        print(f"WARNING Could not merge Assessment C results: {e}")
        return base_result
#################################################################
### Filter out placeholder/invalid CAS values before querying the DB
CAS_NUMBER_PATTERN = re.compile(r"^\d{2,7}-\d{2}-\d$")

def is_valid_cas_number(cas_str):
    """Check the CAS Registry Number format (digits-digits-checkdigit), e.g. 71-43-2."""
    return bool(CAS_NUMBER_PATTERN.match(cas_str))

def clean_cas_values(cas_list):
    """
    Filter a list of CAS values down to real CAS numbers only, dropping
    anything that isn't a valid CAS Registry Number format - missing
    values (NaN, None), placeholders ("not assessed", "no cas", ""),
    and free-text material names (e.g. "wood", "steel"). Also
    de-duplicates while preserving order.
    """
    cleaned = []
    seen = set()
    for cas in cas_list:
        if cas is None or (isinstance(cas, float) and pd.isna(cas)):
            continue
        cas_str = str(cas).strip()
        if not is_valid_cas_number(cas_str):
            continue
        if cas_str not in seen:
            seen.add(cas_str)
            cleaned.append(cas_str)
    return cleaned

def extract_info_from_DB(cas_list, db_path):
    cas_list = clean_cas_values(cas_list)

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

### Extract the C2C colour assessment hazards for a list of CAS numbers
def extract_colour_assessment_C2C(cas_list, db_path):
    """
    Pull all the C2C colour assessment hazard columns from table
    COLOUR_ASSESSMENT_C2C for the given CAS numbers, into one df with
    a CAS column (renamed from "ref") and all the hazard columns.
    """
    cas_list = clean_cas_values(cas_list)

    colour_assessment_cols = [
        "C2C_assessment_carcinogenicity",
        "C2C_assessment_disruption_of_endocrine_system",
        "C2C_assessment_mutagenicity_genotoxicity",
        "C2C_assessment_reproductive_toxicity",
        "C2C_assessment_development_toxicity",
        "C2C_assessment_neurotoxicity",
        "C2C_assessment_oral_toxicity",
        "C2C_assessment_inhalative_toxicity",
        "C2C_assessment_dermal_toxicity",
        "C2C_assessment_skin_eye_respiratory_corrosion_irritation",
        "C2C_assessment_sensitization",
        "C2C_assessment_fish_toxicity",
        "C2C_assessment_invertebrate_toxicity",
        "C2C_assessment_algae_toxicity",
        "C2C_assessment_terrestrial_toxicity",
        "C2C_assessment_other_species_toxicity",
        "C2C_assessment_persistence",
        "C2C_assessment_bioaccumulation",
        "C2C_assessment_combined_pb_risk_flag",
        "C2C_assessment_combined_aquatic_risk_flag",
        "C2C_assessment_climatic_relevance_ozone_depletion_potential",
    ]

    if not cas_list:
        return pd.DataFrame(columns=["CAS"] + colour_assessment_cols), pd.DataFrame()

    # --------------------------
    # connect DB (safe)
    # --------------------------
    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        print(f"[ERROR] Cannot connect to DB: {e}")
        return pd.DataFrame(), pd.DataFrame([{"CAS": "ALL", "table": "COLOUR_ASSESSMENT_C2C", "issue": "DB connection failed"}])

    selected_cols_sql = ", ".join([f'"{c}"' for c in ["ref"] + colour_assessment_cols])
    placeholders = ", ".join(["?"] * len(cas_list))

    # --------------------------
    # query (safe)
    # --------------------------
    try:
        query = f'''
        SELECT {selected_cols_sql}
        FROM COLOUR_ASSESSMENT_C2C
        WHERE ref IN ({placeholders})
        '''
        df = pd.read_sql_query(query, conn, params=tuple(cas_list))
    except Exception as e:
        print(f"[ERROR] Cannot query COLOUR_ASSESSMENT_C2C: {e}")
        conn.close()
        return pd.DataFrame(), pd.DataFrame([{"CAS": "ALL", "table": "COLOUR_ASSESSMENT_C2C", "issue": str(e)}])

    conn.close()

    df = df.rename(columns={"ref": "CAS"})

    # --------------------------
    # MISSING CAS OUTPUT
    # --------------------------
    found_cas = set(df["CAS"])
    missing_cas_log = [
        {"CAS": cas, "table": "COLOUR_ASSESSMENT_C2C", "issue": "missing record"}
        for cas in cas_list if cas not in found_cas
    ]
    df_missing = pd.DataFrame(missing_cas_log)

    return df, df_missing

### Build a df_toxicity_info-shaped DataFrame (matching exactly what every C2C_*/mixture-
### rule function already expects) straight from the real production database, replacing
### the old toxicity-info Excel file as the mixture-rules pipeline's data source.
def build_mixture_rules_toxicity_info_from_db(cas_list, db_path):
    """
    Real DB schema (confirmed against the production database, "Skin Sens 1A" is spelled
    without a period unlike every other SCONCLIM column - renamed below to match the
    convention skin_and_resp_sens_c2c already looks for):
    - ORALTOX / DERMALTOX / INHALTOX: each numeric measurement is stored as a
      ("<label> =", "<label> =-1") pair of TEXT columns - treated here as a (bound1, bound2)
      range and reduced to the WORSE (numerically lower = more hazardous) of the two.
    - AQUATOX / FISHTOX / INVTOX ("daph" in this codebase's naming) / ALGAETOX: same
      TEXT-pair convention for LC50/NOEC values.
    - SENSITISATION: raw CLP classification text per route.
    - SCONCLIM: whatever %-limit columns exist, pulled generically (not hardcoded) so any
      SCL ARCHE adds later is picked up automatically.
    - COLOUR_ASSESSMENT_C2C: pre-computed per-chemical C2C colour ratings, reused for the
      acute-toxicity GREY flag, the skin/eye/resp irritation input, and the sensitization
      "mild sensitization -> YELLOW" passthrough (via extract_colour_assessment_C2C).
    """
    # Every column the mixture-rule functions merge on/read, even when no CAS ends up
    # queryable at all (e.g. a homogeneous material whose only rows have no valid CAS) -
    # returning just ["CAS"] in that case would make every downstream merge raise a
    # KeyError on the FIRST column it tries to read (e.g. "LD50_oral"), which safe_run
    # would then silently swallow as a total failure for that endpoint group, dropping the
    # homogeneous material's row entirely instead of correctly falling through to the
    # "Not full comp" label.
    empty_columns = [
        "CAS", "LD50_oral", "LD50_dermal", "LC50_gas", "LC50_vapour", "LC50_dust_mist_aerosol",
        "CLP oral class", "CLP dermal class", "CLP inhalation class",
        "skin_sensitisation", "resp_sensitisation",
        "aquatic_tox_acute", "aquatic_tox_chronic", "m_factor",
        "fish_lc50", "fish_lc50_qsar", "fish_noec", "fish_noec_qsar",
        "daph_lc50", "daph_lc50_qsar", "daph_noec", "daph_noec_qsar",
        "algae_lc50", "algae_lc50_qsar", "algae_noec", "algae_noec_qsar",
        "oral toxicity C2C assessment", "inhalative toxicity C2C assessment",
        "dermal toxicity C2C assessment", "skin eye respiratory corrosion irritation C2C assessment",
        "sensitization C2C assessment",
    ]

    cas_list = clean_cas_values(cas_list)
    if not cas_list:
        return pd.DataFrame(columns=empty_columns)

    colour_df, _ = extract_colour_assessment_C2C(cas_list, db_path)

    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        print(f"[ERROR] Cannot connect to DB for mixture-rules toxicity info: {e}")
        return pd.DataFrame(columns=empty_columns)

    warned_queries = set()

    def _warn_once(table, col, e):
        key = (table, col)
        if key not in warned_queries:
            warned_queries.add(key)
            print(f"[WARNING] Query against {table}.\"{col}\" failed (reported once): {e}")

    def fetch_worst_numeric(table, cas, value_col):
        """Read the ("<value_col>", "<value_col>-1") pair for this CAS and return the
        worse (lower) of the two after coercing to numeric; NaN if neither parses."""
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT "{value_col}", "{value_col}-1" FROM "{table}" WHERE ref = ?', (cas,))
            row = cur.fetchone()
        except Exception as e:
            _warn_once(table, value_col, e)
            return np.nan
        if not row:
            return np.nan
        nums = [n for n in (pd.to_numeric(v, errors="coerce") for v in row) if pd.notna(n)]
        return min(nums) if nums else np.nan

    def fetch_text(table, cas, *cols):
        try:
            cols_sql = ", ".join(f'"{c}"' for c in cols)
            cur = conn.cursor()
            cur.execute(f'SELECT {cols_sql} FROM "{table}" WHERE ref = ?', (cas,))
            row = cur.fetchone()
        except Exception as e:
            _warn_once(table, ", ".join(cols), e)
            return (None,) * len(cols)
        return row if row else (None,) * len(cols)

    rows = []
    for cas in cas_list:
        (oral_class,) = fetch_text("ORALTOX", cas, "Oral toxicity Acute Tox classified")
        (dermal_class,) = fetch_text("DERMALTOX", cas, "Dermal toxicity Acute Tox classified")
        (inhal_class,) = fetch_text("INHALTOX", cas, "Inhalative toxicity Acute Tox classification")
        skin_sens, resp_sens = fetch_text(
            "SENSITISATION", cas,
            "Skin sensitization CLP classification", "Respiratory sensitization CLP classification",
        )
        aquatic_acute, aquatic_chronic, m_factor_raw = fetch_text(
            "AQUATOX", cas,
            "Aquatic toxicity Acute Tox classified", "Aquatic toxicity Chronic Tox classified", "M factor",
        )

        rows.append({
            "CAS": cas,
            "LD50_oral": fetch_worst_numeric("ORALTOX", cas, "Oral Acute: LD50 ="),
            "LD50_dermal": fetch_worst_numeric("DERMALTOX", cas, "Dermal Acute: LD50 ="),
            "LC50_gas": fetch_worst_numeric("INHALTOX", cas, "Inhalative toxicity Acute: LC50 (gas) ="),
            "LC50_vapour": fetch_worst_numeric("INHALTOX", cas, "Inhalative toxicity Acute: LC50 (vapor) ="),
            "LC50_dust_mist_aerosol": fetch_worst_numeric("INHALTOX", cas, "Inhalative toxicity Acute: LC50 (dust/mist/aerosol) ="),
            "CLP oral class": oral_class,
            "CLP dermal class": dermal_class,
            "CLP inhalation class": inhal_class,
            "skin_sensitisation": skin_sens,
            "resp_sensitisation": resp_sens,
            "aquatic_tox_acute": aquatic_acute,
            "aquatic_tox_chronic": aquatic_chronic,
            "m_factor": pd.to_numeric(m_factor_raw, errors="coerce"),
            "fish_lc50": fetch_worst_numeric("FISHTOX", cas, "Fish toxicity Acute: LC50 (96h) ="),
            "fish_lc50_qsar": fetch_worst_numeric("FISHTOX", cas, "Fish toxicity Acute QSAR: LC50 ="),
            "fish_noec": fetch_worst_numeric("FISHTOX", cas, "Fish toxicity Chronic: NOEC ="),
            "fish_noec_qsar": fetch_worst_numeric("FISHTOX", cas, "Fish toxicity Chronic QSAR: NOEC ="),
            "daph_lc50": fetch_worst_numeric("INVTOX", cas, "Invertebrate toxicity Acute: L(E)C50 (48h) ="),
            "daph_lc50_qsar": fetch_worst_numeric("INVTOX", cas, "Invertebrae toxicity Acute QSAR: LC50 ="),
            "daph_noec": fetch_worst_numeric("INVTOX", cas, "Invertebrae toxicity Chronic: NOEC ="),
            "daph_noec_qsar": fetch_worst_numeric("INVTOX", cas, "Invertebrae toxicity Chronic QSAR: NOEC ="),
            "algae_lc50": fetch_worst_numeric("ALGAETOX", cas, "Algae toxicity Acute: L(E)C50 (72/96h) ="),
            "algae_lc50_qsar": fetch_worst_numeric("ALGAETOX", cas, "Algae toxicity Acute QSAR: LC50 ="),
            "algae_noec": fetch_worst_numeric("ALGAETOX", cas, "Algae toxicity Chronic: NOEC ="),
            "algae_noec_qsar": fetch_worst_numeric("ALGAETOX", cas, "Algae toxicity Chronic QSAR: NOEC ="),
        })

    # SCL columns (SCONCLIM), pulled generically so any endpoint's SCL - present today or
    # added later - is automatically available under the naming convention
    # skin_and_resp_sens_c2c already looks for ("<Endpoint> - Lower/Upper Limit: (%)").
    scl_rename = {"Skin Sens 1A - Upper Limit: (%)": "Skin Sens. 1A - Upper Limit: (%)"}
    try:
        cur = conn.cursor()
        cur.execute('PRAGMA table_info("SCONCLIM")')
        scl_cols = [r[1] for r in cur.fetchall() if r[1] not in ("ID", "ref")]
    except Exception as e:
        print(f"[WARNING] SCONCLIM schema issue: {e}")
        scl_cols = []

    scl_rows = []
    if scl_cols:
        cols_sql = ", ".join(f'"{c}"' for c in scl_cols)
        for cas in cas_list:
            try:
                cur = conn.cursor()
                cur.execute(f'SELECT {cols_sql} FROM "SCONCLIM" WHERE ref = ?', (cas,))
                row = cur.fetchone()
            except Exception:
                row = None
            record = {"CAS": cas}
            if row:
                for col, val in zip(scl_cols, row):
                    record[scl_rename.get(col, col)] = val
            scl_rows.append(record)

    conn.close()

    df = pd.DataFrame(rows)
    if scl_rows:
        df = df.merge(pd.DataFrame(scl_rows), on="CAS", how="left")

    # Merge whenever colour_df has the columns we need - NOT gated on colour_df being
    # non-empty: a chemical simply absent from COLOUR_ASSESSMENT_C2C (a normal, expected
    # case, not a failure) still returns a correctly-columned but zero-ROW DataFrame from
    # extract_colour_assessment_C2C, and skipping the merge in that case would silently
    # drop all 5 "... C2C assessment" columns from the output - which C2C_acute_toxicity's
    # GREY-flag logic then reads UNGUARDED (df_hom["oral toxicity C2C assessment"], no
    # "in df.columns" check), raising a bare KeyError instead of just seeing NaN. Only skip
    # the merge if colour_df itself lacks the expected columns entirely (the DB-connection-
    # or query-failure fallback path in extract_colour_assessment_C2C, which returns a
    # truly columnless DataFrame).
    if "C2C_assessment_oral_toxicity" in colour_df.columns:
        df = df.merge(
            colour_df.rename(columns={
                "C2C_assessment_oral_toxicity": "oral toxicity C2C assessment",
                "C2C_assessment_inhalative_toxicity": "inhalative toxicity C2C assessment",
                "C2C_assessment_dermal_toxicity": "dermal toxicity C2C assessment",
                "C2C_assessment_skin_eye_respiratory_corrosion_irritation": "skin eye respiratory corrosion irritation C2C assessment",
                "C2C_assessment_sensitization": "sensitization C2C assessment",
            })[[
                "CAS",
                "oral toxicity C2C assessment",
                "inhalative toxicity C2C assessment",
                "dermal toxicity C2C assessment",
                "skin eye respiratory corrosion irritation C2C assessment",
                "sensitization C2C assessment",
            ]],
            on="CAS", how="left",
        )

    return df


### Assessment C: every C2C hazard endpoint NOT covered by the additive mixture-rule
### functions above (acute mammalian toxicity; skin/eye/respiratory irritation; skin/
### respiratory sensitization; aquatic toxicity). Per the methodology (section 1.5/2.2),
### CLP/GHS itself does not apply additive summation to Carcinogenicity, Germ Cell
### Mutagenicity, Reproductive Toxicity, or STOT - there is no scientific basis for
### assuming dilution reduces hazard for these endpoints. C2C extends the same non-
### additive treatment to every other endpoint in its 21-endpoint hazard list.
NOT_ENOUGH_INFO_LABEL = "NOT ENOUGH INFO TO CALCULATE - NO MIXTURE RULES APPLIED"

ASSESSMENT_C_ENDPOINTS = {
    "C2C_assessment_carcinogenicity": "Carcinogenicity",
    "C2C_assessment_disruption_of_endocrine_system": "Endocrine Disruption",
    "C2C_assessment_mutagenicity_genotoxicity": "Mutagenicity/Genotoxicity",
    "C2C_assessment_reproductive_toxicity": "Reproductive Toxicity",
    "C2C_assessment_development_toxicity": "Developmental Toxicity",
    "C2C_assessment_neurotoxicity": "Neurotoxicity",
    "C2C_assessment_terrestrial_toxicity": "Terrestrial Toxicity",
    "C2C_assessment_other_species_toxicity": "Other Species Toxicity",
    "C2C_assessment_persistence": "Persistence",
    "C2C_assessment_bioaccumulation": "Bioaccumulation",
    "C2C_assessment_combined_pb_risk_flag": "Combined PB Risk Flag",
    "C2C_assessment_combined_aquatic_risk_flag": "Combined Aquatic Risk Flag",
    "C2C_assessment_climatic_relevance_ozone_depletion_potential": "Climatic Relevance / Ozone Depletion Potential",
}

# GREEN < YELLOW < GREY < RED, matching the quick_static app's own worst_rating convention
# (extract_info_from_DB's rating_rank) - re-used here for consistency across the toolkit.
_ASSESSMENT_C_RANK = {"GREEN": 1, "YELLOW": 2, "GREY": 3, "RED": 4}

# Per-endpoint SCL column name(s) in SCONCLIM, if one exists for that endpoint - none of
# SCONCLIM's current columns (Skin Corr. 1B, Eye/Skin Irrit. 2, STOT SE 3, AAA, Skin Sens.
# 1/1A) correspond to any Assessment C endpoint today. Add an entry here
# (endpoint_colour_col -> "<SCL column> - Lower Limit: (%)") if/when ARCHE adds one; until
# then every endpoint below uses the flat 0.01% cut-off only.
ASSESSMENT_C_SCL_COLUMNS = {}


def assessment_c_mixture_rules(df_product, cas_list, db_path):
    """
    Non-additive C2C mixture rule for every "Assessment C" endpoint: per homogeneous
    material, a chemical is "relevant" if its concentration is >= 0.01% OR above its own
    SCL for that endpoint (if one is defined in ASSESSMENT_C_SCL_COLUMNS); the hom mat's
    rating is the WORST rating among its relevant chemicals. If any relevant chemical has
    no usable rating for that endpoint, the hom mat's rating for that endpoint is
    NOT_ENOUGH_INFO_LABEL rather than a possibly-wrong computed value.
    """
    colour_df, _ = extract_colour_assessment_C2C(cas_list, db_path)

    d = df_product.copy()
    d["conc_hom_mat"] = d[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    d = d.merge(colour_df, on="CAS", how="left")

    hom_materials = df_product["Homogenous Material"].unique().tolist()
    rows = []
    for hom_material in hom_materials:
        sub = d.loc[d["Homogenous Material"] == hom_material]
        record = {"hom_material": hom_material}

        for colour_col, label in ASSESSMENT_C_ENDPOINTS.items():
            out_col = f"C2C {label}"

            relevant_mask = (sub["CAS"] != "not assessed") & sub["conc_hom_mat"].notna() & (sub["conc_hom_mat"] >= 0.0001)
            scl_col = ASSESSMENT_C_SCL_COLUMNS.get(colour_col)
            if scl_col and scl_col in sub.columns:
                scl_fraction = pd.to_numeric(sub[scl_col], errors="coerce") / 100.0
                relevant_mask = relevant_mask | (sub["conc_hom_mat"] > scl_fraction)

            relevant = sub.loc[relevant_mask]

            if relevant.empty:
                record[out_col] = "GREEN"
                continue

            raw_values = relevant[colour_col] if colour_col in relevant.columns else pd.Series(dtype=object)
            if raw_values.isna().any() or len(raw_values) < len(relevant):
                record[out_col] = NOT_ENOUGH_INFO_LABEL
                continue

            ratings = raw_values.astype(str).str.strip().str.upper()
            if (~ratings.isin(_ASSESSMENT_C_RANK)).any():
                record[out_col] = NOT_ENOUGH_INFO_LABEL
                continue

            record[out_col] = max(ratings, key=lambda x: _ASSESSMENT_C_RANK[x])

        rows.append(record)

    result_df = pd.DataFrame(rows)

    # Unknown composition/CAS (the same completeness gate used by every other endpoint
    # group) also invalidates Assessment C's results for that hom mat.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    endpoint_cols = [f"C2C {label}" for label in ASSESSMENT_C_ENDPOINTS.values()]
    result_df = _apply_not_full_composition_label(result_df, incomplete_hom_materials, endpoint_cols)
    return result_df


### Every per-tier running-% column calculate_row_contributions() may have added, in a
### stable (tier ascending, prod before hom_mat, min before max) order - so the detailed
### overview always lists them the same way regardless of dict/column ordering.
_TIER_CONTRIBUTION_PATTERN = re.compile(r"^(min|max)_contribution_(prod|hom_mat)_t(\d+)$")


def _sorted_tier_contribution_cols(columns):
    def sort_key(col):
        m = _TIER_CONTRIBUTION_PATTERN.match(col)
        minmax, kind, tier = m.group(1), m.group(2), int(m.group(3))
        return (tier, kind != "prod", minmax != "min")

    return sorted((c for c in columns if _TIER_CONTRIBUTION_PATTERN.match(c)), key=sort_key)


def _tier_contribution_rename_map(tier_cols):
    labels = {
        ("min", "prod"): "Minimal % of material in product after Tier {t}",
        ("max", "prod"): "Maximal % of material in product after Tier {t}",
        ("min", "hom_mat"): "Minimal % of material in homogenous material after Tier {t}",
        ("max", "hom_mat"): "Maximal % of material in homogenous material after Tier {t}",
    }
    rename_map = {}
    for col in tier_cols:
        m = _TIER_CONTRIBUTION_PATTERN.match(col)
        minmax, kind, tier = m.group(1), m.group(2), m.group(3)
        rename_map[col] = labels[(minmax, kind)].format(t=tier)
    return rename_map


### Build a C2C assessment df (product/material/contribution cols + DB hazards) from a scenarios df
def build_c2c_assessment_df(scenarios_df, db_path):
    """
    Take a scenarios df (all or selected scenarios) and keep only the
    product/material/contribution columns, then join the C2C colour
    assessment hazards pulled from COLOUR_ASSESSMENT_C2C for the CAS
    numbers present in it.
    """
    base_cols = [
        product,
        min_percent_in_product,
        max_percent_in_product,
        hom_mat,
        "final_material_map",
        "scenario_id",
        "active",
        "status_reason",
        "min_contribution_prod",
        "max_contribution_prod",
        "min_contribution_hom_mat",
        "max_contribution_hom_mat",
        "CAS",
    ]
    base_cols = [c for c in base_cols if c in scenarios_df.columns]
    c2c_df = scenarios_df[base_cols].copy()
    # Keep the per-tier running-% columns (if calculate_row_contributions() produced them)
    # OUT of base_cols on purpose - they must land AFTER the hazard columns merged in below,
    # not before, since "N".."AH" in _apply_colour_conditional_formatting/the template's own
    # formulas hardcode the hazard columns' position as starting right after the base
    # columns. Re-attached via the original row index (preserved below through the merge)
    # rather than positionally, so this stays correct even if the hazards merge ever
    # duplicates a row (e.g. more than one hazard match for the same CAS).
    tier_cols = _sorted_tier_contribution_cols(scenarios_df.columns)
    if tier_cols:
        tier_lookup = scenarios_df[tier_cols].copy()
        c2c_df["_orig_row_idx"] = c2c_df.index

    cas_list = clean_cas_values(c2c_df["CAS"].tolist())
    hazards_df, missing_cas_df = extract_colour_assessment_C2C(cas_list, db_path)
    if not missing_cas_df.empty:
        print("CAS missing from COLOUR_ASSESSMENT_C2C:", missing_cas_df)

    c2c_df = c2c_df.merge(hazards_df, on="CAS", how="left")

    if tier_cols:
        for col in tier_cols:
            c2c_df[col] = c2c_df["_orig_row_idx"].map(tier_lookup[col])
        c2c_df.drop(columns=["_orig_row_idx"], inplace=True)

    # human-friendly column names for the saved excel
    rename_map = {
        "final_material_map": "Final Material Map",
        "scenario_id": "Scenario ID",
        "active": "Scenario Status",
        "status_reason": "Scenario Status Reason",
        "min_contribution_prod": "Minimal % of material in product",
        "max_contribution_prod": "Maximal % of material in product",
        "min_contribution_hom_mat": "Minimal % of material in homogenous material",
        "max_contribution_hom_mat": "Maximal % of material in homogenous material",
    }
    rename_map.update(_tier_contribution_rename_map(tier_cols))
    for col in c2c_df.columns:
        if col.startswith("C2C_assessment_"):
            rename_map[col] = col.replace("_", " ")
    c2c_df = c2c_df.rename(columns=rename_map)

    return c2c_df

### Save a C2C assessment df into the "detailed_overview" sheet of the C2C assessment template
### Generate formula rows 3..target_last_row on a summary sheet by translating its row-2 "origin" formula
# matches the hardcoded detailed_overview scan range in the template's formulas,
# e.g. "$A$2:$A$50000" or "$I$2:$I$100000" -> group(1) keeps the "$COL$2:$COL$" part
_SCAN_RANGE_PATTERN = re.compile(r"(\$[A-Za-z]{1,3}\$2:\$[A-Za-z]{1,3}\$)(?:50000|100000)")

def _extend_formula_sheet(ws, target_last_row, scan_last_row):
    if target_last_row < 2:
        return

    # columns that carry the origin formula in row 2 (skips blank spacer columns)
    formula_cols = [c for c in range(1, ws.max_column + 1) if ws.cell(row=2, column=c).value is not None]

    for col in formula_cols:
        origin_cell = ws.cell(row=2, column=col)
        origin_val = origin_cell.value
        origin_text = origin_val.text if hasattr(origin_val, "text") else origin_val
        origin_coord = origin_cell.coordinate
        origin_style = origin_cell._style

        # shrink the detailed_overview scan range to match the actual project size instead
        # of always scanning the template's full 50000/100000-row headroom - this is the
        # main thing that makes the summary sheets slow (or crash Excel) on large projects
        origin_text = _SCAN_RANGE_PATTERN.sub(rf"\g<1>{scan_last_row}", origin_text)
        origin_cell.value = ArrayFormula(ref=origin_coord, text=origin_text)

        # parse the formula once, then cheaply re-translate it for every target row
        translator = Translator(origin_text, origin=origin_coord)

        for row in range(3, target_last_row + 1):
            target_cell = ws.cell(row=row, column=col)
            target_coord = target_cell.coordinate
            translated = translator.translate_formula(target_coord)
            target_cell.value = ArrayFormula(ref=target_coord, text=translated)
            target_cell._style = origin_style

def save_c2c_assessment_workbook(c2c_df, output_path, template_path=C2C_ASSESSMENT_TEMPLATE_PATH):
    """
    Copy templates/C2C_assessment_template.xlsx to output_path and write
    c2c_df into its "detailed_overview" sheet (starting row 2). The
    template's other sheets ("overview", "percentage_assessed",
    "risk_assessed") ship with a single formula row (row 2); this
    generates however many extra formula rows this project needs
    (matched to len(c2c_df), capped at C2C_ASSESSMENT_TEMPLATE_MAX_ROWS)
    AND shrinks each formula's detailed_overview scan range to match
    (plus a small buffer) instead of always scanning the template's full
    50000/100000-row range, so small/medium projects stay fast (and large
    ones don't crash Excel) to recalculate. They then recalculate
    automatically once the file is opened.
    """
    if not os.path.exists(template_path):
        raise FileNotFoundError(
            f"C2C assessment template not found at: {template_path}\n"
            "Check C2C_ASSESSMENT_TEMPLATE_PATH at the top of this file."
        )

    n_rows = len(c2c_df)
    if n_rows > C2C_ASSESSMENT_TEMPLATE_MAX_ROWS:
        print(
            f"[WARNING] {n_rows} rows exceed the template's {C2C_ASSESSMENT_TEMPLATE_MAX_ROWS}-row cap - "
            "the summary sheets will be incomplete for the extra rows."
        )
        n_rows = C2C_ASSESSMENT_TEMPLATE_MAX_ROWS

    shutil.copy(template_path, output_path)

    wb = openpyxl.load_workbook(output_path)
    ws = wb["detailed_overview"]

    template_headers = [cell.value for cell in ws[1] if cell.value is not None]
    df_headers = list(c2c_df.columns)
    if df_headers[: len(template_headers)] != template_headers:
        print(
            "[WARNING] detailed_overview headers no longer match the template.\n"
            f"  template: {template_headers}\n"
            f"  data:     {df_headers}\n"
            "The 'overview' / 'percentage_assessed' / 'risk_assessed' formulas read fixed "
            "columns and may now point at the wrong data - update the template."
        )
    elif len(df_headers) > len(template_headers):
        # Extra trailing columns beyond the template's own headers (e.g. the per-tier %
        # tracking columns) - safe to write, since nothing reads past the template's own
        # columns by fixed letter; the template just has no header cells for them yet.
        for col_offset, header in enumerate(df_headers[len(template_headers):], start=len(template_headers) + 1):
            ws.cell(row=1, column=col_offset, value=header)

    for row_offset, row in enumerate(c2c_df.itertuples(index=False), start=2):
        for col_offset, value in enumerate(row, start=1):
            ws.cell(row=row_offset, column=col_offset, value=None if pd.isna(value) else value)

    # generate as many summary-sheet formula rows as this project needs (row 2 already ships in the template),
    # and shrink each formula's detailed_overview scan range to match (plus a little headroom) instead of
    # always scanning the template's full 50000/100000-row range - this is what actually kills Excel on
    # large projects, since every formula cell re-scans that whole range
    target_last_row = n_rows + 1 if n_rows >= 1 else 2
    scan_last_row = target_last_row + C2C_ASSESSMENT_SCAN_RANGE_BUFFER
    for sheet_name in ("overview", "percentage_assessed", "risk_assessed"):
        _extend_formula_sheet(wb[sheet_name], target_last_row, scan_last_row)

    # force Excel to recalculate the formula sheets when the file is opened
    wb.calculation.fullCalcOnLoad = True

    wb.save(output_path)


### Mixture-rules assessment output, styled the same way as MAS_quick_C2C_assessment_static.py's
### templated output (same colour highlighting/column styling convention) - but built from the
### mixture-rules pipeline's OWN already-aggregated DataFrames (c2c_extremes_df / all_c2c_scenario_
### results_df), which are per-HOMOGENEOUS-MATERIAL, not per-CAS like the quick_static output, so
### this writes plain values into a fresh workbook rather than reusing that per-CAS template file.
_MIXTURE_RULES_COLOUR_STYLES = {
    "red": (PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"), Font(color="9C0006")),
    "yellow": (PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"), Font(color="9C6500")),
    "grey": (PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"), Font(color="3B3B3B")),
    "green": (PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"), Font(color="006100")),
}


def _mixture_rules_hazard_columns(df):
    """Every hazard-rating column this pipeline produces (the 4 additive endpoint groups
    plus the 13 Assessment C endpoints) - identified by the "C2C <label>" naming
    convention shared by all of them, excluding their "<col>_scenario" companion columns
    (plain text, not a colour) so conditional formatting is never applied to those."""
    return [c for c in df.columns if c.startswith("C2C ") and not c.endswith("_scenario")]


def _apply_mixture_rules_colour_formatting(ws, hazard_col_letters, last_row):
    """Colour a cell if its text CONTAINS a hazard keyword (matches this pipeline's own
    labels, e.g. "RED", "NOT FULL COMP...", "NOT ENOUGH INFO..." - the latter two don't
    contain a colour keyword and so are intentionally left unstyled/plain, making them
    visually distinct from a real RED/YELLOW/GREEN/GREY rating)."""
    ws.conditional_formatting._cf_rules.clear()
    for keyword, (fill, font) in _MIXTURE_RULES_COLOUR_STYLES.items():
        for col in hazard_col_letters:
            formula = f'ISNUMBER(SEARCH("{keyword}",{col}2))'
            ws.conditional_formatting.add(f"{col}2:{col}{last_row}", FormulaRule(formula=[formula], fill=fill, font=font, stopIfTrue=False))


def _style_mixture_rules_sheet(ws, last_col, last_data_row):
    """Uniform column widths and a wrapped, taller header row, matching the visual
    convention used by MAS_quick_C2C_assessment_static.py's own output sheets."""
    UNIFORM_WIDTH = 26
    for col in range(1, last_col + 1):
        ws.column_dimensions[get_column_letter(col)].width = UNIFORM_WIDTH

    header_alignment = Alignment(wrap_text=True, vertical="bottom")
    data_alignment = Alignment(wrap_text=False)
    ws.row_dimensions[1].height = 45
    for col in range(1, last_col + 1):
        ws.cell(row=1, column=col).alignment = header_alignment
    for row in range(2, last_data_row + 1):
        for col in range(1, last_col + 1):
            ws.cell(row=row, column=col).alignment = data_alignment


def _write_df_to_sheet(wb, sheet_name, df):
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws = wb.create_sheet(sheet_name)
    for col_idx, col_name in enumerate(df.columns, start=1):
        ws.cell(row=1, column=col_idx, value=col_name)
    for row_offset, row in enumerate(df.itertuples(index=False), start=2):
        for col_offset, value in enumerate(row, start=1):
            ws.cell(row=row_offset, column=col_offset, value=None if pd.isna(value) else value)

    hazard_cols = _mixture_rules_hazard_columns(df)
    hazard_col_letters = [get_column_letter(df.columns.get_loc(c) + 1) for c in hazard_cols]
    last_row = max(len(df) + 1, 2)
    _apply_mixture_rules_colour_formatting(ws, hazard_col_letters, last_row)
    _style_mixture_rules_sheet(ws, len(df.columns), last_row)
    return ws


def save_mixture_rules_assessment_output(c2c_extremes_df, all_c2c_scenario_results_df, output_path):
    """
    Save the mixture-rules assessment as an "overview" sheet (worst-case hazard rating per
    homogeneous material across all scenarios - c2c_extremes_df) and a "detailed_overview"
    sheet (every scenario's own per-homogeneous-material result - all_c2c_scenario_results_df),
    both colour-highlighted the same way as MAS_quick_C2C_assessment_static.py's output, so
    the two "flavours" of C2C assessment stay visually/structurally easy to cross-reference.
    """
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    _write_df_to_sheet(wb, "overview", c2c_extremes_df)
    _write_df_to_sheet(wb, "detailed_overview", all_c2c_scenario_results_df)

    wb.save(output_path)


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
    # Toxicity info now comes exclusively from the database - no Excel toxicity-info
    # fallback. A material/endpoint combination with insufficient DB data is flagged
    # explicitly (NOT_FULL_COMPOSITION_LABEL / NOT_ENOUGH_INFO_LABEL) rather than silently
    # computed on partial data or backfilled from an Excel file.
    print("Proceeding with toxicity info from the database only.")
    print("--------------------------------------------------------------")
    # identify alternatives & make scenarios
    print("Generating scenarios...")
    df = identify_alternative_groups(df, max_tier)
    scenarios = generate_scenarios(df, max_tier)
    scenario_ids = [x['scenario_id'] for x in scenarios]
    print("Scenarios generated. Total number of scenarios: ", len(scenarios))
    print("Calculating... This might take a while...")
    # analyse the dataset: summary for each CAS & product assessed
    summary_df, perecentage_assessed_dict, C2C_mixture_results, all_c2c_scenario_results_df = analyse_the_dataset_with_mixture_rules(df, scenarios, db_path)
    ### Saving:
    now = datetime.now()
    time = now.strftime("%Y%m%d")
    saving_summary = os.path.join(saving_dir, f"summary_{time}_{file_name}.xlsx")
    saving_percent_assessed = os.path.join(saving_dir, f"percent_assessed_{time}_{file_name}.xlsx")
    saving_selected = os.path.join(saving_dir, f"selected_scenarios_{time}_{file_name}.xlsx")
    saving_all_scenarios = os.path.join(saving_dir, f"all_scenarios_{time}_{file_name}.xlsx")
    saving_CAS = os.path.join(saving_dir, f"CAS_{time}_{file_name}.xlsx")
    C2C_mixture_rules_saving = os.path.join(saving_dir, f"mixture_rules_{time}_{file_name}.xlsx")
    saving_all_c2c_mixture_scenario_results = os.path.join(saving_dir, f"all_scenarios_mixture_rules_{time}_{file_name}.xlsx")
    saving_c2c_assessment_all_scenarios = os.path.join(saving_dir, f"C2C_assessment_all_scenarios_{time}_{file_name}.xlsx")
    saving_c2c_assessment_selected_scenarios = os.path.join(saving_dir, f"C2C_assessment_selected_scenarios_{time}_{file_name}.xlsx")

    print("--------------------------------------------------------------")
    summary_df.to_excel(saving_summary, index=False)
    save_mixture_rules_assessment_output(C2C_mixture_results, all_c2c_scenario_results_df, C2C_mixture_rules_saving)
    print("Saved mixture rules assessment (overview + detailed_overview) to file: ", C2C_mixture_rules_saving)
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
        all_c2c_scenario_results_df.to_excel(saving_all_c2c_mixture_scenario_results, index=False)
        print("Saved all scenarios to file: ", saving_all_scenarios)
        c2c_assessment_all_scenarios_df = build_c2c_assessment_df(all_scenarios_df, db_path)
        save_c2c_assessment_workbook(c2c_assessment_all_scenarios_df, saving_c2c_assessment_all_scenarios)
        print("Saved C2C assessment for all scenarios to file: ", saving_c2c_assessment_all_scenarios)
    print("--------------------------------------------------------------")
    print("Do you want to save selected scenarios? (y/n)")
    user_input = input("").strip().lower()
    if user_input == "y":
        chosen = select_scenarios(scenario_ids)
        selected_df = build_selected_scenarios_df(df, scenarios, chosen)
        selected_df.to_excel(saving_selected, index=False)
        print("Saved the selected scenarios to file: ", saving_selected)
        c2c_assessment_selected_scenarios_df = build_c2c_assessment_df(selected_df, db_path)
        save_c2c_assessment_workbook(c2c_assessment_selected_scenarios_df, saving_c2c_assessment_selected_scenarios)
        print("Saved C2C assessment for selected scenarios to file: ", saving_c2c_assessment_selected_scenarios)
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

### Smaller projects: C2C assessment only, no mixture rules (all scenarios only)
def run_c2c_assessment_only():
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
    max_tier = get_highest_tier(df, col_CAS)
    print("Max Tier found: ", max_tier)
    # standardize & clean the df
    df = clean_data(df, max_tier)
    # add columns for analysis
    df = add_helper_columns(df, max_tier)
    df = add_final_map(df, max_tier)
    # how many CAS:
    CAS_count, cas_list = count_CAS_unique(df, "CAS")
    print("Total unique CAS found: ", CAS_count)
    print("--------------------------------------------------------------")
    # identify alternatives & make scenarios
    print("Generating scenarios...")
    df = identify_alternative_groups(df, max_tier)
    scenarios = generate_scenarios(df, max_tier)
    scenario_ids = [x['scenario_id'] for x in scenarios]
    print("Scenarios generated. Total number of scenarios: ", len(scenarios))
    print("Calculating... This might take a while...")
    # C2C assessment only works for all scenarios (no selection here)
    all_scenarios_df = build_selected_scenarios_df(df, scenarios, scenario_ids)
    print("--------------------------------------------------------------")

    # the template's formula sheets only cover detailed_overview rows 2..50000 -
    # bail out early (before hitting the DB) if this project is too big for it
    if len(all_scenarios_df) > C2C_ASSESSMENT_TEMPLATE_MAX_ROWS:
        print(f"The project is too big for a fast assessment as it generates more than "
              f"{C2C_ASSESSMENT_TEMPLATE_MAX_ROWS} rows ({len(all_scenarios_df)} rows).")
        print("The C2C Assessment template (option C) cannot summarise a project this size.")
        print("Do you want to proceed with option A or option B instead? \n"
              "A: just % assessed \n"
              "B: % assessed and mixture rules")
        fallback_choice = ""
        while fallback_choice not in ["A", "B"]:
            fallback_choice = input("Type A or B: ").strip().upper()
            if fallback_choice not in ["A", "B"]:
                print("Please type A or B.")
        print("--------------------------------------------------------------")
        if fallback_choice == "A":
            return run_with_percentage_assessed()
        else:
            return run_wint_C2C_mixture_rules()

    print("Pulling C2C colour assessment hazards from the DB and building the C2C assessment excel...")
    c2c_assessment_all_scenarios_df = build_c2c_assessment_df(all_scenarios_df, db_path)
    ### Saving:
    now = datetime.now()
    time = now.strftime("%Y%m%d")
    saving_c2c_assessment_all_scenarios = os.path.join(saving_dir, f"C2C_assessment_all_scenarios_{time}_{file_name}.xlsx")
    save_c2c_assessment_workbook(c2c_assessment_all_scenarios_df, saving_c2c_assessment_all_scenarios)
    print("Saved C2C assessment for all scenarios to file: ", saving_c2c_assessment_all_scenarios)
    print("--------------------------------------------------------------")
    print("Calculations finished. Have a nice day!")

### Start the program:
if __name__ == "__main__":
    # ---- Ask user ----
    choice = ""
    while choice not in ["A", "B", "C"]:
        choice = input("Which calculation do you want to run? \n"
                       "A: just % assessed \n"
                       "B: % assessed and mixture rules \n"
                       "C: Smaller projects - C2C Assessment without mixture rules (all scenarios only) \n"
                       "Type A, B or C").strip().upper()
        if choice not in ["A", "B", "C"]:
            print("Please type A, B or C.")

    # ---- Execute ----
    if choice == "A":
        result = run_with_percentage_assessed()
    elif choice == "B":
        result = run_wint_C2C_mixture_rules()
    else:
        result = run_c2c_assessment_only()

    print(result)