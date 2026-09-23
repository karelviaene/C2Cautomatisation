### Total C2C assessment

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
# --- Unused: this whole block only served save_c2c_assessment_workbook (below, also
# --- commented out) - the Excel-formula/row-capped single-file approach that every caller
# --- was migrated off of, in favour of save_c2c_assessment_output's plain-value,
# --- auto-splitting approach. No remaining references as of 2026-09 cleanup, kept for
# --- reference. (The newer, still-live MIXTURE_RULES_TEMPLATE_PATH is unrelated to this.)
#
# C2C_ASSESSMENT_TEMPLATE_PATH = os.path.join(
#     os.path.dirname(os.path.abspath(__file__)), "templates", "C2C_assessment_template.xlsx"
# )
# ### The "overview"/"percentage_assessed"/"risk_assessed" sheets ship with
# ### only ONE formula row (row 2) in the template. save_c2c_assessment_workbook()
# ### generates however many extra formula rows this project needs (matched
# ### to the number of rows written to "detailed_overview", capped at
# ### C2C_ASSESSMENT_TEMPLATE_MAX_ROWS below), AND shrinks each formula's
# ### detailed_overview scan range (the template's row 2 hardcodes
# ### $2:$50000 / $2:$100000) down to just cover that many rows plus
# ### C2C_ASSESSMENT_SCAN_RANGE_BUFFER of headroom. Both matter for speed:
# ### a real-world 30,000-row project with the old fixed 50000/100000 scan
# ### range made Excel crash on open, so keep this cap conservative even
# ### though it is technically possible to go higher.
# C2C_ASSESSMENT_TEMPLATE_MAX_ROWS = 5000
# C2C_ASSESSMENT_SCAN_RANGE_BUFFER = 100
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
    """Prompt the user via a file dialog to pick the MAS Excel file and load its first sheet into a dataframe, returning the dataframe, file name, and containing folder."""
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
    """Prompt the user via a directory dialog to choose the folder where output files will be saved, defaulting to the home folder if `default_path` is missing or invalid."""

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
    """Prompt the user via a file dialog to pick a SQL/SQLite database file, returning its path and file name."""
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
# --- Unused: no remaining callers as of 2026-09 cleanup, kept for reference ---
# def open_excel_file_toxicity():
#     messagebox.showinfo("Selection of the excel with toxicity info", "In the next step please select the excel file with toxicity info, make sure the data for the analysis in the first excel sheet.")
#     root = tk.Tk()
#     root.withdraw()
#     try:
#         file_path = filedialog.askopenfilename(
#             title="Select an Excel file",
#             filetypes=[("Excel files", "*.xlsx *.xls"),("All files", "*.*")])
#         if file_path:
#             if file_path.lower().endswith(('.xlsx', '.xls')):
#                 df = pd.read_excel(file_path)
#                 return df
#             else:
#                 print("Selected file is not an Excel")
#                 return None
#         else:
#             print("No file selected")
#             return None
#     except Exception as e:
#         print(f"Error: {e}")
#         return None
#
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         return None, None
### Clean data: add a col row_id for an identifier & normalize Y/N in capital letters etc
def clean_data(df, tier_level=10):
    """Normalize a raw MAS dataframe: strip column names, lowercase yes/no values, add a sequential `row_id`, trim whitespace in text columns, and coerce the min/max percent and weight columns (product-, homogeneous-material- and per-tier-level) to floats while flagging non-numeric entries."""
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
    """Determine the highest tier number present in the dataframe's columns matching `col_pattern` (e.g. "Tier {i} Material"), defaulting to 10 if none are found."""
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
    """Return the deepest (highest-tier) non-null material name for a row, scanning from `tier_level` down to tier 1."""
    for i in range(tier_level, 0, -1):
        col = col_mat.format(i=i)
        if pd.notna(row.get(col)):
            return row[col]
    return None
def get_final_supplier(row, tier_level=10):
    """Return the deepest (highest-tier) non-null supplier name for a row, scanning from `tier_level` down to tier 1."""
    for i in range(tier_level, 0, -1):
        col = col_sup.format(i=i)
        if pd.notna(row.get(col)):
            return row[col]
    return None
def get_final_CAS(row, tier_level=10):
    """Return the deepest (highest-tier) non-null CAS number for a row, or "not assessed" if none is found."""
    for i in range(tier_level, 0, -1):
        col = col_CAS.format(i=i)
        if pd.notna(row.get(col)):
            return row[col]
    return "not assessed"
def get_tier_depth(row, tier_level=10):
    """Return the deepest tier number for which the row has a non-null material entry."""
    for i in range(tier_level, 0, -1):
        col = col_tier_depth.format(i=i)
        if pd.notna(row.get(col)):
            return i
    return None
def add_helper_columns(df, max_tier):
    """Add per-row "CAS", "final_material", "final_supplier" and "tier_depth" helper columns derived from the deepest populated tier."""
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
    """Build a human-readable "Product → Homogeneous Material → Tier 1 (Supplier 1) → ..." path string for a row, stopping at its final tier depth."""
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
    """Add a "final_material_map" column holding each row's `build_location` path string."""
    df = df.copy()
    df["final_material_map"] = df.apply(lambda r: build_location(r, max_tier), axis=1)
    return df
### Identify all the alternatives in the group
def identify_alternative_groups(df, tier_level=10):
    """For each tier, derive a `t{i}_alt_group` column identifying the alternative-material group a row belongs to (product, reference material and anchor), based on the "Is alternative of tier {i} material" flags."""
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
    """Build, per product, the full cartesian-product set of alternative-material scenarios from each tier's `t{i}_alt_group` choices (a single base scenario if a product has no alternatives)."""
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
    """Decide whether a row belongs to (is "active" in) a given scenario, checking product match, per-tier alternative-group choices, and coupling rules against `selected_materials`; returns a (bool, reason) pair."""

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
    """Cascade a row's min/max percentages down through each populated tier to get its final min/max %-of-product and %-of-homogeneous-material contributions, plus a per-tier trace of the running values."""
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
    """Check whether a row's own per-tier alternative-group material picks are consistent with the scenario's choices, ignoring coupling rules."""
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
    """Filter the dataframe to the scenario's product, resolve the set of selected materials (alternative choices plus fixed materials on alternative-consistent rows), and tag every row with `scenario_id`, `active` and `status_reason` via `row_is_active`."""
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
    """Where a homogeneous material's %-of-product isn't given directly, derive it from its (deduplicated, active-only) min/max weight-in-product mass relative to the product total, and back-fill missing %-of-product values with that mass-based estimate."""
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
    """Where a Tier 1 material's %-of-homogeneous-material isn't given directly, derive it from its (deduplicated, active-only) min/max weight-in-hom-mat mass relative to the hom mat total, and back-fill missing values with that mass-based estimate."""
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
    """Run `calc_row_contribution` over every active row (NaN for inactive ones) and attach the resulting min/max %-of-product and %-of-hom-mat contributions plus their per-tier running-value trace columns."""
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
    """In-place update `record[key]_value`/`_scenario` with `value`/`scenario_id` if `value` is not NaN and lower than the current stored value (or none is stored yet)."""
    if pd.isna(value):
        return
    value_col = f"{key}_value"
    scenario_col = f"{key}_scenario"

    if value_col not in record or pd.isna(record[value_col]) or value < record[value_col]:
        record[value_col] = value
        record[scenario_col] = scenario_id
def update_high(record, key, value, scenario_id):
    """In-place update `record[key]_value`/`_scenario` with `value`/`scenario_id` if `value` is not NaN and higher than the current stored value (or none is stored yet)."""
    if pd.isna(value):
        return
    value_col = f"{key}_value"
    scenario_col = f"{key}_scenario"

    if value_col not in record or pd.isna(record[value_col]) or value > record[value_col]:
        record[value_col] = value
        record[scenario_col] = scenario_id
def build_selected_scenarios_df(df, scenarios, selected_scenario_ids):
    """For each scenario in `selected_scenario_ids`, evaluate row activity, derive hom-mat weights, and concatenate the resulting per-scenario dataframes into one combined dataframe."""
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
    """Evaluate every scenario's row contributions, track each row's absolute best/worst-case %-of-product and %-of-hom-mat bounds, compute the worst-case %-assessed across scenarios, run the C2C mixture-rule assessment per (product, hom mat), and build the per-CAS active-rows scaffold (with chemical-class flags) used by the overview/percentage-assessed/risk-assessed report builders; returns (summary_df, percentage_assessed_dict, c2c_extremes_df, all_c2c_scenario_results_df, active_scaffold_df)."""
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
    # Per-scenario per-CAS scaffolding rows (Product/Homogenous Material/CAS/contribution %s/
    # chemical-class flags + the mixture-rule-COMPUTED 8 endpoint values broadcast from that
    # scenario's (Product, Hom Mat) result) - purely scaffolding to feed build_overview_df/
    # build_percentage_assessed_df/build_risk_assessed_df (copied from
    # MAS_quick_C2C_assessment_static.py), which group over exactly this shape. This is
    # DIFFERENT from the per-CAS "detailed_overview" dataframe (build_c2c_assessment_df),
    # which carries each CAS's own RAW colour, not the hom-mat mixture-rule result.
    all_active_scaffold_rows = []

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
                "C2C oral toxicity",
                "C2C dermal toxicity",
                "C2C inhalative toxicity",
                "C2C skin eye respiratory corrosion irritation",
                "C2C sensitization",
                "C2C fish toxicity",
                "C2C invertebrate toxicity",
                "C2C algae toxicity",
            ] + [f"C2C {label}" for label in NO_MIXTURE_RULES_ENDPOINTS.values()]

            # -----------------------------
            # Aggregate worst colour per (PRODUCT, HOMOGENEOUS MATERIAL)
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

                # Group by (Product, homogeneous material) - NOT hom mat alone, so two
                # different products sharing a homogeneous-material name are never conflated.
                for (product_val, hom_mat), group_df in c2c_summary_df.groupby(["Product", hom_col]):
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

                    # Store in per-(Product, homogeneous material) dict
                    rec_hm = c2c_by_hom_mat.setdefault(
                        (product_val, hom_mat), {"Product": product_val, "Homogenous Material": hom_mat}
                    )

                    prev = rec_hm.get(endpoint)
                    if prev is None:
                        rec_hm[endpoint] = worst
                        rec_hm[f"{endpoint}_scenario"] = worst_scenario
                    elif colour_rank[worst] > colour_rank.get(prev, 0):
                        rec_hm[endpoint] = worst
                        rec_hm[f"{endpoint}_scenario"] = worst_scenario

            # -----------------------------
            # Per-CAS scaffolding rows for build_overview_df/build_percentage_assessed_df/
            # build_risk_assessed_df (item 6) - every active CAS row for this scenario,
            # carrying its own %-contribution columns plus the mixture-rule-COMPUTED 8
            # endpoint values broadcast from its (Product, Hom Mat)'s result.
            scaffold_cols = [
                "Product", "Homogenous Material", "CAS", "scenario_id", "active",
                min_percent_in_product, max_percent_in_product,
                "min_contribution_prod", "max_contribution_prod",
                "min_contribution_hom_mat", "max_contribution_hom_mat",
            ]
            scaffold_cols = [c for c in scaffold_cols if c in scenario_evaluated.columns]
            scaffold_rows = scenario_evaluated.loc[active_mask, scaffold_cols].copy()
            scaffold_rows = scaffold_rows[scaffold_rows["CAS"].notna()].copy()
            if not scaffold_rows.empty:
                mixture_cols = ["Product", hom_col] + [c for c in c2c_endpoint_cols if c in c2c_summary_df.columns]
                scaffold_rows = scaffold_rows.merge(
                    c2c_summary_df[mixture_cols], on=["Product", "Homogenous Material"], how="left"
                )
                all_active_scaffold_rows.append(scaffold_rows)

        # -----------------------------
        # Convert to DataFrames for output
        # -----------------------------
        # Worst-case per (Product, homogenous material)
        c2c_extremes_df = pd.DataFrame(c2c_by_hom_mat.values())

        # Optional full trace of all scenario results
        if all_c2c_scenario_results:
            all_c2c_scenario_results_df = pd.concat(all_c2c_scenario_results, ignore_index=True)
        else:
            all_c2c_scenario_results_df = pd.DataFrame()

    # -----------------------------
    # Build the per-CAS scaffolding dataframe (item 6) - concatenated across all scenarios,
    # renamed to the same column names build_overview_df/build_percentage_assessed_df/
    # build_risk_assessed_df (copied from MAS_quick_C2C_assessment_static.py) expect, plus
    # the chemical-class flags (item 4) worst-cased is left to those builders themselves
    # (they read the raw per-CAS "Organohalogen"/"Toxic metal"/"SVHC" columns merged in here).
    # -----------------------------
    if all_active_scaffold_rows:
        active_scaffold_df = pd.concat(all_active_scaffold_rows, ignore_index=True)
        cas_list_for_scaffold = clean_cas_values(active_scaffold_df["CAS"].tolist())
        chemical_class_df = extract_chemical_class(cas_list_for_scaffold, db_path)
        active_scaffold_df = active_scaffold_df.merge(chemical_class_df, on="CAS", how="left")
        active_scaffold_df["Scenario Status"] = True
        active_scaffold_df = active_scaffold_df.rename(columns={
            "scenario_id": "Scenario ID",
            "min_contribution_prod": "Minimal % of material in product",
            "max_contribution_prod": "Maximal % of material in product",
            "min_contribution_hom_mat": "Minimal % of material in homogenous material",
            "max_contribution_hom_mat": "Maximal % of material in homogenous material",
        })
    else:
        active_scaffold_df = pd.DataFrame()

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


    return summary_df, perecentage_assessed_dict, c2c_extremes_df, all_c2c_scenario_results_df, active_scaffold_df
def analyse_the_dataset(df, scenarios):
    """Evaluate every scenario's row contributions and track each row's absolute best/worst-case %-of-product and %-of-hom-mat bounds plus the worst-case %-assessed across scenarios (composition-percentage analysis only, without running the C2C mixture rules); returns (summary_df, percentage_assessed_dict)."""
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
    """Prompt the user on the console to pick scenario IDs by number (or "all"/"x" for none), and return the selected list."""
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
    """Return the count and list of unique, non-null values in `column_name`, excluding "not assessed" entries."""
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
    """Write the %-assessed summary, any invalid-material disclaimer/info, and the per-scenario calculation details to an Excel workbook."""
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
    """(Product, Homogeneous Material) pairs with at least one active row of unknown %
    (NaN) or unknown CAS ('not assessed'). Keyed on the PAIR, not the hom-mat name alone,
    so two different products that happen to share a homogeneous-material name are never
    conflated (a real bug this pipeline used to have)."""
    d = df_product.copy()
    d["conc_hom_mat"] = d[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    unknown_mask = d["conc_hom_mat"].isna() | (d["CAS"] == "not assessed")
    pairs = d.loc[unknown_mask, ["Product", "Homogenous Material"]].drop_duplicates()
    return set(pairs.itertuples(index=False, name=None))


def _apply_not_full_composition_label(df, incomplete_pairs, cols, label=NOT_FULL_COMPOSITION_LABEL):
    """Overwrite `cols` with `label` (default NOT_FULL_COMPOSITION_LABEL) for every row
    whose (Product, hom_material) pair is incomplete. `df` must carry both a "Product" and
    a "hom_material" column. Pass label=NOT_ENOUGH_DB_DATA_PLACEHOLDER for the "composition
    is fully known, but the database lacks the hazard data" case instead."""
    if not incomplete_pairs or df.empty:
        return df
    if "Product" in df.columns:
        pair_keys = list(zip(df["Product"], df["hom_material"]))
        mask = pd.Series([p in incomplete_pairs for p in pair_keys], index=df.index)
    else:
        # Fallback for any caller that hasn't been threaded with Product yet - match on
        # hom_material alone against either pair member (keeps old behaviour, doesn't crash).
        hom_mats_only = {p[1] for p in incomplete_pairs}
        mask = df["hom_material"].isin(hom_mats_only)
    existing_cols = [c for c in cols if c in df.columns]
    # these columns may currently be numeric (e.g. an ATE value) - cast to object first so
    # assigning the text label doesn't trip pandas' incompatible-dtype warning/future error
    for c in existing_cols:
        if df[c].dtype != object:
            df[c] = df[c].astype(object)
    df.loc[mask, existing_cols] = label
    return df


### The 8 endpoints the additive mixture rule CAN apply to, mapped to the per-chemical raw
### DB colour column (from COLOUR_ASSESSMENT_C2C, merged into df_toxicity_info by
### build_mixture_rules_toxicity_info_from_db) used for the "current worst case rating"
### fallback whenever the additive calculation itself can't produce a trustworthy value for
### a given (Product, Homogeneous Material). This is DIFFERENT from a genuinely-computed
### GREY coming out of the additive rule itself (e.g. the GREY_oral_tox-style flags) - that
### is a real mixture-rule result and stays a plain, unprefixed "GREY".
###
### Two distinct reasons get two distinct labels, so a reader can tell "go complete the
### MAS composition" apart from "go add data to the database" at a glance:
### - INCOMPLETE_COMP_LABEL: an active ingredient's identity or % is itself unknown (the
###   MAS composition is incomplete) - NOT_FULL_COMPOSITION_LABEL is the pre-fallback
###   placeholder written by _hom_materials_with_unknown_composition-based checks.
### - NOT_ENOUGH_DB_DATA_LABEL: every ingredient IS identified and quantified, but the
###   database itself lacks the specific hazard data (LD50/LC50/rating/etc.) a relevant,
###   known-CAS ingredient needs - NOT_ENOUGH_DB_DATA_PLACEHOLDER is that case's pre-fallback
###   placeholder.
INCOMPLETE_COMP_LABEL = "INCOMPLETE COMP - NO MIXTURE RULES - CURRENT WORST CASE RATING: {colour}"
NOT_ENOUGH_DB_DATA_LABEL = "NOT ENOUGH DATA IN DB TO CALCULATE MIXTURE RULES - WORST CASE: {colour}"
NOT_ENOUGH_DB_DATA_PLACEHOLDER = "NOT ENOUGH DATA IN DB TO CALCULATE MIXTURE RULES"

MIXTURE_RULE_CAPABLE_ENDPOINTS = {
    "C2C oral toxicity": "oral toxicity C2C assessment",
    "C2C dermal toxicity": "dermal toxicity C2C assessment",
    "C2C inhalative toxicity": "inhalative toxicity C2C assessment",
    "C2C skin eye respiratory corrosion irritation": "skin eye respiratory corrosion irritation C2C assessment",
    "C2C sensitization": "sensitization C2C assessment",
    "C2C fish toxicity": "fish toxicity C2C assessment",
    "C2C invertebrate toxicity": "invertebrate toxicity C2C assessment",
    "C2C algae toxicity": "algae toxicity C2C assessment",
}


def _worst_case_raw_colour(sub_df, colour_col):
    """Worst raw per-chemical colour (GREEN < YELLOW < GREY < RED) across `sub_df`'s rows
    for `colour_col`, treating a missing/unrecognised value as GREY - the same "missing =
    GREY" fallback convention used by assessment_with_no_mixture_rules's own per-endpoint
    loop, factored out here so both places share one implementation."""
    if colour_col not in sub_df.columns or sub_df.empty:
        return "GREY"
    ratings = sub_df[colour_col].astype(str).str.strip().str.upper()
    ratings = ratings.where(ratings.isin(_NO_MIXTURE_RULES_RANK), "GREY")
    if ratings.empty:
        return "GREY"
    return max(ratings, key=lambda x: _NO_MIXTURE_RULES_RANK[x])


def _apply_incomplete_comp_fallback(result_df, df_product, colour_df):
    """For the 8 mixture-rule-capable endpoints only: wherever the additive calculation
    could not produce a trustworthy value for a (Product, Homogeneous Material), replace
    the pre-fallback placeholder with the worst INDIVIDUAL raw colour among that (Product,
    Hom Mat)'s own relevant chemicals (missing = GREY, same convention as
    assessment_with_no_mixture_rules), wrapped in whichever of two labels matches the
    reason:
    - NOT_FULL_COMPOSITION_LABEL (an active ingredient's identity/% is itself unknown) ->
      INCOMPLETE_COMP_LABEL ("INCOMPLETE COMP - NO MIXTURE RULES - ...").
    - NOT_ENOUGH_DB_DATA_PLACEHOLDER, or a bare NaN (composition is fully known, but the
      database lacks the hazard data a relevant ingredient needs) -> NOT_ENOUGH_DB_DATA_LABEL
      ("NOT ENOUGH DATA IN DB TO CALCULATE MIXTURE RULES - ..."). NaN defaults here rather
      than to the composition label, since a genuine composition gap is always written
      explicitly as NOT_FULL_COMPOSITION_LABEL upstream - a bare NaN reaching this point
      means composition was fine and the calculation itself just had nothing to work with.

    `colour_df` must carry "CAS" plus the raw colour columns named in
    MIXTURE_RULE_CAPABLE_ENDPOINTS's values (see build_mixture_rules_toxicity_info_from_db's
    renaming of the COLOUR_ASSESSMENT_C2C columns). `result_df` must carry "Product" and
    "hom_material" columns.
    """
    if result_df.empty or "Product" not in result_df.columns:
        return result_df

    d = df_product.copy()
    d["conc_hom_mat"] = d[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    d = d.merge(colour_df, on="CAS", how="left")
    relevant_mask = (d["CAS"] != "not assessed") & d["conc_hom_mat"].notna() & (d["conc_hom_mat"] >= 0.0001)
    d = d.loc[relevant_mask]
    groups = {key: sub for key, sub in d.groupby(["Product", "Homogenous Material"], sort=False)}

    for out_col, colour_col in MIXTURE_RULE_CAPABLE_ENDPOINTS.items():
        if out_col not in result_df.columns:
            continue
        as_text = result_df[out_col].astype(str).str.upper()
        is_unknown_comp = as_text == NOT_FULL_COMPOSITION_LABEL.upper()
        is_missing_db_data = result_df[out_col].isna() | (as_text == NOT_ENOUGH_DB_DATA_PLACEHOLDER.upper())
        needs_fallback = is_unknown_comp | is_missing_db_data
        if not needs_fallback.any():
            continue
        if result_df[out_col].dtype != object:
            result_df[out_col] = result_df[out_col].astype(object)
        for idx in result_df.index[needs_fallback]:
            key = (result_df.at[idx, "Product"], result_df.at[idx, "hom_material"])
            sub = groups.get(key)
            worst_colour = _worst_case_raw_colour(sub, colour_col) if sub is not None else "GREY"
            label = INCOMPLETE_COMP_LABEL if is_unknown_comp[idx] else NOT_ENOUGH_DB_DATA_LABEL
            result_df.at[idx, out_col] = label.format(colour=worst_colour)

    return result_df


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
    # (Product, Homogenous Material) pairs, not hom-mat name alone - two different products
    # sharing a homogeneous-material name must never be conflated.
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )
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
    # Output starts with one row per (Product, homogeneous material) pair
    final_df = pd.DataFrame(product_hom_pairs, columns=["Product", "hom_material"])
    # Store unknown ATE chemicals here as dicts
    all_unknown_chemicals = []
    # Homogeneous materials whose composition/CAS itself is unknown can't get a trustworthy
    # rating for ANY route - we don't even know what's in the mixture. This is distinct from
    # (and always applied on top of) the per-route missing-hazard-data check below.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)

    # Per CLP/GHS and the C2C Mixture Hazard Assessment Methodology (section 3.2.1), Oral,
    # Dermal, and Inhalation Toxicity are three FULLY INDEPENDENT mixture-rule computations
    # (own ATE, own cut-offs, own rating) for the "Acute Mammalian Toxicity" sub-endpoint -
    # not one combined rating. So a data gap in one route must only invalidate THAT route's
    # own output, not the other routes that were fully computable. route_incomplete_pairs
    # tracks this per route ("oral"/"dermal"/"inhalation"), separately from
    # incomplete_hom_materials above (which still applies to every route, since an unknown
    # composition casts doubt on all of them equally).
    known_row_mask = (
        (df_calculation["CAS"] != "not assessed")
        & df_calculation["conc_hom_mat"].notna()
        & (df_calculation["conc_hom_mat"] >= 0.001)
    )
    route_groups = {}
    route_clp_cols = {}
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
        # "oral"/"dermal" stay their own groups; the three inhalation forms (gas/vapour/
        # dust-mist-aerosol) are ALTERNATE representations of the same exposure route
        # depending on the substance's physical form - a real substance is only ever tested
        # under ONE of them, so a material is only flagged for "inhalation" if NONE of the
        # requested inhalation endpoints have data, not if any single one of the three is
        # missing (checking all 3 independently, as an earlier version of this fix did,
        # flagged almost every real ingredient, since virtually none have all 3 populated).
        _group_key = "inhalation" if _route.startswith("inhalation") else _route
        route_groups.setdefault(_group_key, []).append(_filled_col)
        route_clp_cols.setdefault(_group_key, _clp_col)

    route_incomplete_pairs = {key: set() for key in route_groups}
    for _group_key, _filled_cols in route_groups.items():
        # A substance CLP has definitively classified as "Not classified" for this route
        # (with no measured value either) has a real, informative result - it contributes
        # nothing to the ATE sum, per CLP, and is NOT "missing hazard data". Only flag the
        # material as incomplete when the classification itself gives no answer either.
        clp_col = route_clp_cols[_group_key]
        not_classified_for_route = df_calculation[clp_col].astype(str).str.strip() == "Not classified"
        group_missing = df_calculation[_filled_cols].isna().all(axis=1) & known_row_mask & ~not_classified_for_route
        if group_missing.any():
            missing_pairs = df_calculation.loc[group_missing, ["Product", "Homogenous Material"]].drop_duplicates()
            route_incomplete_pairs[_group_key] |= set(missing_pairs.itertuples(index=False, name=None))

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

        # loop over each (Product, homogenous material) pair
        for product_val, hom_material in product_hom_pairs:
            df_hom = df_ate.loc[
                (df_ate["Product"] == product_val) & (df_ate["Homogenous Material"] == hom_material)
            ].copy()

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
                "Product": product_val,
                "hom_material": hom_material,
                cfg["ate_col"]: (
                    round(float(ate), 2)
                    if pd.notna(ate)
                    else np.nan
                ),
            })

        df_single_ate = pd.DataFrame(ate_rows)

        final_df = final_df.merge(df_single_ate, on=["Product", "hom_material"], how="left")


    # 4. GREY flags based on constituent assessments

    grey_rows = []

    for product_val, hom_material in product_hom_pairs:
        df_hom = df_calculation.loc[
            (df_calculation["Product"] == product_val) & (df_calculation["Homogenous Material"] == hom_material)
        ].copy()

        sum_oral_grey = df_hom.loc[df_hom["oral toxicity C2C assessment"] == "GREY","conc_hom_mat"].sum()

        sum_inhal_grey = df_hom.loc[df_hom["inhalative toxicity C2C assessment"] == "GREY","conc_hom_mat"].sum()

        sum_dermal_grey = df_hom.loc[df_hom["dermal toxicity C2C assessment"] == "GREY","conc_hom_mat"].sum()

        grey_rows.append({
            "Product": product_val,
            "hom_material": hom_material,
            "GREY_oral_tox": ("Yes" if sum_oral_grey >= 0.001 else "No"),
            "GREY_inhal_tox": ("Yes" if sum_inhal_grey >= 0.001 else "No"),
            "GREY_dermal_tox": ("Yes" if sum_dermal_grey >= 0.001 else "No")
        })

    grey_df = pd.DataFrame(grey_rows)

    final_df = final_df.merge(grey_df,on=["Product", "hom_material"],how="left")

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

    # 6. Per-route C2C acute toxicity ratings - oral/dermal/inhalative are reported as 3
    # FULLY INDEPENDENT outputs (confirmed by the user): each is derived only from its own
    # route's already-computed classification column(s) and its own GREY flag. They do NOT
    # need to agree with each other, and are never derived from a shared/combined verdict
    # (unlike the earlier single "C2C acute toxicity" column, which forced all 3 routes to
    # agree on one rating - removed).
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

    def _route_rating(route_cols, grey_col, out_col):
        final_df[out_col] = None
        final_df.loc[final_df[route_cols].eq("RED").any(axis=1), out_col] = "RED"
        final_df.loc[final_df[out_col].isna() & (final_df[grey_col] == "Yes"), out_col] = "GREY"
        final_df.loc[final_df[out_col].isna() & final_df[route_cols].eq("YELLOW").any(axis=1), out_col] = "YELLOW"
        final_df.loc[final_df[out_col].isna() & final_df[route_cols].eq("GREEN").any(axis=1), out_col] = "GREEN"

    _route_rating(["Acute toxicity oral C2C"], "GREY_oral_tox", "C2C oral toxicity")
    _route_rating(["Acute toxicity dermal C2C"], "GREY_dermal_tox", "C2C dermal toxicity")
    _route_rating(
        [
            "Acute toxicity inhalation (gases) C2C",
            "Acute toxicity inhalation (vapour) C2C",
            "Acute toxicity inhalation (dust/mist) C2C",
        ],
        "GREY_inhal_tox",
        "C2C inhalative toxicity",
    )

    # 6b. Any homogeneous material with unknown composition/CAS can't get a trustworthy
    # rating for ANY route - replace whatever was computed (including a possibly-wrong
    # RED/YELLOW/GREEN/GREY) with NOT_FULL_COMPOSITION_LABEL. A route with its OWN missing
    # DATABASE hazard data (route_incomplete_pairs) - composition is fully known, the
    # database just lacks what that route needs - only invalidates THAT route's own
    # columns, with the DISTINCT NOT_ENOUGH_DB_DATA_PLACEHOLDER label, per the methodology's
    # independent-per-route treatment (see the comment above route_incomplete_pairs) - oral
    # stays a real computed rating even if inhalation data is missing for one ingredient,
    # and vice versa. (mixture_rules_C2C_assessment_from_db's later call to
    # _apply_incomplete_comp_fallback upgrades each placeholder into its own final fallback
    # label - INCOMPLETE_COMP_LABEL or NOT_ENOUGH_DB_DATA_LABEL respectively.)
    route_output_cols = {
        "oral": ["Acute toxicity oral C2C", "C2C oral toxicity", "ATE_based_on_LD50_oral"],
        "dermal": ["Acute toxicity dermal C2C", "C2C dermal toxicity", "ATE_based_on_LD50_dermal"],
        "inhalation": [
            "Acute toxicity inhalation (gases) C2C",
            "Acute toxicity inhalation (vapour) C2C",
            "Acute toxicity inhalation (dust/mist) C2C",
            "C2C inhalative toxicity",
            "ATE_based_on_LC50_gas",
            "ATE_based_on_LC50_vapour",
            "ATE_based_on_LC50_dust_mist_aerosol",
        ],
    }
    for _group_key, _cols in route_output_cols.items():
        _existing_cols = [c for c in _cols if c in final_df.columns]
        # DB-data-missing first, unknown-composition second, so composition (the more
        # fundamental problem, when both apply to the same pair) wins the overwrite.
        final_df = _apply_not_full_composition_label(
            final_df,
            route_incomplete_pairs.get(_group_key, set()) - incomplete_hom_materials,
            _existing_cols,
            label=NOT_ENOUGH_DB_DATA_PLACEHOLDER,
        )
        final_df = _apply_not_full_composition_label(final_df, incomplete_hom_materials, _existing_cols)

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
    """Apply the C2C skin-corrosion/irritation mixture rule (concentration-weighted RED/GREY/YELLOW/GREEN thresholds on per-ingredient corrosion/irritation ratings) to each (Product, Homogeneous Material) pair, returning the per-pair "skin_corr" rating."""
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # (Product, Homogenous Material) pairs, not hom-mat name alone
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )

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
    # assessment for each (Product, hom mat) pair
    for product_val, hom_material in product_hom_pairs:
        df_calc_hom_material = df_calculation.loc[
            (df_calculation["Product"] == product_val) & (df_calculation["Homogenous Material"] == hom_material)
        ]
        rating = skin_irr_mixture_rating(df_calc_hom_material)
        skin_corr_for_each_material.append({
            "Product": product_val,
            "hom_material": hom_material,
            f"skin_corr": rating})

    skin_results_df = pd.DataFrame(skin_corr_for_each_material)
    return skin_results_df
def eye_corr_mixture_rule_c2c(df_product, df_toxicity_info):
    """Apply the C2C eye-corrosion/irritation mixture rule (concentration-weighted RED/GREY/YELLOW/GREEN thresholds on per-ingredient corrosion/irritation ratings) to each (Product, Homogeneous Material) pair, returning the per-pair "eye_corr" rating."""
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # (Product, Homogenous Material) pairs, not hom-mat name alone
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )

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
    # assessment for each (Product, hom mat) pair
    for product_val, hom_material in product_hom_pairs:
        df_calc_hom_material = df_calculation.loc[
            (df_calculation["Product"] == product_val) & (df_calculation["Homogenous Material"] == hom_material)
        ]
        rating = eye_irr_mixture_rating(df_calc_hom_material)
        eye_corr_for_each_material.append({
            "Product": product_val,
            "hom_material": hom_material,
            f"eye_corr": rating})

    eye_results_df = pd.DataFrame(eye_corr_for_each_material)
    return eye_results_df
def resp_corr_rule_c2c(df_product, df_toxicity_info):
    """For each (Product, Homogeneous Material) pair, take the worst (RED > GREY > YELLOW > GREEN) per-ingredient respiratory corrosion/irritation rating as the pair's "resp_corr" result."""
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # (Product, Homogenous Material) pairs, not hom-mat name alone
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )
    resp_corr_for_each_material = []
    for product_val, hom_material in product_hom_pairs:
        df = df_calculation.loc[
            (df_calculation["Product"] == product_val) & (df_calculation["Homogenous Material"] == hom_material)
        ]
        rating_col = "skin eye respiratory corrosion irritation C2C assessment"
        rank = {"RED": 0, "GREY": 1, "YELLOW": 2, "GREEN": 3}
        # .get(..., worst_rank) instead of raw rank[x]: a missing/unexpected rating (e.g. NaN
        # from a CAS not found in the toxicity DB) must never raise and take down every other
        # homogeneous material's result in this same call - the completeness check in
        # corr_n_irr_mixture_rule_c2c is what actually decides whether to trust this value.
        worst_rank = max(rank.values()) + 1
        rating = min(df[rating_col], key=lambda x: rank.get(x, worst_rank))
        resp_corr_for_each_material.append({
            "Product": product_val,
            "hom_material": hom_material,
            f"resp_corr": rating})

    resp_results_df = pd.DataFrame(resp_corr_for_each_material)
    return resp_results_df
def corr_n_irr_mixture_rule_c2c(df_product, df_toxicity_info):
    """Combine the skin, eye and respiratory corrosion/irritation mixture-rule results into an overall "C2C skin eye respiratory corrosion irritation" rating (the worst of the three) per (Product, Homogeneous Material) pair, overwriting pairs with missing DB data or unknown/incomplete composition with the appropriate not-full-composition label."""
    skin_result = skin_corr_mixture_rule_c2c(df_product, df_toxicity_info)
    eye_result = eye_corr_mixture_rule_c2c(df_product, df_toxicity_info)
    resp_result = resp_corr_rule_c2c(df_product, df_toxicity_info)
    df_results = (
        skin_result.merge(eye_result, on=["Product", "hom_material"], how="left")
        .merge(resp_result, on=["Product", "hom_material"], how="left")
    )
    df_results["C2C skin eye respiratory corrosion irritation"] = None
    rank = {"RED": 0, "GREY": 1, "YELLOW": 2, "GREEN": 3}
    df_results["C2C skin eye respiratory corrosion irritation"] = (
        df_results[["skin_corr", "eye_corr", "resp_corr"]]
        .apply(lambda row: min(row, key=lambda x: rank.get(x, float("inf"))), axis=1))

    # Unknown composition/CAS (identity/% itself unknown) vs. a known-CAS ingredient simply
    # missing the corrosion/irritation rating in the database - two different reasons, two
    # different labels (see NOT_ENOUGH_DB_DATA_PLACEHOLDER's docstring), both meaning this
    # result can't be trusted.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    rating_col = "skin eye respiratory corrosion irritation C2C assessment"
    df_calc = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    df_calc["conc_hom_mat"] = df_calc[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    missing_rating_mask = (
        df_calc[rating_col].isna() & (df_calc["CAS"] != "not assessed") & df_calc["conc_hom_mat"].notna()
    )
    missing_pairs = df_calc.loc[missing_rating_mask, ["Product", "Homogenous Material"]].drop_duplicates()
    missing_db_data_pairs = set(missing_pairs.itertuples(index=False, name=None)) - incomplete_hom_materials

    result_cols = ["skin_corr", "eye_corr", "resp_corr", "C2C skin eye respiratory corrosion irritation"]
    # DB-data-missing first, unknown-composition second, so composition (the more
    # fundamental problem, when both apply to the same pair) wins the overwrite.
    df_results = _apply_not_full_composition_label(
        df_results, missing_db_data_pairs, result_cols, label=NOT_ENOUGH_DB_DATA_PLACEHOLDER
    )
    df_results = _apply_not_full_composition_label(df_results, incomplete_hom_materials, result_cols)
    return df_results

### 3. Skin and Respiratory Sensitization ###
def skin_and_resp_sens_c2c(df_product, df_toxicity_info):
    """Derive the C2C skin/respiratory sensitization rating per (Product, Homogeneous Material) pair by checking each ingredient's concentration against its specific concentration limit (SCL) when available, falling back to the generic CLP 1/1A (>=0.1%) and 1B (>=1.0%) thresholds and any pre-computed chemical-level sensitization rating, taking the worst result and labelling pairs with unknown composition or missing DB sensitization data accordingly."""
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")

    # (Product, Homogenous Material) pairs, not hom-mat name alone
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )

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

    # Step 3: assess per (Product, homogenous material) pair
    sensitization_for_each_material = []
    for product_val, hom_material in product_hom_pairs:
        df = df_calculation.loc[
            (df_calculation["Product"] == product_val) & (df_calculation["Homogenous Material"] == hom_material)
        ].copy()
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
            "Product": product_val,
            "hom_material": hom_material,
            "C2C sensitization": rating,
            "_missing_sensitization_data": row_missing_data,
        })

    result_df = pd.DataFrame(sensitization_for_each_material)

    # Unknown composition/CAS (identity/% itself unknown) vs. a known-CAS ingredient for
    # which no sensitization data could be found in the database at all - two different
    # reasons, two different labels (see NOT_ENOUGH_DB_DATA_PLACEHOLDER's docstring), both
    # meaning this result can't be trusted.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    missing_db_data_pairs = set()
    if "_missing_sensitization_data" in result_df.columns:
        missing_pairs = result_df.loc[
            result_df["_missing_sensitization_data"], ["Product", "hom_material"]
        ].drop_duplicates()
        missing_db_data_pairs = set(missing_pairs.itertuples(index=False, name=None)) - incomplete_hom_materials
    # DB-data-missing first, unknown-composition second, so composition (the more
    # fundamental problem, when both apply to the same pair) wins the overwrite.
    result_df = _apply_not_full_composition_label(
        result_df, missing_db_data_pairs, ["C2C sensitization"], label=NOT_ENOUGH_DB_DATA_PLACEHOLDER
    )
    result_df = _apply_not_full_composition_label(
        result_df, incomplete_hom_materials, ["C2C sensitization"]
    )
    return result_df.drop(columns=["_missing_sensitization_data"], errors="ignore")
# --- Unused: no remaining callers as of 2026-09 cleanup, kept for reference ---
# def skin_sens_clp(df_product, df_toxicity_info):
#     df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
#
#     # get the unique hom materials
#     hom_materials = df_product["Homogenous Material"].unique().tolist()
#
#     # save the highest value of contribution of hom mat
#     df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
#
#     # List of endpoints for sensitization:
#     endpoints = ["Skin Sens. 1", "Skin Sens. 1A", "Skin Sens. 1B"]
#     # Step 1: Create SCL columns (from the DB lowest of Lower/Upper Limits)
#     for ep in endpoints:
#         lower_col = f"{ep} - Lower Limit: (%)"
#         upper_col = f"{ep} - Upper Limit: (%)"
#
#         # Check if at least one of the columns exists
#         if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
#             # Use min row-wise, ignoring missing columns
#             df_calculation[f"SCL {ep}"] = df_calculation[[c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)
#
#     # Step 2: Create check columns comparing concentration in the mixture with SCL
#     for ep in endpoints:
#         scl_col = f"SCL {ep}"
#         check_col = f"{scl_col} - check"
#
#         if scl_col in df_calculation.columns:
#             df_calculation[check_col] = np.where(
#                 df_calculation[scl_col].isna(),
#                 None,  # SCL missing
#                 np.where(df_calculation["conc_hom_mat"] > df_calculation[scl_col], "Yes", "No")
#             )
#
#     # Step 3: assess per homogenous material
#     sensitization_for_each_material = []
#     for hom_material in hom_materials:
#         df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
#         df["sensitization assessment"] = None
#         # Loop over all check columns e.g. "SCL Skin Sens. 1 - check"
#         for col in df.columns:
#             # check SCL for each
#             if col.endswith("- check"):
#                 #print(col)
#                 # For rows where check is "Yes" and assessment not set yet
#                 if col in ["SCL Skin Sens. 1A - check"]:
#                     df.loc[(df[col] == "Yes"), "sensitization assessment"] = "cat. 1A"
#                 elif col in ["SCL Skin Sens. 1B - check"]:
#                     df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
#                 elif col in ["SCL Skin Sens. 1 - check"]:
#                     df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"
#             # check general conc limits
#             if col in ["skin_sensitisation"]:
#                 # for Sens. 1A
#                 df.loc[
#                     ((df[col].str.contains("Skin Sens. 1A", case=False, na=False)) & (df["conc_hom_mat"] >= 0.001)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1A"
#                 # for Sens. 1B
#                 df.loc[((df[col].str.contains("Skin Sens. 1B", case=False, na=False)) & (df["conc_hom_mat"]>=0.01)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
#                 # for Sens. 1
#                 df.loc[
#                     ((df[col].str.contains("Skin Sens. 1: H317", case=False, na=False)) & (df["conc_hom_mat"] >= 0.01)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"
#
#         rating_col = "sensitization assessment"
#         rank = { "cat. 1A": 0 ,"cat. 1B": 1, "cat. 1": 2, None: 3}
#         rating = min(df[rating_col], key=lambda x: rank[x])
#         sensitization_for_each_material.append({
#             "hom_material": hom_material,
#             f"CLP Skin Sensitization": rating})
#
#     return pd.DataFrame(sensitization_for_each_material)
# def resp_sens_clp(df_product, df_toxicity_info, state = "solid/liquid" or "gas"):
#     if state == "solid/liquid":
#         lim_1a = 0.001
#         lim_1b = 0.01
#         lim_1 = 0.01
#     elif state == "gas":
#         lim_1a = 0.001
#         lim_1b = 0.002
#         lim_1 = 0.002
#
#     df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
#
#     # get the unique hom materials
#     hom_materials = df_product["Homogenous Material"].unique().tolist()
#
#     # save the highest value of contribution of hom mat
#     df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
#
#     # List of endpoints for sensitization:
#     endpoints = ["Resp. Sens. 1A", "Resp. Sens. 1B", "Resp. Sens. 1"]
#     # Step 1: Create SCL columns (from the DB lowest of Lower/Upper Limits)
#     for ep in endpoints:
#         lower_col = f"{ep} - Lower Limit: (%)"
#         upper_col = f"{ep} - Upper Limit: (%)"
#
#         # Check if at least one of the columns exists
#         if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
#             # Use min row-wise, ignoring missing columns
#             df_calculation[f"SCL {ep}"] = df_calculation[[c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)
#
#     # Step 2: Create check columns comparing concentration in the mixture with SCL
#     for ep in endpoints:
#         scl_col = f"SCL {ep}"
#         check_col = f"{scl_col} - check"
#
#         if scl_col in df_calculation.columns:
#             df_calculation[check_col] = np.where(
#                 df_calculation[scl_col].isna(),
#                 None,  # SCL missing
#                 np.where(df_calculation["conc_hom_mat"] > df_calculation[scl_col], "Yes", "No")
#             )
#
#     # Step 3: assess per homogenous material
#     sensitization_for_each_material = []
#     for hom_material in hom_materials:
#         df = df_calculation.loc[df_calculation["Homogenous Material"] == hom_material]
#         df["sensitization assessment"] = None
#         # Loop over all check columns e.g. "SCL Skin Sens. 1 - check"
#         for col in df.columns:
#             # check SCL for each
#             if col.endswith("- check"):
#                 #print(col)
#                 # For rows where check is "Yes" and assessment not set yet
#                 if col in ["SCL Resp. Sens. 1A - check"]:
#                     df.loc[(df[col] == "Yes"), "sensitization assessment"] = "cat. 1A"
#                 elif col in ["SCL Resp. Sens. 1B - check"]:
#                     df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
#                 elif col in ["SCL Resp. Sens. 1 - check"]:
#                     df.loc[(df[col] == "Yes") & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"
#             # check general conc limits
#             if col in ["resp_sensitisation"]:
#                 # for Sens. 1A
#                 df.loc[
#                     ((df[col].str.contains("Resp. Sens. 1A", case=False, na=False)) & (df["conc_hom_mat"] >= lim_1a)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1A"
#                 # for Sens. 1B
#                 df.loc[((df[col].str.contains("Resp. Sens. 1B", case=False, na=False)) & (df["conc_hom_mat"]>=lim_1b)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1B"
#                 # for Sens. 1
#                 df.loc[
#                     ((df[col].str.contains("Resp. Sens. 1: H317", case=False, na=False)) & (df["conc_hom_mat"] >= lim_1)) & df["sensitization assessment"].isna(), "sensitization assessment"] = "cat. 1"
#
#         rating_col = "sensitization assessment"
#         rank = { "cat. 1A": 0 ,"cat. 1B": 1, "cat. 1": 2, None: 3}
#         rating = min(df[rating_col], key=lambda x: rank[x])
#         sensitization_for_each_material.append({
#             "hom_material": hom_material,
#             f"CLP Resp Sensitization": rating})
#
#     return pd.DataFrame(sensitization_for_each_material)

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
    """Apply the CLP/C2C acute aquatic toxicity mixture rule for one species (`type` = "fish"/"daph"/"algae") to each (Product, Homogeneous Material) pair, classifying each ingredient from its worst experimental/QSAR LC50 (or its own hazard class) into Acute 1 (M-factor scaled)/Acute 2/YELLOW/GREEN/GREY, then combining those into an overall RED/GREY/YELLOW/GREEN mixture rating."""

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
    # (Product, Homogenous Material) pairs, not hom-mat name alone
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )

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
    # assessment for each (Product, hom mat) pair
    for product_val, hom_material in product_hom_pairs:
        df = df_calculation.loc[
            (df_calculation["Product"] == product_val) & (df_calculation["Homogenous Material"] == hom_material)
        ]
        # Compute the sums for each category based on concentration thresholds
        sum_acute1_x_m_factor = (df.loc[(df_calculation.loc[df.index, hazard_col] == 'Acute 1') & (df[conc_col] >= 0.001), conc_col] *
                      df.loc[(df_calculation.loc[df.index, hazard_col] == 'Acute 1') & (df[conc_col] >= 0.001), m_col]).sum()
        sum_acute2 = df.loc[(df_calculation.loc[df.index, hazard_col] == 'Acute 2') & (df[conc_col] >= 0.01), conc_col].sum()
        sum_yellow = df.loc[(df_calculation.loc[df.index, hazard_col] == 'YELLOW') & (df[conc_col] >= 0.01), conc_col].sum()
        sum_grey   = df.loc[(df_calculation.loc[df.index, hazard_col] == 'GREY') & (df[conc_col] >= 0.001), conc_col].sum()


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
            "Product": product_val,
            "hom_material": hom_material,
            f"{type} aquatic acute tox": mixture_hazard})
    return pd.DataFrame(results_for_each_material)
def final_acute_aquatic_c2c(df_product, df_toxicity_info):
    """Per-species acute aquatic toxicity only (fish/invertebrate/algae) - NO cross-species
    combination here. Per explicit confirmation, the 3 aquatic species outputs are fully
    independent and must not be forced to agree with each other; final_aquatic_c2c combines
    each species' OWN acute+chronic result separately instead."""
    results_fish = acute_aquatic_c2c(df_product, df_toxicity_info, type = "fish")
    result_daph = acute_aquatic_c2c(df_product, df_toxicity_info, type = "daph")
    results_algae = acute_aquatic_c2c(df_product, df_toxicity_info, type = "algae")

    results_aqua_tox_acute = (
        results_fish.merge(result_daph, on=["Product", "hom_material"], how="outer")
        .merge(results_algae, on=["Product", "hom_material"], how="outer")
    )
    return results_aqua_tox_acute
## Chronic aquatic tox
def chronic_aquatic_c2c(df_product, df_toxicity_info, type = "fish" or "daph" or "algae"):
    """Apply the CLP/C2C chronic aquatic toxicity mixture rule for one species (`type` = "fish"/"daph"/"algae") to each (Product, Homogeneous Material) pair, classifying each ingredient from its worst experimental/QSAR NOEC (or its own hazard class) into Chronic 1 (M-factor scaled, including the sub-0.1% carve-out)/2/3/4/YELLOW/GREEN/GREY, then combining those weighted sums into an overall RED/GREY/YELLOW/GREEN mixture rating."""

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
    # (Product, Homogenous Material) pairs, not hom-mat name alone
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )

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
    # assessment for each (Product, hom mat) pair
    for product_val, hom_material in product_hom_pairs:
        df = df_calculation.loc[
            (df_calculation["Product"] == product_val) & (df_calculation["Homogenous Material"] == hom_material)
        ]
        df_hazard = df_calculation.loc[df.index, hazard_col]
        # Compute the sums for each category based on concentration thresholds. Table 17
        # footnote 15: a highly toxic Chronic 1 chemical (NOEC <= 0.01 mg/L) still counts
        # even below the normal 0.1% cutoff - previously such a chemical was silently
        # excluded whenever its concentration fell under 0.1%.
        chronic1_relevant = (df_hazard == 'Chronic 1') & (
            (df[conc_col] >= 0.001) | (df[noec] <= 0.01)
        )
        sum_chronic1_x_m_factor = (
                df.loc[chronic1_relevant, conc_col] *
                df.loc[chronic1_relevant, m_col]).sum()

        sum_chronic2 = df.loc[(df_hazard == 'Chronic 2') &(df[conc_col] >= 0.01), conc_col].sum()

        sum_chronic3 = df.loc[(df_hazard == 'Chronic 3') &(df[conc_col] >= 0.01),conc_col].sum()

        sum_chronic4 = df.loc[(df_hazard == 'Chronic 4') &(df[conc_col] >= 0.01),conc_col].sum()

        sum_grey = df.loc[(df_hazard == 'GREY')&(df[conc_col] >= 0.001),conc_col].sum()

        sum_yellow = df.loc[(df_hazard == 'YELLOW') &(df[conc_col] >= 0.01),conc_col].sum()

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
            "Product": product_val,
            "hom_material": hom_material,
            f"{type} aquatic chronic tox": mixture_hazard})
    return pd.DataFrame(results_for_each_material)
def final_chronic_aquatic_c2c(df_product, df_toxicity_info):
    """Per-species chronic aquatic toxicity only (fish/invertebrate/algae) - NO cross-species
    combination here, for the same "fully independent species" reason as
    final_acute_aquatic_c2c above."""
    results_fish = chronic_aquatic_c2c(df_product, df_toxicity_info, type = "fish")
    result_daph = chronic_aquatic_c2c(df_product, df_toxicity_info, type = "daph")
    results_algae = chronic_aquatic_c2c(df_product, df_toxicity_info, type = "algae")

    results_aqua_tox_chronic = (
        results_fish.merge(result_daph, on=["Product", "hom_material"], how="outer")
        .merge(results_algae, on=["Product", "hom_material"], how="outer")
    )
    return results_aqua_tox_chronic
# final c2c aquatic assessment
def final_aquatic_c2c(df_product, df_toxicity_info):
    """3 fully independent per-species outputs: "C2C fish toxicity", "C2C invertebrate
    toxicity" (daphnia) and "C2C algae toxicity". Per explicit confirmation, the 3 species
    do NOT need to agree with each other - each is derived only from its OWN acute+chronic
    worst-case, never from a combined/shared verdict across species (the previous single
    "C2C Acute and Chronic Aquatic Toxicity" column, which forced all 3 species to agree on
    one rating, has been removed)."""
    results_acute = final_acute_aquatic_c2c(df_product, df_toxicity_info)
    results_chronic = final_chronic_aquatic_c2c(df_product, df_toxicity_info)
    df = results_acute.merge(results_chronic, on=["Product", "hom_material"], how="outer")

    # Combine ACUTE and CHRONIC (for the SAME species) by worst-case (RED > GREY > YELLOW >
    # GREEN), the same priority order used everywhere else in this file to combine
    # sub-ratings (sub-endpoint combination in corr_n_irr/sensitization, etc.). Figure 8 in
    # the methodology document (p.29) draws this as acute GREEN/RED/GREY locking in the
    # final rating unconditionally, with chronic only consulted when acute == YELLOW - but
    # per explicit confirmation, that gated reading is NOT the intended rule: chronic data
    # must be able to escalate the result even when acute is GREEN (a substance can be
    # acutely harmless yet chronically hazardous - e.g. persistent/bioaccumulative - and the
    # methodology's own text says chronic data "should be considered" whenever available,
    # not only when acute is YELLOW). Worst-case combination is the conservative choice here.
    priority = {'RED': 0, 'GREY': 1, 'YELLOW': 2, 'GREEN': 3}

    def _worst_of(acute, chronic):
        if pd.isna(chronic):
            return acute
        if pd.isna(acute):
            return chronic
        return acute if priority.get(acute, 99) <= priority.get(chronic, 99) else chronic

    df["C2C fish toxicity"] = df.apply(
        lambda r: _worst_of(r.get("fish aquatic acute tox"), r.get("fish aquatic chronic tox")), axis=1
    )
    df["C2C invertebrate toxicity"] = df.apply(
        lambda r: _worst_of(r.get("daph aquatic acute tox"), r.get("daph aquatic chronic tox")), axis=1
    )
    df["C2C algae toxicity"] = df.apply(
        lambda r: _worst_of(r.get("algae aquatic acute tox"), r.get("algae aquatic chronic tox")), axis=1
    )

    return df
### All C2C assessments at once ###
def mixture_rules_C2C_assessment(df_product, df_toxicity_info):
    """Run all 4 additive C2C mixture-rule endpoint groups (acute toxicity, corrosion/irritation, sensitization, aquatic toxicity) per (Product, Homogeneous Material), each wrapped so a failure degrades to an empty/placeholder result instead of crashing the pipeline, merge them into one summary with the not-full-composition label applied to aquatic columns, and attach a diagnostic column listing chemicals with unknown ATE data."""

    def safe_run(func, name, fallback):
        """Helper: run function safely and never crash pipeline."""
        try:
            return func()
        except Exception as e:
            print(f"WARNING {name} failed: {e}")
            return fallback

    # ---- EXPECTED OUTPUT STRUCTURE (fallbacks) ----
    empty_acute = pd.DataFrame(columns=["Product", "hom_material", "C2C oral toxicity", "C2C dermal toxicity", "C2C inhalative toxicity"])
    empty_corr = pd.DataFrame(columns=["Product", "hom_material", "C2C skin eye respiratory corrosion irritation"])
    empty_sens = pd.DataFrame(columns=["Product", "hom_material", "C2C sensitization"])
    empty_aqua = pd.DataFrame(columns=["Product", "hom_material", "C2C fish toxicity", "C2C invertebrate toxicity", "C2C algae toxicity"])
    empty_unknown = pd.DataFrame(columns=["Product", "hom_material"])

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
            .merge(corr_n_irr_C2C_df, on=["Product", "hom_material"], how="outer")
            .merge(sens_C2C_df, on=["Product", "hom_material"], how="outer")
            .merge(final_aquatic_results, on=["Product", "hom_material"], how="outer")
        )
    except Exception as e:
        print(f"[WARNING] Final merge failed: {e}")
        return empty_acute.copy()

    # Cleaning the summary - the 8 mixture-rule-capable endpoints, fully independent of
    # each other (oral/dermal/inhalative acute toxicity; fish/invertebrate/algae aquatic
    # toxicity; the still-combined corrosion/irritation and sensitization columns).
    endpoint_cols = [
        "C2C oral toxicity",
        "C2C dermal toxicity",
        "C2C inhalative toxicity",
        "C2C skin eye respiratory corrosion irritation",
        "C2C sensitization",
        "C2C fish toxicity",
        "C2C invertebrate toxicity",
        "C2C algae toxicity",
    ]
    try:
        final_c2c_results_summary = final_c2c_results[["Product", "hom_material"] + endpoint_cols].copy()
    except Exception as e:
        print(f"WARNING Column selection failed: {e}")
        final_c2c_results_summary = final_c2c_results[["Product", "hom_material"]].copy()

    # Unknown composition/CAS covers acute toxicity, corrosion/irritation and sensitization
    # already (each overrides its own column internally) - aquatic toxicity doesn't have that
    # check yet, so apply it here for those 3 columns specifically.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    final_c2c_results_summary = _apply_not_full_composition_label(
        final_c2c_results_summary, incomplete_hom_materials,
        ["C2C fish toxicity", "C2C invertebrate toxicity", "C2C algae toxicity"]
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
### unchanged, and merges in the no-mixture-rules endpoints (assessment_with_no_mixture_rules).
def mixture_rules_C2C_assessment_from_db(df_product, db_path):
    """Build toxicity info straight from the SQLite database (fetching the raw colour-assessment rows once and reusing them), run `mixture_rules_C2C_assessment`'s 4 additive endpoint groups, merge in the no-mixture-rules endpoints, and apply the worst-raw-colour fallback for (Product, Hom Mat) pairs whose additive calculation could not produce a trustworthy value."""
    cas_list = df_product["CAS"].unique().tolist()
    # Fetched once and threaded through build_mixture_rules_toxicity_info_from_db,
    # assessment_with_no_mixture_rules, and the incomplete-composition fallback below -
    # all three need the exact same COLOUR_ASSESSMENT_C2C rows for this cas_list, so
    # querying it 3 times (as this used to) is pure redundant DB round-trips, multiplied
    # by however many scenarios analyse_the_dataset_with_mixture_rules calls this from.
    colour_df, _ = extract_colour_assessment_C2C(cas_list, db_path)
    df_toxicity_info = build_mixture_rules_toxicity_info_from_db(cas_list, db_path, colour_df=colour_df)

    base_result = mixture_rules_C2C_assessment(df_product, df_toxicity_info)

    # The no-mixture-rules assessment is not individually safe_run-wrapped internally
    # (unlike each of the 4 groups inside mixture_rules_C2C_assessment above) - a DB
    # hiccup or schema surprise here must not take down the otherwise-valid,
    # already-computed base_result for this scenario. Degrade to NOT_ENOUGH_INFO_LABEL
    # for this assessment's own columns only.
    try:
        no_mixture_rules_result = assessment_with_no_mixture_rules(
            df_product, cas_list, db_path, colour_df=colour_df, toxicity_info_df=df_toxicity_info
        )
    except Exception as e:
        print(f"WARNING assessment_with_no_mixture_rules failed: {e}")
        product_hom_pairs = df_product[["Product", "Homogenous Material"]].drop_duplicates()
        no_mixture_rules_result = pd.DataFrame({
            "Product": product_hom_pairs["Product"].tolist(),
            "hom_material": product_hom_pairs["Homogenous Material"].tolist(),
            **{f"C2C {label}": NOT_ENOUGH_INFO_LABEL for label in NO_MIXTURE_RULES_ENDPOINTS.values()},
        })

    try:
        result = base_result.merge(no_mixture_rules_result, on=["Product", "hom_material"], how="outer")
    except Exception as e:
        print(f"WARNING Could not merge no-mixture-rules assessment results: {e}")
        result = base_result

    # Fallback for the 8 mixture-rule-capable endpoints: wherever the additive calculation
    # could not produce a trustworthy value for a (Product, Hom Mat) - unknown composition,
    # or no usable hazard data at all for that endpoint - replace the plain
    # NOT_FULL_COMPOSITION_LABEL/NaN with the worst individual raw colour among that (Product,
    # Hom Mat)'s own chemicals instead (see _apply_incomplete_comp_fallback's docstring).
    try:
        fallback_colour_df = colour_df.rename(columns={
            "C2C_assessment_oral_toxicity": "oral toxicity C2C assessment",
            "C2C_assessment_inhalative_toxicity": "inhalative toxicity C2C assessment",
            "C2C_assessment_dermal_toxicity": "dermal toxicity C2C assessment",
            "C2C_assessment_skin_eye_respiratory_corrosion_irritation": "skin eye respiratory corrosion irritation C2C assessment",
            "C2C_assessment_sensitization": "sensitization C2C assessment",
            "C2C_assessment_fish_toxicity": "fish toxicity C2C assessment",
            "C2C_assessment_invertebrate_toxicity": "invertebrate toxicity C2C assessment",
            "C2C_assessment_algae_toxicity": "algae toxicity C2C assessment",
        })
        result = _apply_incomplete_comp_fallback(result, df_product, fallback_colour_df)
    except Exception as e:
        print(f"WARNING Could not apply incomplete-composition fallback: {e}")

    return result
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
    """Query the SQLite database per CAS number for acute toxicity, corrosion/irritation, sensitization, aquatic toxicity and SCL data (combining automated and manual ratings, logging any missing records/tables), returning the combined per-CAS toxicity info dataframe and a dataframe of missing-data log entries."""
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
def build_mixture_rules_toxicity_info_from_db(cas_list, db_path, colour_df=None):
    """
    `colour_df` lets a caller that already fetched extract_colour_assessment_C2C(cas_list,
    db_path) for this same cas_list (e.g. mixture_rules_C2C_assessment_from_db, which also
    needs it for assessment_with_no_mixture_rules and the incomplete-composition fallback)
    pass it in instead of this function re-querying COLOUR_ASSESSMENT_C2C itself. Pass
    nothing (the default) to fetch it here as before.

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
        "fish toxicity C2C assessment", "invertebrate toxicity C2C assessment", "algae toxicity C2C assessment",
    ]
    empty_columns += [f"SCL - {label} - value" for label in NO_MIXTURE_RULES_ENDPOINTS.values()]
    empty_columns += [f"SCL - {label} -> Yes / No" for label in NO_MIXTURE_RULES_ENDPOINTS.values()]

    cas_list = clean_cas_values(cas_list)
    if not cas_list:
        return pd.DataFrame(columns=empty_columns)

    if colour_df is None:
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

    # No-mixture-rules endpoints (assessment_with_no_mixture_rules): detect, per endpoint,
    # whether SCONCLIM actually defines an SCL for it - by the same "<label> - Lower/Upper
    # Limit: (%)" naming convention as the sensitization SCLs above - and expose it per CAS
    # as a Yes/No flag plus the actual %-value (min of lower/upper, blank if neither
    # exists). "Yes" here is necessarily per-CAS: a column only "exists" for an endpoint
    # that ARCHE has entered at least one row for, but any given CAS may still have no
    # value of its own in it. assessment_with_no_mixture_rules reads these same value
    # columns for its per-substance relevance check, so the detailed_overview record and
    # the calculation are guaranteed to agree.
    for label in NO_MIXTURE_RULES_ENDPOINTS.values():
        lower_col = f"{label} - Lower Limit: (%)"
        upper_col = f"{label} - Upper Limit: (%)"
        present_cols = [c for c in [lower_col, upper_col] if c in df.columns]
        value_col = f"SCL - {label} - value"
        flag_col = f"SCL - {label} -> Yes / No"
        if present_cols:
            df[value_col] = pd.to_numeric(df[present_cols].min(axis=1), errors="coerce")
        else:
            df[value_col] = np.nan
        df[flag_col] = np.where(df[value_col].notna(), "Yes", "No")

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
                "C2C_assessment_fish_toxicity": "fish toxicity C2C assessment",
                "C2C_assessment_invertebrate_toxicity": "invertebrate toxicity C2C assessment",
                "C2C_assessment_algae_toxicity": "algae toxicity C2C assessment",
            })[[
                "CAS",
                "oral toxicity C2C assessment",
                "inhalative toxicity C2C assessment",
                "dermal toxicity C2C assessment",
                "skin eye respiratory corrosion irritation C2C assessment",
                "sensitization C2C assessment",
                "fish toxicity C2C assessment",
                "invertebrate toxicity C2C assessment",
                "algae toxicity C2C assessment",
            ]],
            on="CAS", how="left",
        )

    return df


### Assessment with no mixture rules: every C2C hazard endpoint NOT covered by the
### additive mixture-rule functions above (acute mammalian toxicity; skin/eye/respiratory
### irritation; skin/respiratory sensitization; aquatic toxicity). Per the methodology
### (section 1.5/2.2), CLP/GHS itself does not apply additive summation to Carcinogenicity,
### Germ Cell Mutagenicity, Reproductive Toxicity, or STOT - there is no scientific basis
### for assuming dilution reduces hazard for these endpoints. C2C extends the same non-
### additive treatment to every other endpoint in its 21-endpoint hazard list.
NOT_ENOUGH_INFO_LABEL = "NOT ENOUGH INFO TO CALCULATE - NO MIXTURE RULES APPLIED"

NO_MIXTURE_RULES_ENDPOINTS = {
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
_NO_MIXTURE_RULES_RANK = {"GREEN": 1, "YELLOW": 2, "GREY": 3, "RED": 4}

def assessment_with_no_mixture_rules(df_product, cas_list, db_path, colour_df=None, toxicity_info_df=None):
    """
    Non-additive C2C mixture rule for every "no mixture rules" endpoint: per homogeneous
    material, a chemical is "relevant" if:
    - this endpoint has its OWN SCL for that specific chemical (SCONCLIM's
      "<label> - Lower/Upper Limit: (%)" columns, detected dynamically - see
      build_mixture_rules_toxicity_info_from_db's "SCL - <label> - value"/"SCL - <label> ->
      Yes / No" columns), in which case the chemical's concentration is compared ONLY
      against that SCL (never against the flat cut-off too - an SCL replaces it, it doesn't
      add another way in);
    - otherwise (no SCL exists for this endpoint at all, or this particular chemical has
      none of its own even though the endpoint does for others), the flat 0.01% cut-off
      applies instead.
    The hom mat's rating is the WORST rating among its relevant chemicals. A relevant
    chemical with no usable rating for that endpoint is treated as GREY rather than
    invalidating the whole hom mat's result for that endpoint.

    `colour_df` lets a caller that already fetched extract_colour_assessment_C2C(cas_list,
    db_path) for this same cas_list pass it in instead of re-querying the DB here - see
    build_mixture_rules_toxicity_info_from_db's identical parameter for the rationale.
    `toxicity_info_df` similarly lets a caller that already built
    build_mixture_rules_toxicity_info_from_db(cas_list, db_path) pass it in (it's where the
    "SCL - <label> - value" columns this function reads come from) instead of rebuilding it
    here.
    """
    if colour_df is None:
        colour_df, _ = extract_colour_assessment_C2C(cas_list, db_path)
    if toxicity_info_df is None:
        toxicity_info_df = build_mixture_rules_toxicity_info_from_db(cas_list, db_path, colour_df=colour_df)

    d = df_product.copy()
    d["conc_hom_mat"] = d[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    d = d.merge(colour_df, on="CAS", how="left")

    scl_value_cols = [f"SCL - {label} - value" for label in NO_MIXTURE_RULES_ENDPOINTS.values()]
    scl_value_cols = [c for c in scl_value_cols if c in toxicity_info_df.columns]
    if scl_value_cols:
        d = d.merge(toxicity_info_df[["CAS"] + scl_value_cols], on="CAS", how="left")

    # (Product, Homogenous Material) pairs, not hom-mat name alone
    product_hom_pairs = list(
        df_product[["Product", "Homogenous Material"]].drop_duplicates().itertuples(index=False, name=None)
    )
    rows = []
    for product_val, hom_material in product_hom_pairs:
        sub = d.loc[(d["Product"] == product_val) & (d["Homogenous Material"] == hom_material)]
        record = {"Product": product_val, "hom_material": hom_material}

        for colour_col, label in NO_MIXTURE_RULES_ENDPOINTS.items():
            out_col = f"C2C {label}"

            base_relevant = (sub["CAS"] != "not assessed") & sub["conc_hom_mat"].notna()
            value_col = f"SCL - {label} - value"
            if value_col in sub.columns:
                own_scl_fraction = pd.to_numeric(sub[value_col], errors="coerce") / 100.0
                has_own_scl = own_scl_fraction.notna()
                threshold_met = (
                    (has_own_scl & (sub["conc_hom_mat"] >= own_scl_fraction))
                    | (~has_own_scl & (sub["conc_hom_mat"] >= 0.0001))
                )
            else:
                threshold_met = sub["conc_hom_mat"] >= 0.0001
            relevant_mask = base_relevant & threshold_met

            relevant = sub.loc[relevant_mask]

            if relevant.empty:
                record[out_col] = "GREEN"
                continue

            # A chemical with no usable rating for this endpoint (missing from the DB,
            # blank, or an unrecognised value) is treated as GREY rather than degrading
            # the whole hom mat's result to NOT_ENOUGH_INFO_LABEL - the same "missing =
            # GREY" convention _worst_case_raw_colour was factored out for, reused here so
            # both places share one implementation.
            record[out_col] = _worst_case_raw_colour(relevant, colour_col)

        rows.append(record)

    result_df = pd.DataFrame(rows)

    # Unknown composition/CAS (the same completeness gate used by every other endpoint
    # group) also invalidates this assessment's results for that hom mat.
    incomplete_hom_materials = _hom_materials_with_unknown_composition(df_product)
    endpoint_cols = [f"C2C {label}" for label in NO_MIXTURE_RULES_ENDPOINTS.values()]
    result_df = _apply_not_full_composition_label(result_df, incomplete_hom_materials, endpoint_cols)
    return result_df


### Every per-tier running-% column calculate_row_contributions() may have added, in a
### stable (tier ascending, prod before hom_mat, min before max) order - so the detailed
### overview always lists them the same way regardless of dict/column ordering.
_TIER_CONTRIBUTION_PATTERN = re.compile(r"^(min|max)_contribution_(prod|hom_mat)_t(\d+)$")


def _sorted_tier_contribution_cols(columns):
    """Return the per-tier contribution columns (matching `_TIER_CONTRIBUTION_PATTERN`) from `columns`, ordered by tier number then prod-before-hom_mat then min-before-max."""
    def sort_key(col):
        m = _TIER_CONTRIBUTION_PATTERN.match(col)
        minmax, kind, tier = m.group(1), m.group(2), int(m.group(3))
        return (tier, kind != "prod", minmax != "min")

    return sorted((c for c in columns if _TIER_CONTRIBUTION_PATTERN.match(c)), key=sort_key)


def _tier_contribution_rename_map(tier_cols):
    """Build a {raw column name: human-readable label} map for per-tier contribution columns, e.g. "min_contribution_prod_t2" -> "Minimal % of material in product after Tier 2"."""
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
def build_c2c_assessment_df(scenarios_df, db_path, include_mixture_rule_db_details=False):
    """
    Take a scenarios df (all or selected scenarios) and keep only the
    product/material/contribution columns, then join the C2C colour
    assessment hazards pulled from COLOUR_ASSESSMENT_C2C for the CAS
    numbers present in it.

    Pass include_mixture_rule_db_details=True (mixture rules pipeline only) to also merge in
    every other per-CAS raw value the additive mixture-rule calculation reads from the DB -
    LD50/LC50 measurements, CLP classification text, sensitisation/aquatic classification
    text, M-factor, and every SCONCLIM SCL column (blank for a CAS/endpoint with none) - via
    build_mixture_rules_toxicity_info_from_db. Placed right after the 21 hazard colour
    columns and before the per-tier running-% columns, so the detailed_overview carries a
    full record of the values the calculation actually used.
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

    if include_mixture_rule_db_details:
        toxicity_info_df = build_mixture_rules_toxicity_info_from_db(cas_list, db_path, colour_df=hazards_df)
        # The 8 "<endpoint> C2C assessment" columns duplicate the hazard block already
        # merged in above (just under a different naming convention) - drop them here so
        # each raw colour appears exactly once in the output.
        duplicate_cols = [c for c in toxicity_info_df.columns if c.endswith(" C2C assessment")]
        toxicity_info_df = toxicity_info_df.drop(columns=duplicate_cols, errors="ignore")
        c2c_df = c2c_df.merge(toxicity_info_df, on="CAS", how="left")

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


def build_percent_assessed_detailed_df(scenarios_df):
    """Same per-CAS row shape/column names as build_c2c_assessment_df, but with NO database
    lookup at all - used by the no-DB "Percent Assessed" pipeline's detailed_overview,
    which only ever needs the composition/contribution columns, never hazard colours."""
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
    df = scenarios_df[base_cols].copy()

    tier_cols = _sorted_tier_contribution_cols(scenarios_df.columns)
    for col in tier_cols:
        df[col] = scenarios_df[col].values

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
    return df.rename(columns=rename_map)

# --- Unused: no remaining callers as of 2026-09 cleanup (only ever called from
# --- save_c2c_assessment_workbook, also commented out below), kept for reference ---
# ### Save a C2C assessment df into the "detailed_overview" sheet of the C2C assessment template
# ### Generate formula rows 3..target_last_row on a summary sheet by translating its row-2 "origin" formula
# # matches the hardcoded detailed_overview scan range in the template's formulas,
# # e.g. "$A$2:$A$50000" or "$I$2:$I$100000" -> group(1) keeps the "$COL$2:$COL$" part
# _SCAN_RANGE_PATTERN = re.compile(r"(\$[A-Za-z]{1,3}\$2:\$[A-Za-z]{1,3}\$)(?:50000|100000)")
#
# def _extend_formula_sheet(ws, target_last_row, scan_last_row):
#     if target_last_row < 2:
#         return
#
#     # columns that carry the origin formula in row 2 (skips blank spacer columns)
#     formula_cols = [c for c in range(1, ws.max_column + 1) if ws.cell(row=2, column=c).value is not None]
#
#     for col in formula_cols:
#         origin_cell = ws.cell(row=2, column=col)
#         origin_val = origin_cell.value
#         origin_text = origin_val.text if hasattr(origin_val, "text") else origin_val
#         origin_coord = origin_cell.coordinate
#         origin_style = origin_cell._style
#
#         # shrink the detailed_overview scan range to match the actual project size instead
#         # of always scanning the template's full 50000/100000-row headroom - this is the
#         # main thing that makes the summary sheets slow (or crash Excel) on large projects
#         origin_text = _SCAN_RANGE_PATTERN.sub(rf"\g<1>{scan_last_row}", origin_text)
#         origin_cell.value = ArrayFormula(ref=origin_coord, text=origin_text)
#
#         # parse the formula once, then cheaply re-translate it for every target row
#         translator = Translator(origin_text, origin=origin_coord)
#
#         for row in range(3, target_last_row + 1):
#             target_cell = ws.cell(row=row, column=col)
#             target_coord = target_cell.coordinate
#             translated = translator.translate_formula(target_coord)
#             target_cell.value = ArrayFormula(ref=target_coord, text=translated)
#             target_cell._style = origin_style

# --- Unused: no remaining callers as of 2026-09 cleanup - run_c2c_assessment_only() and
# --- run_wint_C2C_mixture_rules()'s "save selected scenarios" branch were both migrated to
# --- save_c2c_assessment_output (plain values, auto-splitting, no row cap). Kept for reference. ---
# def save_c2c_assessment_workbook(c2c_df, output_path, template_path=C2C_ASSESSMENT_TEMPLATE_PATH):
#     """
#     Copy templates/C2C_assessment_template.xlsx to output_path and write
#     c2c_df into its "detailed_overview" sheet (starting row 2). The
#     template's other sheets ("overview", "percentage_assessed",
#     "risk_assessed") ship with a single formula row (row 2); this
#     generates however many extra formula rows this project needs
#     (matched to len(c2c_df), capped at C2C_ASSESSMENT_TEMPLATE_MAX_ROWS)
#     AND shrinks each formula's detailed_overview scan range to match
#     (plus a small buffer) instead of always scanning the template's full
#     50000/100000-row range, so small/medium projects stay fast (and large
#     ones don't crash Excel) to recalculate. They then recalculate
#     automatically once the file is opened.
#     """
#     if not os.path.exists(template_path):
#         raise FileNotFoundError(
#             f"C2C assessment template not found at: {template_path}\n"
#             "Check C2C_ASSESSMENT_TEMPLATE_PATH at the top of this file."
#         )
#
#     n_rows = len(c2c_df)
#     if n_rows > C2C_ASSESSMENT_TEMPLATE_MAX_ROWS:
#         print(
#             f"[WARNING] {n_rows} rows exceed the template's {C2C_ASSESSMENT_TEMPLATE_MAX_ROWS}-row cap - "
#             "the summary sheets will be incomplete for the extra rows."
#         )
#         n_rows = C2C_ASSESSMENT_TEMPLATE_MAX_ROWS
#
#     shutil.copy(template_path, output_path)
#
#     wb = openpyxl.load_workbook(output_path)
#     ws = wb["detailed_overview"]
#
#     template_headers = [cell.value for cell in ws[1] if cell.value is not None]
#     df_headers = list(c2c_df.columns)
#     if df_headers[: len(template_headers)] != template_headers:
#         print(
#             "[WARNING] detailed_overview headers no longer match the template.\n"
#             f"  template: {template_headers}\n"
#             f"  data:     {df_headers}\n"
#             "The 'overview' / 'percentage_assessed' / 'risk_assessed' formulas read fixed "
#             "columns and may now point at the wrong data - update the template."
#         )
#     elif len(df_headers) > len(template_headers):
#         # Extra trailing columns beyond the template's own headers (e.g. the per-tier %
#         # tracking columns) - safe to write, since nothing reads past the template's own
#         # columns by fixed letter; the template just has no header cells for them yet.
#         for col_offset, header in enumerate(df_headers[len(template_headers):], start=len(template_headers) + 1):
#             ws.cell(row=1, column=col_offset, value=header)
#
#     for row_offset, row in enumerate(c2c_df.itertuples(index=False), start=2):
#         for col_offset, value in enumerate(row, start=1):
#             ws.cell(row=row_offset, column=col_offset, value=None if pd.isna(value) else value)
#
#     # generate as many summary-sheet formula rows as this project needs (row 2 already ships in the template),
#     # and shrink each formula's detailed_overview scan range to match (plus a little headroom) instead of
#     # always scanning the template's full 50000/100000-row range - this is what actually kills Excel on
#     # large projects, since every formula cell re-scans that whole range
#     target_last_row = n_rows + 1 if n_rows >= 1 else 2
#     scan_last_row = target_last_row + C2C_ASSESSMENT_SCAN_RANGE_BUFFER
#     for sheet_name in ("overview", "percentage_assessed", "risk_assessed"):
#         _extend_formula_sheet(wb[sheet_name], target_last_row, scan_last_row)
#
#     # force Excel to recalculate the formula sheets when the file is opened
#     wb.calculation.fullCalcOnLoad = True
#
#     wb.save(output_path)


### New "overview"/"percentage_assessed"/"risk_assessed" builders (item 6), copied from
### MAS_quick_C2C_assessment_static.py (this project's convention: each app folder is
### self-contained, so shared helpers are copied rather than imported across folders), and
### adapted for the mixture-rules dataset. Fed from the "active_scaffold_df" built inside
### analyse_the_dataset_with_mixture_rules (Product/Homogenous Material/Scenario ID/Scenario
### Status/CAS/%-contributions/chemical-class raw columns + the mixture-rule-COMPUTED 8
### endpoint values broadcast per (Product, Hom Mat)) - NOT the per-CAS RAW-colour dataframe
### used for detailed_overview (build_c2c_assessment_df), which stays untouched (item 5).
MIXTURE_RULES_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "C2C_Quick_assessment_program", "templates",
    "C2C_assessment_template.xlsx",
)

DETAILED_OVERVIEW_ROW_CAP = 50000

# Order matches the 21 columns produced by extract_colour_assessment_C2C()/
# build_c2c_assessment_df(), i.e. detailed_overview columns N..AH - kept in sync with
# MAS_quick_C2C_assessment_static.py's own HAZARD_COLS_READABLE.
HAZARD_COLS_READABLE = [
    "C2C assessment carcinogenicity",
    "C2C assessment disruption of endocrine system",
    "C2C assessment mutagenicity genotoxicity",
    "C2C assessment reproductive toxicity",
    "C2C assessment development toxicity",
    "C2C assessment neurotoxicity",
    "C2C assessment oral toxicity",
    "C2C assessment inhalative toxicity",
    "C2C assessment dermal toxicity",
    "C2C assessment skin eye respiratory corrosion irritation",
    "C2C assessment sensitization",
    "C2C assessment fish toxicity",
    "C2C assessment invertebrate toxicity",
    "C2C assessment algae toxicity",
    "C2C assessment terrestrial toxicity",
    "C2C assessment other species toxicity",
    "C2C assessment persistence",
    "C2C assessment bioaccumulation",
    "C2C assessment combined pb risk flag",
    "C2C assessment combined aquatic risk flag",
    "C2C assessment climatic relevance ozone depletion potential",
]

# "overview" sheet's paired "Scenario ID_<suffix>" column per hazard, same order
SCENARIO_ID_SUFFIXES = [
    "carcinogenicity", "endocrine", "mutagenicity", "reproductive", "development",
    "neurotoxicity", "oral", "inhalative", "dermal", "skin_eye", "sensitization",
    "fish", "invertebrate", "algae", "terrestrial", "other_species", "persistence",
    "bioaccumulation", "combined_pb", "combined_aquatic", "ozone",
]

# Mapping from the mixture-rules pipeline's own output column names (used inside
# active_scaffold_df) to the HAZARD_COLS_READABLE names build_overview_df/
# build_percentage_assessed_df/build_risk_assessed_df group over - the 8 mixture-rule-capable
# endpoints only; the other 13 (no-mixture-rules) endpoints already use the "C2C <label>"
# convention from NO_MIXTURE_RULES_ENDPOINTS, mapped the same way below.
_MIXTURE_ENDPOINT_TO_READABLE = {
    "C2C oral toxicity": "C2C assessment oral toxicity",
    "C2C dermal toxicity": "C2C assessment dermal toxicity",
    "C2C inhalative toxicity": "C2C assessment inhalative toxicity",
    "C2C skin eye respiratory corrosion irritation": "C2C assessment skin eye respiratory corrosion irritation",
    "C2C sensitization": "C2C assessment sensitization",
    "C2C fish toxicity": "C2C assessment fish toxicity",
    "C2C invertebrate toxicity": "C2C assessment invertebrate toxicity",
    "C2C algae toxicity": "C2C assessment algae toxicity",
}
# The 13 no-mixture-rules endpoints' OWN column names (assessment_with_no_mixture_rules
# produces f"C2C {label}" using NO_MIXTURE_RULES_ENDPOINTS's human-readable labels, e.g.
# "C2C Mutagenicity/Genotoxicity", "C2C Climatic Relevance / Ozone Depletion Potential") do
# NOT line up 1:1 with HAZARD_COLS_READABLE's own text (different wording/punctuation -
# "Developmental" vs "development", a "/" HAZARD_COLS_READABLE omits, etc.), so this mapping
# is spelled out explicitly rather than derived by a text transform.
_MIXTURE_ENDPOINT_TO_READABLE.update({
    "C2C Carcinogenicity": "C2C assessment carcinogenicity",
    "C2C Endocrine Disruption": "C2C assessment disruption of endocrine system",
    "C2C Mutagenicity/Genotoxicity": "C2C assessment mutagenicity genotoxicity",
    "C2C Reproductive Toxicity": "C2C assessment reproductive toxicity",
    "C2C Developmental Toxicity": "C2C assessment development toxicity",
    "C2C Neurotoxicity": "C2C assessment neurotoxicity",
    "C2C Terrestrial Toxicity": "C2C assessment terrestrial toxicity",
    "C2C Other Species Toxicity": "C2C assessment other species toxicity",
    "C2C Persistence": "C2C assessment persistence",
    "C2C Bioaccumulation": "C2C assessment bioaccumulation",
    "C2C Combined PB Risk Flag": "C2C assessment combined pb risk flag",
    "C2C Combined Aquatic Risk Flag": "C2C assessment combined aquatic risk flag",
    "C2C Climatic Relevance / Ozone Depletion Potential": "C2C assessment climatic relevance ozone depletion potential",
})
# Sanity check at import time: every value must be a real HAZARD_COLS_READABLE column, and
# every HAZARD_COLS_READABLE column not in the 8 additive endpoints must be covered here -
# a silent mismatch would otherwise surface only much later as a confusing missing-column
# KeyError inside build_overview_df/build_risk_assessed_df.
assert set(_MIXTURE_ENDPOINT_TO_READABLE.values()) == set(HAZARD_COLS_READABLE), (
    set(_MIXTURE_ENDPOINT_TO_READABLE.values()) ^ set(HAZARD_COLS_READABLE)
)

# The 8 mixture-rule-capable endpoints' readable (HAZARD_COLS_READABLE) column names -
# used by _reattach_incomplete_comp_prefix to know which columns can ever carry the
# INCOMPLETE_COMP_LABEL fallback.
MIXTURE_RULE_CAPABLE_READABLE_COLS = {
    _MIXTURE_ENDPOINT_TO_READABLE[k] for k in MIXTURE_RULE_CAPABLE_ENDPOINTS
}
# The literal text before "{colour}" in each fallback label, used to detect (by prefix
# match) whether a raw hazard value was already in that fallback state, and to re-attach
# the matching label after classify_colour() strips it down to a bare colour.
_FALLBACK_LABEL_PREFIXES = [
    (INCOMPLETE_COMP_LABEL.split("{colour}")[0], INCOMPLETE_COMP_LABEL),
    (NOT_ENOUGH_DB_DATA_LABEL.split("{colour}")[0], NOT_ENOUGH_DB_DATA_LABEL),
]

COLOUR_RANK = {"GREEN": 1, "YELLOW": 2, "GREY": 3, "RED": 4}
RANK_TO_COLOUR = {v: k for k, v in COLOUR_RANK.items()}


def _reattach_incomplete_comp_prefix(active_df, group_cols, hazard_col, group_index, colours):
    """_worst_colour_by_group's `colours` are always a bare GREEN/YELLOW/GREY/RED -
    classify_colour() strips any surrounding text, including whichever fallback prefix
    _apply_incomplete_comp_fallback wrote into the raw hazard value (INCOMPLETE_COMP_LABEL
    for unknown composition, or NOT_ENOUGH_DB_DATA_LABEL for a fully-known composition the
    database just lacks hazard data for). For the 8 mixture-rule-capable endpoints,
    re-attach whichever prefix matches this group's own (broadcast, so identical across
    every CAS row of the group) raw value - otherwise the sheet would silently show a
    plain colour with no indication the additive mixture rule couldn't actually run for
    that endpoint, or which of the two reasons applied."""
    if hazard_col not in MIXTURE_RULE_CAPABLE_READABLE_COLS:
        return colours.values
    upper_values = active_df[hazard_col].astype(str).str.upper()
    tmp = active_df[list(group_cols)].copy()
    # Per group, which fallback label (if any) its raw value carries - checked in order,
    # first match wins (a group's broadcast value is only ever one or the other, never both).
    # A plain dict, not a pandas Series, since pd.Series(None, dtype=object) silently
    # normalizes None to NaN (a float), which then crashes label.format() below.
    label_by_group = {}
    for prefix, label in _FALLBACK_LABEL_PREFIXES:
        matches = upper_values.str.startswith(prefix.upper())
        if not matches.any():
            continue
        tmp["_matches"] = matches.values
        group_matches = tmp.groupby(list(group_cols), sort=False)["_matches"].any()
        for key, matched in group_matches.items():
            if matched and key not in label_by_group:
                label_by_group[key] = label
    return [
        label_by_group[key].format(colour=c) if key in label_by_group else c
        for key, c in zip(group_index, colours.values)
    ]


def _apply_without_mixture_rules_label(hazard_col, colours):
    """Quick-assessment (mixture_rules_ran=False) equivalent of _reattach_incomplete_comp_prefix -
    matches MAS_quick_C2C_assessment_current.py's _display_colour: since mixture rules never ran
    at all here, every one of the 8 mixture-rule-capable endpoints unconditionally gets prefixed,
    regardless of the individual raw value (no INCOMPLETE_COMP_LABEL/NOT_ENOUGH_DB_DATA_LABEL
    fallback state applies - those only exist inside the mixture-rules pipeline)."""
    if hazard_col not in MIXTURE_RULE_CAPABLE_READABLE_COLS:
        return colours.values
    return [f"WITHOUT MIXTURE RULES: {c}" for c in colours.values]

# Plain worst-case: GREY is a real, competing state for these endpoints.
OVERALL_RATING_STANDARD_ENDPOINTS = [
    "C2C assessment mutagenicity genotoxicity",
    "C2C assessment oral toxicity",
    "C2C assessment inhalative toxicity",
    "C2C assessment dermal toxicity",
    "C2C assessment skin eye respiratory corrosion irritation",
    "C2C assessment sensitization",
    "C2C assessment combined aquatic risk flag",
]

# Worst-case among RED/YELLOW/GREEN only - GREY never competes.
OVERALL_RATING_GREY_IGNORED_ENDPOINTS = [
    "C2C assessment carcinogenicity",
    "C2C assessment disruption of endocrine system",
    "C2C assessment neurotoxicity",
    "C2C assessment terrestrial toxicity",
    "C2C assessment other species toxicity",
    "C2C assessment climatic relevance ozone depletion potential",
]

# --- Unused: no remaining references as of 2026-09 cleanup (the coupled pair is
# --- hardcoded directly in _resolve_coupled_pair/_overall_c2c_rating instead), kept for reference ---
# # Coupled pair - resolved to a single effective colour before competing.
# OVERALL_RATING_COUPLED_ENDPOINTS = (
#     "C2C assessment reproductive toxicity",
#     "C2C assessment development toxicity",
# )
#
# # Never influence the overall rating at all.
# OVERALL_RATING_EXCLUDED_ENDPOINTS = [
#     "C2C assessment fish toxicity",
#     "C2C assessment invertebrate toxicity",
#     "C2C assessment algae toxicity",
#     "C2C assessment persistence",
#     "C2C assessment bioaccumulation",
#     "C2C assessment combined pb risk flag",
# ]

COL_OVERALL_RATING = "Overall C2C Material Health Rating"
COL_OVERALL_RATING_COMMENT = "Overall C2C Material Health Rating Comment"


def _resolve_coupled_pair(reproductive_colour, development_colour):
    """Reproductive + development toxicity are coupled: any RED wins outright; two GREYs
    stay GREY; a GREY paired with a real colour defers entirely to that real colour; two
    real (non-grey, non-red) colours take their own worst case. Returns (resolved_colour,
    [endpoint names that actually carry that resolved colour])."""
    pair = {
        "C2C assessment reproductive toxicity": reproductive_colour,
        "C2C assessment development toxicity": development_colour,
    }
    if reproductive_colour == "RED" or development_colour == "RED":
        resolved = "RED"
    elif reproductive_colour == "GREY" and development_colour == "GREY":
        resolved = "GREY"
    elif reproductive_colour == "GREY" or development_colour == "GREY":
        resolved = development_colour if reproductive_colour == "GREY" else reproductive_colour
    else:
        resolved = "YELLOW" if "YELLOW" in (reproductive_colour, development_colour) else "GREEN"
    contributing = [name for name, colour in pair.items() if colour == resolved]
    return resolved, contributing


def _overall_c2c_rating(colours_by_endpoint):
    """colours_by_endpoint: dict of {hazard column name -> raw worst-case colour} for one
    (Product, Hom Mat). See MAS_quick_C2C_assessment_static.py's identical function for the
    full rationale of the 3 exceptions to plain worst-case."""
    contributions = {}

    for endpoint in OVERALL_RATING_STANDARD_ENDPOINTS:
        contributions[endpoint] = colours_by_endpoint.get(endpoint)

    for endpoint in OVERALL_RATING_GREY_IGNORED_ENDPOINTS:
        colour = colours_by_endpoint.get(endpoint)
        if colour != "GREY":
            contributions[endpoint] = colour

    reproductive = colours_by_endpoint.get("C2C assessment reproductive toxicity")
    development = colours_by_endpoint.get("C2C assessment development toxicity")
    resolved_pair, pair_contributors = _resolve_coupled_pair(reproductive, development)
    for endpoint in pair_contributors:
        contributions[endpoint] = resolved_pair

    valid = {ep: c for ep, c in contributions.items() if c in COLOUR_RANK}
    if not valid:
        return "GREY", "No data available to determine the overall rating."

    worst_rank = max(COLOUR_RANK[c] for c in valid.values())
    worst_colour = RANK_TO_COLOUR[worst_rank]
    causing = sorted(ep.replace("C2C assessment ", "") for ep, c in valid.items() if COLOUR_RANK[c] == worst_rank)

    if worst_colour == "GREEN":
        comment = "Overall assessment is green - no endpoint indicates a higher hazard."
    else:
        comment = f"Overall assessment is {worst_colour.lower()} due to " + " and ".join(causing) + "."
    return worst_colour, comment


# active_scaffold_df column names (post analyse_the_dataset_with_mixture_rules renaming)
COL_PRODUCT = product
COL_HOM_MAT = hom_mat
COL_SCENARIO_ID = "Scenario ID"
COL_ACTIVE = "Scenario Status"
COL_CAS = "CAS"
COL_MIN_PROD = "Minimal % of material in product"
COL_MAX_PROD = "Maximal % of material in product"
COL_MIN_HOM = "Minimal % of material in homogenous material"
COL_MAX_HOM = "Maximal % of material in homogenous material"
COL_MIN_PCT_HOMMAT_IN_PROD = min_percent_in_product
COL_MAX_PCT_HOMMAT_IN_PROD = max_percent_in_product
COL_FINAL_MATERIAL_MAP = "Final Material Map"


def classify_colour(value):
    """Same substring-match convention as the template's SEARCH("RED"/"YELLOW"/"GREEN",...)
    cascade: default GREY (covers both a genuinely-computed GREY and the
    INCOMPLETE COMP/NOT ENOUGH INFO labels, which are intentionally left uncoloured red/
    yellow/green but still need a rank for worst-case comparisons)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        text = ""
    else:
        text = str(value).upper()
    if "RED" in text:
        return "RED"
    if "YELLOW" in text:
        return "YELLOW"
    if "GREEN" in text:
        return "GREEN"
    return "GREY"


def _join_unique(values):
    """Join non-empty, non-None values into a single comma-separated string, preserving first-seen order and dropping duplicates."""
    return ", ".join(dict.fromkeys(v for v in values if v is not None and v != ""))


def _pct_assessed_by_group(active_df, group_cols, min_col, max_col, group_index):
    """MIN(1 - sum(min_col over 'not assessed' rows), 1 - sum(max_col over 'not assessed' rows)), per group."""
    not_assessed = active_df[active_df[COL_CAS] == "not assessed"]
    sum_min = not_assessed.groupby(group_cols, sort=False)[min_col].sum()
    sum_max = not_assessed.groupby(group_cols, sort=False)[max_col].sum()
    sum_min = sum_min.reindex(group_index, fill_value=0)
    sum_max = sum_max.reindex(group_index, fill_value=0)
    result = pd.concat([1 - sum_min, 1 - sum_max], axis=1).min(axis=1)
    return result.round(10)


# CHEMICALCLASS columns to surface a worst-case "Contains X?" flag for in risk_assessed and
# overview, and the reader-facing label for each.
CHEMICAL_CLASS_RISK_FLAGS = {
    "Organohalogen": "Contains organohalogens",
    "Toxic metal": "Contains toxic metals",
    "SVHC": "SVHC",
}
CHEMICAL_CLASS_COLS = ["Harmonized", "Organohalogen", "Toxic metal", "SVHC"]

_OVERVIEW_SPACER_COL = ""
NOT_SUFFICIENT_DATA_LABEL = "manual assessment needed"
PCT_ASSESSED_FLAG_OK = "OK"


def extract_chemical_class(cas_list, db_path):
    """Pull the Harmonized/Organohalogen/Toxic metal/SVHC chemical-class flags from
    CHEMICALCLASS for the given CAS numbers - copied unchanged from
    MAS_quick_C2C_assessment_static.py."""
    cas_list = clean_cas_values(cas_list)

    if not cas_list:
        return pd.DataFrame(columns=["CAS"] + CHEMICAL_CLASS_COLS)

    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        print(f"[ERROR] Cannot connect to DB: {e}")
        return pd.DataFrame(columns=["CAS"] + CHEMICAL_CLASS_COLS)

    selected_cols_sql = ", ".join([f'"{c}"' for c in ["ref"] + CHEMICAL_CLASS_COLS])
    placeholders = ", ".join(["?"] * len(cas_list))

    try:
        query = f'''
        SELECT {selected_cols_sql}
        FROM CHEMICALCLASS
        WHERE ref IN ({placeholders})
        '''
        df = pd.read_sql_query(query, conn, params=tuple(cas_list))
    except Exception as e:
        print(f"[ERROR] Cannot query CHEMICALCLASS: {e}")
        conn.close()
        return pd.DataFrame(columns=["CAS"] + CHEMICAL_CLASS_COLS)

    conn.close()

    df = df.rename(columns={"ref": "CAS"})
    return df


def _classify_chemical_class_value(value):
    """Maps a raw CHEMICALCLASS free-text value to YES/NO/NO_DATA."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "NO_DATA"
    text = str(value).strip()
    if text == "" or text == "?":
        return "NO_DATA"
    if text.lower() == "no":
        return "NO"
    return "YES"


def _worst_chemical_class_by_group(active_df, group_cols, chemical_col, group_index):
    """Per group, the worst case across every active CAS's chemical-class flag - copied
    unchanged from MAS_quick_C2C_assessment_static.py (see its docstring for full rationale)."""
    tmp = active_df[list(group_cols) + [chemical_col, COL_CAS]].copy()
    tmp["_class"] = tmp[chemical_col].apply(_classify_chemical_class_value)

    def _worst(grp):
        yes_cas = sorted(grp.loc[grp["_class"] == "YES", COL_CAS].dropna().astype(str).unique().tolist())
        if yes_cas:
            return f"Yes (CAS: {', '.join(yes_cas)})"
        if (grp["_class"] == "NO_DATA").any():
            return NOT_SUFFICIENT_DATA_LABEL
        return "No"

    if tmp.empty:
        result = pd.Series(dtype=object)
    else:
        result = tmp.groupby(list(group_cols), sort=False)[["_class", COL_CAS]].apply(_worst)
    return result.reindex(group_index, fill_value=NOT_SUFFICIENT_DATA_LABEL)


def _worst_colour_by_group(active_df, group_cols, hazard_col, group_index, with_scenarios=False):
    """Worst colour per group among active rows; optionally also the (joined) scenario ids that produced it."""
    ranks = active_df[hazard_col].map(classify_colour).map(COLOUR_RANK)
    tmp = active_df[list(group_cols)].copy()
    tmp["_rank"] = ranks

    worst_rank = tmp.groupby(list(group_cols), sort=False)["_rank"].max()
    worst_rank = worst_rank.reindex(group_index, fill_value=COLOUR_RANK["GREY"])
    colours = worst_rank.map(RANK_TO_COLOUR)

    if not with_scenarios:
        return colours, None

    tmp["_scenario"] = active_df[COL_SCENARIO_ID].values
    grp_max = tmp.groupby(list(group_cols), sort=False)["_rank"].transform("max")
    mask = tmp["_rank"] == grp_max
    scenario_lists = (
        tmp[mask]
        .groupby(list(group_cols), sort=False)["_scenario"]
        .agg(_join_unique)
        .reindex(group_index, fill_value="")
    )
    return colours, scenario_lists


def _build_flagged_issues_by_product(active_df, products_index, missing_cas_df):
    """Per Product, flag missing %-composition and missing-CAS data quality issues - matches
    MAS_quick_C2C_assessment_static.py's own version (see its docstring), which labels each
    flagged material by "Final Material Map" (COL_FINAL_MATERIAL_MAP). That column only
    exists in the per-CAS dataframe build_c2c_assessment_df produces (Quick Assessment's own
    detailed data) - the mixture-rules pipeline's active_scaffold_df has no per-CAS material
    map, so this falls back to COL_HOM_MAT there instead of raising a KeyError."""
    material_label_col = COL_FINAL_MATERIAL_MAP if COL_FINAL_MATERIAL_MAP in active_df.columns else COL_HOM_MAT
    composition_cols = [COL_MIN_PROD, COL_MAX_PROD, COL_MIN_HOM, COL_MAX_HOM]
    composition_missing = active_df[composition_cols].isna().any(axis=1)
    missing_material_cols = [COL_PRODUCT, COL_CAS, material_label_col]
    missing_materials_df = active_df.loc[composition_missing, missing_material_cols].drop_duplicates()
    missing_materials_df["_label"] = missing_materials_df[COL_CAS] + " (" + missing_materials_df[material_label_col] + ")"
    missing_materials_by_product = missing_materials_df.groupby(COL_PRODUCT, sort=False)["_label"].agg(list)

    missing_cas_set = set(missing_cas_df["CAS"]) if missing_cas_df is not None and not missing_cas_df.empty else set()
    cas_by_product = (
        active_df.groupby(COL_PRODUCT, sort=False)[COL_CAS]
        .agg(lambda s: sorted(set(s) & missing_cas_set))
    )

    pct_flags = []
    cas_flags = []
    for prod in products_index:
        materials_here = missing_materials_by_product.get(prod, [])
        if materials_here:
            pct_flags.append(
                f"Missing % composition for: {'; '.join(materials_here)} - this could influence the % assessed calculation."
            )
        else:
            pct_flags.append(PCT_ASSESSED_FLAG_OK)
        missing_here = cas_by_product.get(prod, [])
        cas_flags.append(", ".join(missing_here) if missing_here else PCT_ASSESSED_FLAG_OK)
    return pct_flags, cas_flags


def build_overview_df(detailed_df, missing_cas_df=None, mixture_rules_ran=True):
    """Build the "overview" sheet's left block (worst %-assessed and its scenario(s) per Product, with % assessed/missing-CAS flags) and right block (per Product+Homogeneous Material: %-in-product range, worst chemical-class flags, worst raw colour per hazard endpoint with scenario IDs, and the derived overall C2C material health rating and comment).

    Pass mixture_rules_ran=False (quick assessment / option C) when detailed_df's hazard
    colours are raw per-CAS DB values that never went through the additive mixture-rule
    calculation - the 8 mixture-rule-capable endpoints are then unconditionally labelled
    "WITHOUT MIXTURE RULES: {colour}" instead of getting _reattach_incomplete_comp_prefix's
    fallback-only labelling, matching MAS_quick_C2C_assessment_current.py's _display_colour."""
    active_df = detailed_df[detailed_df[COL_ACTIVE] == True].copy()

    # ---- left block: worst % assessed per Product, across all its scenarios ----
    idx_ps = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    pct_by_ps = _pct_assessed_by_group(active_df, [COL_PRODUCT, COL_SCENARIO_ID], COL_MIN_PROD, COL_MAX_PROD, idx_ps)

    df_ps = pct_by_ps.rename("pct").reset_index()
    min_per_product = df_ps.groupby(COL_PRODUCT, sort=False)["pct"].transform("min")
    worst_mask = df_ps["pct"] == min_per_product
    worst_pct = df_ps.groupby(COL_PRODUCT, sort=False)["pct"].min()
    worst_scenarios = (
        df_ps[worst_mask]
        .groupby(COL_PRODUCT, sort=False)[COL_SCENARIO_ID]
        .agg(_join_unique)
        .reindex(worst_pct.index)
    )
    pct_flags, cas_flags = _build_flagged_issues_by_product(active_df, worst_pct.index, missing_cas_df)
    left_df = pd.DataFrame({
        "Flagged for % assessed:": pct_flags,
        "C2C hazard assessment missing CAS:": cas_flags,
        "Product": worst_pct.index,
        "% assessed": worst_pct.values,
        "Scenario ID": worst_scenarios.values,
    })

    # ---- right block: worst colour (+ contributing scenarios) per Product+HomMat, across all scenarios ----
    idx_ph = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_HOM_MAT]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    right_df = pd.DataFrame(index=idx_ph).reset_index()
    right_df.columns = ["Product", "Homogenous Material"]

    grp_all_ph = detailed_df.groupby([COL_PRODUCT, COL_HOM_MAT], sort=False)
    min_pct_hm = grp_all_ph[COL_MIN_PCT_HOMMAT_IN_PROD].min().reindex(idx_ph)
    max_pct_hm = grp_all_ph[COL_MAX_PCT_HOMMAT_IN_PROD].max().reindex(idx_ph)

    hom_mat_values = right_df.pop("Homogenous Material").values
    right_df.insert(1, "Min % Homogenous material in Product", min_pct_hm.values)
    right_df.insert(2, "Max % Homogenous material in Product", max_pct_hm.values)
    right_df.insert(3, "Homogenous Material", hom_mat_values)
    right_df.insert(4, COL_OVERALL_RATING, None)
    right_df.insert(5, COL_OVERALL_RATING_COMMENT, None)

    insert_at = 6
    for chemical_col, label in CHEMICAL_CLASS_RISK_FLAGS.items():
        if chemical_col not in detailed_df.columns:
            continue
        flags = _worst_chemical_class_by_group(active_df, [COL_PRODUCT, COL_HOM_MAT], chemical_col, idx_ph)
        right_df.insert(insert_at, label, flags.values)
        insert_at += 1

    right_df.insert(insert_at, _OVERVIEW_SPACER_COL, "")

    raw_colours_by_hazard = {}
    for hazard_col, suffix in zip(HAZARD_COLS_READABLE, SCENARIO_ID_SUFFIXES):
        colours, scenario_lists = _worst_colour_by_group(
            active_df, [COL_PRODUCT, COL_HOM_MAT], hazard_col, idx_ph, with_scenarios=True
        )
        raw_colours_by_hazard[hazard_col] = colours.values
        # classify_colour() (inside _worst_colour_by_group) strips any prefix down to a bare
        # colour - re-attach INCOMPLETE_COMP_LABEL for the 8 mixture-rule-capable endpoints
        # where the underlying raw value was in that fallback state (see
        # _reattach_incomplete_comp_prefix's docstring). Overall rating still uses the bare
        # `colours` (raw_colours_by_hazard), not this display-only wrapped value.
        if mixture_rules_ran:
            right_df[hazard_col] = _reattach_incomplete_comp_prefix(
                active_df, [COL_PRODUCT, COL_HOM_MAT], hazard_col, idx_ph, colours
            )
        else:
            right_df[hazard_col] = _apply_without_mixture_rules_label(hazard_col, colours)
        right_df[f"Scenario ID_{suffix}"] = scenario_lists.values

    overall_ratings = []
    overall_comments = []
    for i in range(len(idx_ph)):
        colours_here = {hc: raw_colours_by_hazard[hc][i] for hc in HAZARD_COLS_READABLE}
        rating, comment = _overall_c2c_rating(colours_here)
        overall_ratings.append(rating)
        overall_comments.append(comment)
    right_df[COL_OVERALL_RATING] = overall_ratings
    right_df[COL_OVERALL_RATING_COMMENT] = overall_comments

    return left_df, right_df


def build_percent_assessed_overview_df(detailed_df):
    """Same left-block logic as build_overview_df (worst % assessed per Product across its
    scenarios), but with NO "C2C hazard assessment missing CAS:" flag column and a right
    block with NO hazard columns, NO chemical-class flags, and NO Overall C2C Material
    Health Rating - used by the no-DB Percent Assessed pipeline, which never queries
    COLOUR_ASSESSMENT_C2C/CHEMICALCLASS and so has none of that data to show."""
    active_df = detailed_df[detailed_df[COL_ACTIVE] == True].copy()

    idx_ps = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    pct_by_ps = _pct_assessed_by_group(active_df, [COL_PRODUCT, COL_SCENARIO_ID], COL_MIN_PROD, COL_MAX_PROD, idx_ps)

    df_ps = pct_by_ps.rename("pct").reset_index()
    min_per_product = df_ps.groupby(COL_PRODUCT, sort=False)["pct"].transform("min")
    worst_mask = df_ps["pct"] == min_per_product
    worst_pct = df_ps.groupby(COL_PRODUCT, sort=False)["pct"].min()
    worst_scenarios = (
        df_ps[worst_mask]
        .groupby(COL_PRODUCT, sort=False)[COL_SCENARIO_ID]
        .agg(_join_unique)
        .reindex(worst_pct.index)
    )
    pct_flags, _ = _build_flagged_issues_by_product(active_df, worst_pct.index, None)
    left_df = pd.DataFrame({
        "Flagged for % assessed:": pct_flags,
        "Product": worst_pct.index,
        "% assessed": worst_pct.values,
        "Scenario ID": worst_scenarios.values,
    })

    idx_ph = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_HOM_MAT]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    right_df = pd.DataFrame(index=idx_ph).reset_index()
    right_df.columns = ["Product", "Homogenous Material"]

    grp_all_ph = detailed_df.groupby([COL_PRODUCT, COL_HOM_MAT], sort=False)
    min_pct_hm = grp_all_ph[COL_MIN_PCT_HOMMAT_IN_PROD].min().reindex(idx_ph)
    max_pct_hm = grp_all_ph[COL_MAX_PCT_HOMMAT_IN_PROD].max().reindex(idx_ph)

    hom_mat_values = right_df.pop("Homogenous Material").values
    right_df.insert(1, "Min % Homogenous material in Product", min_pct_hm.values)
    right_df.insert(2, "Max % Homogenous material in Product", max_pct_hm.values)
    right_df.insert(3, "Homogenous Material", hom_mat_values)

    return left_df, right_df


def build_percentage_assessed_df(detailed_df):
    """Build the "percentage_assessed" sheet's left block (%-assessed per Product+Scenario) and right block (%-assessed per Product+Homogeneous Material+Scenario), without picking a single worst case."""
    active_df = detailed_df[detailed_df[COL_ACTIVE] == True].copy()

    idx_ps = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    pct_by_ps = _pct_assessed_by_group(active_df, [COL_PRODUCT, COL_SCENARIO_ID], COL_MIN_PROD, COL_MAX_PROD, idx_ps)
    left_df = pct_by_ps.rename("% assessed").reset_index()
    left_df.columns = ["Product", "Scenario ID", "% assessed"]

    idx_phs = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    pct_by_phs = _pct_assessed_by_group(active_df, [COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], COL_MIN_HOM, COL_MAX_HOM, idx_phs)
    right_df = pct_by_phs.rename("% assessed").reset_index()
    right_df.columns = ["Product", "Homogenous Material", "Scenario ID", "% assessed"]

    return left_df, right_df


def build_risk_assessed_df(detailed_df, mixture_rules_ran=True):
    """Build the "risk_assessed" sheet: per Product+Homogeneous Material+Scenario, the %-in-product range plus the worst chemical-class flags for that scenario.

    See build_overview_df's docstring for mixture_rules_ran's meaning."""
    active_df = detailed_df[detailed_df[COL_ACTIVE] == True].copy()

    idx_phs = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    df = pd.DataFrame(index=idx_phs).reset_index()
    df.columns = ["Product", "Homogenous Material", "Scenario ID"]

    grp_all = detailed_df.groupby([COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], sort=False)
    min_pct = grp_all[COL_MIN_PCT_HOMMAT_IN_PROD].min().reindex(idx_phs)
    max_pct = grp_all[COL_MAX_PCT_HOMMAT_IN_PROD].max().reindex(idx_phs)
    df.insert(1, "Min % Homogenous material in Product", min_pct.values)
    df.insert(2, "Max % Homogenous material in Product", max_pct.values)

    insert_at = df.columns.get_loc("Scenario ID") + 1
    for chemical_col, label in CHEMICAL_CLASS_RISK_FLAGS.items():
        if chemical_col not in detailed_df.columns:
            continue
        flags = _worst_chemical_class_by_group(
            active_df, [COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], chemical_col, idx_phs
        )
        df.insert(insert_at, label, flags.values)
        insert_at += 1

    for hazard_col in HAZARD_COLS_READABLE:
        colours, _ = _worst_colour_by_group(
            active_df, [COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], hazard_col, idx_phs, with_scenarios=False
        )
        if mixture_rules_ran:
            df[hazard_col] = _reattach_incomplete_comp_prefix(
                active_df, [COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], hazard_col, idx_phs, colours
            )
        else:
            df[hazard_col] = _apply_without_mixture_rules_label(hazard_col, colours)

    return df


def _clear_sheet_rows(ws):
    """Delete every row below the header - call ONCE per sheet before writing any dataframe to it."""
    if ws.max_row > 1:
        ws.delete_rows(2, ws.max_row - 1)


def _write_df_to_sheet_by_header(ws, df, start_col=1, end_col=None, allow_new_columns=False):
    """Write df starting at row 2, matching columns by the sheet's row-1 header text - copied
    unchanged from MAS_quick_C2C_assessment_static.py (see its docstring for full rationale)."""
    if end_col is None:
        end_col = ws.max_column
    header_to_col = {
        cell.value: cell.column
        for cell in ws[1]
        if cell.value is not None and start_col <= cell.column <= end_col
    }

    if allow_new_columns:
        next_col = end_col + 1
        for header in df.columns:
            if header not in header_to_col:
                ws.cell(row=1, column=next_col, value=header)
                header_to_col[header] = next_col
                next_col += 1

    for row_offset, row in enumerate(df.itertuples(index=False), start=2):
        row_dict = dict(zip(df.columns, row))
        for header, value in row_dict.items():
            col_idx = header_to_col.get(header)
            if col_idx is None:
                continue
            if value is None or (isinstance(value, float) and pd.isna(value)):
                value = None
            ws.cell(row=row_offset, column=col_idx, value=value)


def _write_df_to_sheet_positional(ws, df, start_col=1):
    """Write df's own headers (row 1) and data (from row 2), purely by column position -
    copied unchanged from MAS_quick_C2C_assessment_static.py (see its docstring)."""
    for col_idx, col_name in enumerate(df.columns, start=start_col):
        ws.cell(row=1, column=col_idx).value = col_name or None
    for row_offset, row in enumerate(df.itertuples(index=False), start=2):
        for col_offset, value in enumerate(row, start=start_col):
            if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
                value = None
            ws.cell(row=row_offset, column=col_offset).value = value


_COLOUR_STYLES = {
    "red": (PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"), Font(color="9C0006")),
    "yellow": (PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"), Font(color="9C6500")),
    "grey": (PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"), Font(color="3B3B3B")),
    "green": (PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"), Font(color="006100")),
}


def _apply_colour_conditional_formatting(ws, first_col_letter, last_col_letter, last_row):
    """Contiguous-range variant - only use where every column in the range is an actual colour column."""
    ws.conditional_formatting._cf_rules.clear()
    anchor = f"{first_col_letter}2"
    cell_range = f"{first_col_letter}2:{last_col_letter}{last_row}"
    for keyword, (fill, font) in _COLOUR_STYLES.items():
        formula = f'ISNUMBER(SEARCH("{keyword}",{anchor}))'
        ws.conditional_formatting.add(cell_range, FormulaRule(formula=[formula], fill=fill, font=font, stopIfTrue=False))


def _style_overview_sheet(ws, last_col, last_data_row, extra_spacer_cols=None, base_spacer_cols=(3, 7)):
    """Uniform column widths, wrapped header row, spacer columns kept narrow - copied
    unchanged from MAS_quick_C2C_assessment_static.py (see its docstring), except
    `base_spacer_cols` is now a parameter (default (3, 7), the mixture-rules/quick-assessment
    overview layout's own fixed spacer positions) rather than hardcoded, since the no-DB
    Percent Assessed overview has a different column layout (no CAS-missing flag column, no
    hazard block) and so different spacer positions."""
    UNIFORM_WIDTH = 22
    SPACER_WIDTH = 3
    SPACER_COLS = set(base_spacer_cols) | set(extra_spacer_cols or ())

    for col in range(1, last_col + 1):
        letter = get_column_letter(col)
        ws.column_dimensions[letter].width = SPACER_WIDTH if col in SPACER_COLS else UNIFORM_WIDTH

    header_alignment = Alignment(wrap_text=True, vertical="bottom")
    data_alignment = Alignment(wrap_text=False)
    ws.row_dimensions[1].height = 45
    for col in range(1, last_col + 1):
        ws.cell(row=1, column=col).alignment = header_alignment
    for row in range(2, last_data_row + 1):
        for col in range(1, last_col + 1):
            ws.cell(row=row, column=col).alignment = data_alignment


def _hazard_col_range(df):
    """First/last column letters of the 21-column hazard block within a df built from
    build_c2c_assessment_df() (detailed_overview) or build_risk_assessed_df() (risk_assessed).
    Returns None if df has no hazard columns at all (e.g. the no-DB Percent Assessed
    pipeline's detailed_overview, which never has hazard data to colour)."""
    hazard_cols = [c for c in df.columns if c.startswith("C2C assessment ")]
    if not hazard_cols:
        return None
    first_idx = df.columns.get_loc(hazard_cols[0])
    last_idx = df.columns.get_loc(hazard_cols[-1])
    return get_column_letter(first_idx + 1), get_column_letter(last_idx + 1)


def _apply_colour_conditional_formatting_cols(ws, col_letters, last_row):
    """Non-contiguous variant, one column at a time - copied unchanged from
    MAS_quick_C2C_assessment_static.py (see its docstring)."""
    ws.conditional_formatting._cf_rules.clear()
    for keyword, (fill, font) in _COLOUR_STYLES.items():
        for col in col_letters:
            formula = f'ISNUMBER(SEARCH("{keyword}",{col}2))'
            ws.conditional_formatting.add(f"{col}2:{col}{last_row}", FormulaRule(formula=[formula], fill=fill, font=font, stopIfTrue=False))


def save_c2c_assessment_workbook_static(
    c2c_df, missing_cas_df, output_path, template_path=MIXTURE_RULES_TEMPLATE_PATH, write_detailed=True,
    mixture_rules_ran=True
):
    """
    Copy the shared C2C assessment template to output_path, but instead of generating Excel
    formulas for "overview"/"percentage_assessed"/"risk_assessed", compute their values in
    pandas here and write them as plain data - copied/adapted from
    MAS_quick_C2C_assessment_static.py's identical function (see its docstring for the full
    layout rationale). Pass write_detailed=False for a "summary only" file - "detailed_overview"
    is then dropped entirely (its data lives in the separate detailed_overview file(s), see
    save_c2c_assessment_output()). Pass mixture_rules_ran=False for quick assessment (option C) -
    see build_overview_df's docstring.
    """
    if not os.path.exists(template_path):
        raise FileNotFoundError(
            f"C2C assessment template not found at: {template_path}\n"
            "Check MIXTURE_RULES_TEMPLATE_PATH at the top of this file."
        )

    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)

    ws_detail = wb["detailed_overview"]
    if write_detailed:
        if ws_detail.max_column > len(c2c_df.columns):
            ws_detail.delete_cols(len(c2c_df.columns) + 1, ws_detail.max_column - len(c2c_df.columns))
        _clear_sheet_rows(ws_detail)
        _write_df_to_sheet_positional(ws_detail, c2c_df)
    else:
        del wb["detailed_overview"]
        ws_detail = None

    ws_overview = wb["overview"]
    ws_overview.insert_cols(1, amount=3)
    bold = Font(bold=True)
    header_cell_pct = ws_overview.cell(row=1, column=1, value="Flagged for % assessed:")
    header_cell_pct.font = bold
    header_cell_cas = ws_overview.cell(row=1, column=2, value="C2C hazard assessment missing CAS:")
    header_cell_cas.font = bold

    overview_left, overview_right = build_overview_df(c2c_df, missing_cas_df, mixture_rules_ran=mixture_rules_ran)
    percentage_left, percentage_right = build_percentage_assessed_df(c2c_df)
    risk_df = build_risk_assessed_df(c2c_df, mixture_rules_ran=mixture_rules_ran)

    _clear_sheet_rows(ws_overview)
    _write_df_to_sheet_by_header(ws_overview, overview_left, start_col=1, end_col=7)
    _write_df_to_sheet_positional(ws_overview, overview_right, start_col=8)

    red_font = Font(color="FF0000")
    for row_offset in range(2, 2 + len(overview_left)):
        cell = ws_overview.cell(row=row_offset, column=1)
        if cell.value and cell.value != PCT_ASSESSED_FLAG_OK:
            cell.font = red_font

    ws_pct = wb["percentage_assessed"]
    _clear_sheet_rows(ws_pct)
    _write_df_to_sheet_by_header(ws_pct, percentage_left, start_col=1, end_col=3)
    _write_df_to_sheet_by_header(ws_pct, percentage_right, start_col=5, end_col=8)

    ws_risk = wb["risk_assessed"]
    _clear_sheet_rows(ws_risk)
    _write_df_to_sheet_positional(ws_risk, risk_df)

    n_rows = max(len(overview_right), len(risk_df), 1)
    last_row = n_rows + 1 + 100
    if write_detailed:
        hazard_range = _hazard_col_range(c2c_df)
        if hazard_range is not None:
            first_letter, last_letter = hazard_range
            _apply_colour_conditional_formatting(ws_detail, first_letter, last_letter, len(c2c_df) + 1 + 100)
    overview_right_start_col = 8
    hazard_start_in_block = overview_right.columns.get_loc(HAZARD_COLS_READABLE[0])
    hazard_start_col = overview_right_start_col + hazard_start_in_block
    overview_hazard_cols = [get_column_letter(c) for c in range(hazard_start_col, hazard_start_col + 2 * len(HAZARD_COLS_READABLE), 2)]
    overall_rating_col = get_column_letter(overview_right_start_col + overview_right.columns.get_loc(COL_OVERALL_RATING))
    _apply_colour_conditional_formatting_cols(ws_overview, [overall_rating_col] + overview_hazard_cols, last_row)
    risk_first_letter, risk_last_letter = _hazard_col_range(risk_df)
    _apply_colour_conditional_formatting(wb["risk_assessed"], risk_first_letter, risk_last_letter, last_row)

    overview_last_data_row = max(len(overview_left), len(overview_right), 1) + 1
    overview_last_col = 7 + len(overview_right.columns)
    overview_spacer_col = overview_right_start_col + overview_right.columns.get_loc(_OVERVIEW_SPACER_COL)
    _style_overview_sheet(
        ws_overview, last_col=overview_last_col, last_data_row=overview_last_data_row,
        extra_spacer_cols={overview_spacer_col},
    )

    wb.save(output_path)


def save_percent_assessed_workbook(detailed_df, output_path, template_path=MIXTURE_RULES_TEMPLATE_PATH, write_detailed=True):
    """Percent-Assessed-only summary: same shared template, but this pipeline never queries
    the database, so there's no hazard data, no chemical-class flags, and nothing for a
    "risk_assessed" sheet to show (that sheet is entirely built from hazard colours) - it is
    deleted from the copied template entirely. "overview" has no hazard block, no
    chemical-class flags, no Overall C2C Material Health Rating, and no "C2C hazard
    assessment missing CAS:" flag - just the % assessed / composition data
    (build_percent_assessed_overview_df). Pass write_detailed=False for a "summary only"
    file, matching save_c2c_assessment_workbook_static's own convention.
    """
    if not os.path.exists(template_path):
        raise FileNotFoundError(
            f"C2C assessment template not found at: {template_path}\n"
            "Check MIXTURE_RULES_TEMPLATE_PATH at the top of this file."
        )

    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)

    del wb["risk_assessed"]

    ws_detail = wb["detailed_overview"]
    if write_detailed:
        _clear_sheet_rows(ws_detail)
        _write_df_to_sheet_positional(ws_detail, detailed_df)
    else:
        del wb["detailed_overview"]
        ws_detail = None

    ws_overview = wb["overview"]
    ws_overview.insert_cols(1, amount=2)
    ws_overview.cell(row=1, column=1, value="Flagged for % assessed:").font = Font(bold=True)

    overview_left, overview_right = build_percent_assessed_overview_df(detailed_df)
    percentage_left, percentage_right = build_percentage_assessed_df(detailed_df)

    # The template's "overview" sheet ships with a hazard block (21 endpoints, 2 cols each)
    # this pipeline never fills - delete those leftover columns entirely so no stale header
    # text survives past what overview_right actually writes (positional writes only ever
    # overwrite as many columns as the dataframe has, never clear extra pre-existing ones).
    overview_last_col = 6 + len(overview_right.columns)
    if ws_overview.max_column > overview_last_col:
        ws_overview.delete_cols(overview_last_col + 1, ws_overview.max_column - overview_last_col)

    _clear_sheet_rows(ws_overview)
    _write_df_to_sheet_by_header(ws_overview, overview_left, start_col=1, end_col=6)
    _write_df_to_sheet_positional(ws_overview, overview_right, start_col=7)

    red_font = Font(color="FF0000")
    for row_offset in range(2, 2 + len(overview_left)):
        cell = ws_overview.cell(row=row_offset, column=1)
        if cell.value and cell.value != PCT_ASSESSED_FLAG_OK:
            cell.font = red_font

    ws_pct = wb["percentage_assessed"]
    _clear_sheet_rows(ws_pct)
    _write_df_to_sheet_by_header(ws_pct, percentage_left, start_col=1, end_col=3)
    _write_df_to_sheet_by_header(ws_pct, percentage_right, start_col=5, end_col=8)

    if write_detailed:
        hazard_range = _hazard_col_range(detailed_df)
        if hazard_range is not None:
            first_letter, last_letter = hazard_range
            _apply_colour_conditional_formatting(ws_detail, first_letter, last_letter, len(detailed_df) + 1 + 100)

    overview_last_data_row = max(len(overview_left), len(overview_right), 1) + 1
    # Layout differs from save_c2c_assessment_workbook_static's: col1 = flag header, col2 =
    # spacer before Product(left) at col3, col6 = spacer before Product(right) at col7 - not
    # the (3, 7) default, since there's no 2nd flag column or hazard block here.
    _style_overview_sheet(
        ws_overview, last_col=overview_last_col, last_data_row=overview_last_data_row,
        base_spacer_cols=(2, 6),
    )

    wb.save(output_path)


def save_detailed_overview_only(detail_df, output_path, template_path=MIXTURE_RULES_TEMPLATE_PATH):
    """Write just the "detailed_overview" sheet (data + colour conditional formatting) for a
    subset of the full data - copied unchanged from MAS_quick_C2C_assessment_static.py."""
    if not os.path.exists(template_path):
        raise FileNotFoundError(
            f"C2C assessment template not found at: {template_path}\n"
            "Check MIXTURE_RULES_TEMPLATE_PATH at the top of this file."
        )

    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)

    for sheet_name in ("overview", "percentage_assessed", "risk_assessed"):
        del wb[sheet_name]

    ws_detail = wb["detailed_overview"]
    # detail_df may have fewer columns than the template ships with (e.g. the no-DB Percent
    # Assessed pipeline, with no hazard columns at all) - _write_df_to_sheet_positional only
    # ever overwrites as many columns as detail_df has, so drop any leftover template
    # columns beyond that first, or their stale header text/data would survive untouched.
    if ws_detail.max_column > len(detail_df.columns):
        ws_detail.delete_cols(len(detail_df.columns) + 1, ws_detail.max_column - len(detail_df.columns))
    _clear_sheet_rows(ws_detail)
    _write_df_to_sheet_positional(ws_detail, detail_df)

    hazard_range = _hazard_col_range(detail_df)
    if hazard_range is not None:
        last_row = len(detail_df) + 1 + 100
        first_letter, last_letter = hazard_range
        _apply_colour_conditional_formatting(ws_detail, first_letter, last_letter, last_row)

    wb.save(output_path)


def _sanitize_filename_part(text):
    """Replace filesystem-unsafe characters in `text` with underscores for use in a file name, falling back to "unnamed" if the result is empty."""
    return re.sub(r'[\\/*?:"<>|]', "_", str(text)).strip() or "unnamed"


def _split_scenarios_into_batches(df_p, row_cap):
    """Split one product's rows into consecutive scenario batches - copied unchanged from
    MAS_quick_C2C_assessment_static.py (see its docstring)."""
    scenario_order = df_p[COL_SCENARIO_ID].drop_duplicates().tolist()
    counts = df_p[COL_SCENARIO_ID].value_counts()
    max_rows = max(row_cap, 1)

    batches = []
    current_scenarios, current_rows, start_idx = [], 0, 1
    for i, sid in enumerate(scenario_order, start=1):
        rows = int(counts[sid])
        if current_scenarios and current_rows + rows > max_rows:
            batches.append((current_scenarios, start_idx, i - 1))
            current_scenarios, current_rows, start_idx = [], 0, i
        current_scenarios.append(sid)
        current_rows += rows
    if current_scenarios:
        batches.append((current_scenarios, start_idx, len(scenario_order)))
    return batches


def _save_detailed_overview_files(c2c_df, detail_dir, file_stem, date_str, name_base, template_path):
    """Shared file-splitting/naming logic for a detailed_overview output directory, used by
    both save_c2c_assessment_output and save_c2c_detailed_overview_output. name_base is the
    filename prefix (e.g. "C2C_quick_assessment_detailed_overview", "C2C_percent_assessed_detailed_overview",
    "C2C_assessment_detailed_overview") - the caller uses the same string to name detail_dir itself, so
    the folder and the file(s) inside it share one naming scheme. Returns the list of saved paths."""
    saved_paths = []

    total_rows = len(c2c_df)
    if total_rows < DETAILED_OVERVIEW_ROW_CAP:
        detail_path = os.path.join(detail_dir, f"{name_base}_{file_stem}_{date_str}.xlsx")
        save_detailed_overview_only(c2c_df, detail_path, template_path=template_path)
        saved_paths.append(detail_path)
        return saved_paths

    print(
        f"detailed_overview would need {total_rows} rows, at or above the {DETAILED_OVERVIEW_ROW_CAP} cap - "
        f"splitting it into one file per product (and, for any product still too big on its own, further "
        f"into scenario-range batches), saved under: {detail_dir}"
    )

    for prod, df_p in c2c_df.groupby(COL_PRODUCT, sort=False):
        prod_label = _sanitize_filename_part(prod)

        if len(df_p) < DETAILED_OVERVIEW_ROW_CAP:
            n_scenarios = df_p[COL_SCENARIO_ID].nunique()
            out_path = os.path.join(
                detail_dir,
                f"{name_base}_{prod_label}_scenarios_1-{n_scenarios}_{file_stem}_{date_str}.xlsx",
            )
            save_detailed_overview_only(df_p, out_path, template_path=template_path)
            saved_paths.append(out_path)
            continue

        for scenario_ids, start_idx, end_idx in _split_scenarios_into_batches(df_p, DETAILED_OVERVIEW_ROW_CAP):
            df_batch = df_p[df_p[COL_SCENARIO_ID].isin(scenario_ids)]
            out_path = os.path.join(
                detail_dir,
                f"{name_base}_{prod_label}_scenarios_{start_idx}-{end_idx}_{file_stem}_{date_str}.xlsx",
            )
            save_detailed_overview_only(df_batch, out_path, template_path=template_path)
            saved_paths.append(out_path)

    return saved_paths


def save_c2c_assessment_output(
    c2c_df, missing_cas_df, saving_dir, file_name, date_str, template_path=MIXTURE_RULES_TEMPLATE_PATH,
    mixture_rules_ran=True, name_base="C2C_assessment", detail_name_base=None
):
    """Save the C2C assessment (summary file + separate detailed_overview file(s)) - copied
    unchanged from MAS_quick_C2C_assessment_static.py (see its docstring for the full
    file-splitting rationale), plus mixture_rules_ran (see build_overview_df's docstring) -
    pass False for quick assessment (option C), where c2c_df carries raw per-CAS colours that
    never went through the mixture-rule calculation.

    name_base is the summary file's prefix (e.g. "C2C_quick_assessment" for option C, default
    "C2C_assessment" for option B); detail_name_base is the detailed_overview folder+file
    prefix, defaulting to f"{name_base}_detailed_overview" when not given. Returns the list of
    saved file paths."""
    file_stem = os.path.splitext(file_name)[0]
    if detail_name_base is None:
        detail_name_base = f"{name_base}_detailed_overview"
    saved_paths = []

    summary_path = os.path.join(saving_dir, f"{name_base}_{file_stem}_{date_str}.xlsx")
    save_c2c_assessment_workbook_static(
        c2c_df, missing_cas_df, summary_path, template_path=template_path, write_detailed=False,
        mixture_rules_ran=mixture_rules_ran
    )
    saved_paths.append(summary_path)

    detail_dir = os.path.join(saving_dir, f"{detail_name_base}_{file_stem}_{date_str}")
    os.makedirs(detail_dir, exist_ok=True)
    saved_paths += _save_detailed_overview_files(c2c_df, detail_dir, file_stem, date_str, detail_name_base, template_path)

    return saved_paths


def save_c2c_detailed_overview_output(
    c2c_df, saving_dir, file_name, date_str, template_path=MIXTURE_RULES_TEMPLATE_PATH, name_base="C2C_assessment_detailed_overview"
):
    """Same file-splitting/naming as save_c2c_assessment_output's detailed_overview half,
    but WITHOUT also writing its "<name_base>_<file>_<date>.xlsx" summary file - for
    run_mixture_rules, where that summary would be redundant with (and wrong relative to,
    since it would show raw per-CAS colours rather than the mixture-rule-computed result)
    the separately-saved mixture-rule summary (save_c2c_assessment_workbook_static on
    active_scaffold_df). name_base is the detailed_overview folder+file prefix (e.g.
    "C2C_percent_assessed_detailed_overview" for option A). Returns the list of saved
    detailed_overview file paths."""
    file_stem = os.path.splitext(file_name)[0]

    detail_dir = os.path.join(saving_dir, f"{name_base}_{file_stem}_{date_str}")
    os.makedirs(detail_dir, exist_ok=True)

    return _save_detailed_overview_files(c2c_df, detail_dir, file_stem, date_str, name_base, template_path)


def rename_mixture_rules_endpoints_to_readable(active_scaffold_df):
    """Rename active_scaffold_df's mixture-rule output columns (the pipeline's own "C2C
    <label>" convention) to the HAZARD_COLS_READABLE names build_overview_df/
    build_percentage_assessed_df/build_risk_assessed_df group over. Any of the 21 columns
    NOT present in active_scaffold_df (shouldn't normally happen once analyse_the_dataset_
    with_mixture_rules has run) is left absent - the builders tolerate a missing hazard
    column by treating it as GREY (see _worst_colour_by_group's classify_colour default)."""
    rename_map = {
        old: new for old, new in _MIXTURE_ENDPOINT_TO_READABLE.items() if old in active_scaffold_df.columns
    }
    return active_scaffold_df.rename(columns=rename_map)


#################################################################
### Calculating with mixture rules
def run_wint_C2C_mixture_rules():
    """CLI entry point (option B): prompt for the MAS Excel file, output folder and database, run the full C2C mixture-rule assessment across all scenarios from DB-only toxicity data, always save the summary overview/percentage_assessed/risk_assessed workbook, and optionally save all-scenarios and/or user-selected-scenarios detailed_overview outputs."""
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
    _, _, C2C_mixture_results, all_c2c_scenario_results_df, active_scaffold_df = analyse_the_dataset_with_mixture_rules(df, scenarios, db_path)
    ### Saving:
    now = datetime.now()
    time = now.strftime("%Y%m%d")
    file_stem = os.path.splitext(file_name)[0]
    saving_selected = os.path.join(saving_dir, f"selected_scenarios_{time}_{file_stem}.xlsx")
    saving_all_scenarios = os.path.join(saving_dir, f"all_scenarios_{time}_{file_stem}.xlsx")
    C2C_mixture_rules_saving = os.path.join(saving_dir, f"C2C_assessment_{file_stem}_{time}.xlsx")

    print("--------------------------------------------------------------")
    # "overview"/"percentage_assessed"/"risk_assessed" (mixture-rule-computed), summary-only
    # file (no detailed_overview here - that's the separate per-CAS RAW-colour output below).
    # This and detailed_overview are the ONLY two outputs this pipeline always saves.
    readable_scaffold_df = rename_mixture_rules_endpoints_to_readable(active_scaffold_df)
    save_c2c_assessment_workbook_static(readable_scaffold_df, pd.DataFrame(), C2C_mixture_rules_saving, write_detailed=False)
    print("Saved mixture rules assessment (overview/percentage_assessed/risk_assessed) to file: ", C2C_mixture_rules_saving)
    print("--------------------------------------------------------------")
    print("Do you want to save all scenarios? (y/n)")
    user_input = input("").strip().lower()
    if user_input == "y":
        print("Saving...")
        all_scenarios_df = build_selected_scenarios_df(df, scenarios, scenario_ids)
        all_scenarios_df.to_excel(saving_all_scenarios, index=False)
        print("Saved all scenarios to file: ", saving_all_scenarios)
        # detailed_overview: per-CAS, each chemical's own RAW colour (not the hom-mat
        # mixture-rule result) - same as option A's own output.
        c2c_assessment_all_scenarios_df = build_c2c_assessment_df(all_scenarios_df, db_path, include_mixture_rule_db_details=True)
        save_c2c_detailed_overview_output(c2c_assessment_all_scenarios_df, saving_dir, file_name, time)
        print("Saved C2C assessment (detailed_overview) for all scenarios under: ", saving_dir)
    print("--------------------------------------------------------------")
    print("Do you want to save selected scenarios? (y/n)")
    user_input = input("").strip().lower()
    if user_input == "y":
        chosen = select_scenarios(scenario_ids)
        selected_df = build_selected_scenarios_df(df, scenarios, chosen)
        selected_df.to_excel(saving_selected, index=False)
        print("Saved the selected scenarios to file: ", saving_selected)
        c2c_assessment_selected_scenarios_df = build_c2c_assessment_df(selected_df, db_path, include_mixture_rule_db_details=True)
        cas_list_selected = clean_cas_values(c2c_assessment_selected_scenarios_df["CAS"].tolist()) if "CAS" in c2c_assessment_selected_scenarios_df.columns else []
        _, missing_cas_selected_df = extract_colour_assessment_C2C(cas_list_selected, db_path)
        saved_selected_paths = save_c2c_assessment_output(c2c_assessment_selected_scenarios_df, missing_cas_selected_df, saving_dir, file_name, time)
        for p in saved_selected_paths:
            print("Saved C2C assessment for selected scenarios to file: ", p)
    print("--------------------------------------------------------------")
    print("Calculations finished. Have a nice day!")

def run_with_percentage_assessed():
    """CLI entry point (option A): prompt for the MAS Excel file and output folder, run the composition-percentage-only analysis (no mixture rules/DB), and save the per-CAS summary, %-assessed workbook, unique-CAS list, and optionally all-scenarios and/or user-selected-scenarios outputs."""
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
    file_stem = os.path.splitext(file_name)[0]
    saving_summary = os.path.join(saving_dir, f"summary_{time}_{file_stem}.xlsx")
    saving_percent_assessed = os.path.join(saving_dir, f"percent_assessed_{time}_{file_stem}.xlsx")
    saving_selected = os.path.join(saving_dir, f"selected_scenarios_{time}_{file_stem}.xlsx")
    saving_all_scenarios = os.path.join(saving_dir, f"all_scenarios_{time}_{file_stem}.xlsx")
    saving_CAS = os.path.join(saving_dir, f"CAS_{time}_{file_stem}.xlsx")
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
    """CLI entry point (option C, "Quick assessment"): prompt for the MAS Excel file, output folder and database, build all scenarios (no scenario selection), pull the raw per-CAS C2C colour assessment from the DB, and save the detailed_overview C2C assessment output (auto-split across files if needed)."""
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

    print("Pulling C2C colour assessment hazards from the DB and building the C2C assessment excel...")
    c2c_assessment_all_scenarios_df = build_c2c_assessment_df(all_scenarios_df, db_path)
    ### Saving: same as MAS_quick_C2C_assessment_static.py's own Quick Assessment (no row
    # cap, no bailing out to a different option - a project too big for one detailed_overview
    # file is split into several instead, same as save_c2c_assessment_output's own docstring).
    now = datetime.now()
    time = now.strftime("%Y%m%d")
    cas_list_all = clean_cas_values(c2c_assessment_all_scenarios_df["CAS"].tolist()) if "CAS" in c2c_assessment_all_scenarios_df.columns else []
    _, missing_cas_all_df = extract_colour_assessment_C2C(cas_list_all, db_path)
    saved_paths = save_c2c_assessment_output(
        c2c_assessment_all_scenarios_df, missing_cas_all_df, saving_dir, file_name, time,
        mixture_rules_ran=False, name_base="C2C_quick_assessment"
    )
    for p in saved_paths:
        print("Saved: ", p)
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
                       "C: Quick assessment \n"
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