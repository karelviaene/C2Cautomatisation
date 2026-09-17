### Quick C2C Assessment - STATIC VALUES (no Excel formulas)
### Same output as MAS_quick_C2C_assessment.py (same 4 sheets, same headers,
### same colour conditional formatting) but the "overview", "percentage_assessed"
### and "risk_assessed" sheets are computed here in pandas and written as plain
### values, instead of being computed by heavy LET/LAMBDA/FILTER/UNIQUE array
### formulas inside Excel. That was crashing Excel on real projects around
### 30,000 rows even after shrinking the formulas' scan range - since there is
### no Excel calculation happening at all here, there is no such row cap: this
### script only writes data + light conditional formatting, which Excel handles
### fine even well beyond that size.
### Kept as a SEPARATE script from MAS_quick_C2C_assessment.py on purpose, so
### the formula-based version stays available if this one turns out to have
### replicated the Excel logic incorrectly somewhere.
### If MAS_automation_with_mixture_rules_current.py's shared logic changes
### (column names, template layout, DB schema, CAS cleaning), mirror the
### change here too - this file intentionally duplicates that logic rather
### than importing it.

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
import sqlite3
import openpyxl
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils import get_column_letter
from openpyxl.styles import PatternFill, Font, Alignment

########################################################################
### C2C ASSESSMENT EXCEL TEMPLATE
### Only used here for its sheet names/order, headers (row 1) and column
### widths - NOT for its formulas, which this script never touches; the
### "overview"/"percentage_assessed"/"risk_assessed" sheets' formula rows
### are deleted and replaced with plain computed values instead. Keep in
### sync with MAS_automation_with_mixture_rules_current.py if the template
### or build_c2c_assessment_df()'s column layout ever changes.
C2C_ASSESSMENT_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "templates", "C2C_assessment_template.xlsx"
)
### Conditional formatting is re-applied fresh over this many rows on every
### save (cheap, unlike formulas) so it always covers the actual data.
CONDITIONAL_FORMATTING_ROW_HEADROOM = 200000
### Excel itself (not this script) starts struggling with very large sheets of
### plain data too, at large enough sizes - if "detailed_overview" would need
### row_count at or above this, save_c2c_assessment_output() splits the output
### into a summary file (overview/percentage_assessed/risk_assessed, always
### computed from the FULL data regardless of the split) plus one
### detailed_overview file per product - and, if even a single product's data
### is still too big, further batched by scenario within that product.
DETAILED_OVERVIEW_ROW_CAP = 50000
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
        initialdir=default_path   #  key line
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
### calculate CAS numebrs unique:
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

### Build a C2C assessment df (product/material/contribution cols + DB hazards) from a scenarios df
def build_c2c_assessment_df(scenarios_df, db_path):
    """
    Take a scenarios df (all scenarios) and keep only the
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

    cas_list = clean_cas_values(c2c_df["CAS"].tolist())
    hazards_df, missing_cas_df = extract_colour_assessment_C2C(cas_list, db_path)
    # missing_cas_df is no longer printed here - it's written into the overview
    # sheet's "Flagged issues:" column instead, see build_overview_df()

    c2c_df = c2c_df.merge(hazards_df, on="CAS", how="left")

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
    for col in c2c_df.columns:
        if col.startswith("C2C_assessment_"):
            rename_map[col] = col.replace("_", " ")
    c2c_df = c2c_df.rename(columns=rename_map)

    return c2c_df, missing_cas_df

########################################################################
### STATIC (pandas-computed) replacements for the template's Excel formulas
########################################################################

# Order matches the 21 columns produced by extract_colour_assessment_C2C() /
# build_c2c_assessment_df(), i.e. detailed_overview columns N..AH.
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

# these hazards were computed via mixture rules in the full (option B) analysis;
# this quick/no-mixture-rules assessment only has the raw per-substance value for
# them, so the template (and this script) labels them accordingly
WITHOUT_MIXTURE_RULES_COLS = {
    "C2C assessment oral toxicity",
    "C2C assessment inhalative toxicity",
    "C2C assessment dermal toxicity",
    "C2C assessment skin eye respiratory corrosion irritation",
    "C2C assessment sensitization",
    "C2C assessment fish toxicity",
    "C2C assessment invertebrate toxicity",
    "C2C assessment algae toxicity",
}

COLOUR_RANK = {"GREEN": 1, "YELLOW": 2, "GREY": 3, "RED": 4}
RANK_TO_COLOUR = {v: k for k, v in COLOUR_RANK.items()}

# detailed_overview column names (post build_c2c_assessment_df rename)
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
    """Same logic as the template's SEARCH("RED"/"YELLOW"/"GREEN",...) cascade: substring match, default GREY."""
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


def _display_colour(hazard_col, colour):
    if hazard_col in WITHOUT_MIXTURE_RULES_COLS:
        return f"WITHOUT MIXTURE RULES: {colour}"
    return colour


def _join_unique(values):
    return ", ".join(dict.fromkeys(v for v in values if v is not None and v != ""))


def _pct_assessed_by_group(active_df, group_cols, min_col, max_col, group_index):
    """MIN(1 - sum(min_col over 'not assessed' rows), 1 - sum(max_col over 'not assessed' rows)), per group."""
    not_assessed = active_df[active_df[COL_CAS] == "not assessed"]
    sum_min = not_assessed.groupby(group_cols, sort=False)[min_col].sum()
    sum_max = not_assessed.groupby(group_cols, sort=False)[max_col].sum()
    sum_min = sum_min.reindex(group_index, fill_value=0)
    sum_max = sum_max.reindex(group_index, fill_value=0)
    return pd.concat([1 - sum_min, 1 - sum_max], axis=1).min(axis=1)


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


PCT_ASSESSED_FLAG_OK = "OK"


def _build_flagged_issues_by_product(active_df, products_index, missing_cas_df):
    """
    Per Product, flag two known data-quality issues that would otherwise silently
    distort the % assessed calculation:
    1. Some active material has no % composition (NaN in its min/max contribution
       columns) - pandas' sum(skipna=True) then quietly drops it instead of counting
       it, making % assessed look better than it actually is.
    2. Some active material's CAS was not found in COLOUR_ASSESSMENT_C2C (missing_cas_df).
    Returns two lists (same order as products_index): the % assessed flag text
    (naming each affected material as "CAS (Final Material Map)", or "OK" if
    none), and the missing-CAS list text (or "").
    """
    composition_cols = [COL_MIN_PROD, COL_MAX_PROD, COL_MIN_HOM, COL_MAX_HOM]
    composition_missing = active_df[composition_cols].isna().any(axis=1)
    missing_material_cols = [COL_PRODUCT, COL_CAS, COL_FINAL_MATERIAL_MAP]
    missing_materials_df = active_df.loc[composition_missing, missing_material_cols].drop_duplicates()
    missing_materials_df["_label"] = missing_materials_df[COL_CAS] + " (" + missing_materials_df[COL_FINAL_MATERIAL_MAP] + ")"
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
        cas_flags.append(", ".join(missing_here))
    return pct_flags, cas_flags


def build_overview_df(detailed_df, missing_cas_df=None):
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

    for hazard_col, suffix in zip(HAZARD_COLS_READABLE, SCENARIO_ID_SUFFIXES):
        colours, scenario_lists = _worst_colour_by_group(
            active_df, [COL_PRODUCT, COL_HOM_MAT], hazard_col, idx_ph, with_scenarios=True
        )
        right_df[hazard_col] = [_display_colour(hazard_col, c) for c in colours.values]
        right_df[f"Scenario ID_{suffix}"] = scenario_lists.values

    return left_df, right_df


def build_percentage_assessed_df(detailed_df):
    active_df = detailed_df[detailed_df[COL_ACTIVE] == True].copy()

    # ---- left block: % assessed per (Product, Scenario ID) - same formula as overview's per-scenario step ----
    idx_ps = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    pct_by_ps = _pct_assessed_by_group(active_df, [COL_PRODUCT, COL_SCENARIO_ID], COL_MIN_PROD, COL_MAX_PROD, idx_ps)
    left_df = pct_by_ps.rename("% assessed").reset_index()
    left_df.columns = ["Product", "Scenario ID", "% assessed"]

    # ---- right block: % assessed per (Product, Homogenous Material, Scenario ID) ----
    idx_phs = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    pct_by_phs = _pct_assessed_by_group(active_df, [COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], COL_MIN_HOM, COL_MAX_HOM, idx_phs)
    right_df = pct_by_phs.rename("% assessed").reset_index()
    right_df.columns = ["Product", "Homogenous Material", "Scenario ID", "% assessed"]

    return left_df, right_df


def build_risk_assessed_df(detailed_df):
    active_df = detailed_df[detailed_df[COL_ACTIVE] == True].copy()

    idx_phs = pd.MultiIndex.from_frame(
        detailed_df[[COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID]].dropna(subset=[COL_PRODUCT]).drop_duplicates()
    )
    df = pd.DataFrame(index=idx_phs).reset_index()
    df.columns = ["Product", "Homogenous Material", "Scenario ID"]

    # Min/Max % Homogenous material in Product - taken over ALL rows (no active filter), like MINIFS/MAXIFS
    grp_all = detailed_df.groupby([COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], sort=False)
    min_pct = grp_all[COL_MIN_PCT_HOMMAT_IN_PROD].min().reindex(idx_phs)
    max_pct = grp_all[COL_MAX_PCT_HOMMAT_IN_PROD].max().reindex(idx_phs)
    df.insert(1, "Min % Homogenous material in Product", min_pct.values)
    df.insert(2, "Max % Homogenous material in Product", max_pct.values)

    for hazard_col in HAZARD_COLS_READABLE:
        colours, _ = _worst_colour_by_group(
            active_df, [COL_PRODUCT, COL_HOM_MAT, COL_SCENARIO_ID], hazard_col, idx_phs, with_scenarios=False
        )
        df[hazard_col] = [_display_colour(hazard_col, c) for c in colours.values]

    return df


def _clear_sheet_rows(ws):
    """Delete every row below the header - call ONCE per sheet before writing any dataframe to it."""
    if ws.max_row > 1:
        ws.delete_rows(2, ws.max_row - 1)


def _write_df_to_sheet_by_header(ws, df, start_col=1, end_col=None):
    """Write df starting at row 2, matching columns by the sheet's row-1 header text (leaves unmapped/blank spacer columns untouched).
    Does NOT clear existing rows first - call _clear_sheet_rows(ws) once before writing one or more dataframes to the same sheet.
    "overview" and "percentage_assessed" reuse the same header text ("Product", "Scenario ID", "% assessed") for both
    their left and right blocks, so header matching MUST be scoped to that block's column range (start_col/end_col,
    1-indexed, inclusive) - otherwise a plain header->column dict collapses to just the last matching column."""
    if end_col is None:
        end_col = ws.max_column
    header_to_col = {
        cell.value: cell.column
        for cell in ws[1]
        if cell.value is not None and start_col <= cell.column <= end_col
    }

    for row_offset, row in enumerate(df.itertuples(index=False), start=2):
        row_dict = dict(zip(df.columns, row))
        for header, value in row_dict.items():
            col_idx = header_to_col.get(header)
            if col_idx is None:
                continue
            if value is None or (isinstance(value, float) and pd.isna(value)):
                value = None
            ws.cell(row=row_offset, column=col_idx, value=value)


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


def _style_overview_sheet(ws, last_col, last_data_row):
    """
    Uniform column widths (so nothing looks randomly wider/narrower), header row
    text-wrapped (so long headers don't force a huge column width), data rows NOT
    wrapped, and the spacer columns (C and G) kept narrow so they read as clean
    separators instead of swallowing overflow text from the column before them.
    """
    UNIFORM_WIDTH = 22
    SPACER_WIDTH = 3
    SPACER_COLS = {3, 7}

    for col in range(1, last_col + 1):
        letter = get_column_letter(col)
        ws.column_dimensions[letter].width = SPACER_WIDTH if col in SPACER_COLS else UNIFORM_WIDTH

    header_alignment = Alignment(wrap_text=True, vertical="bottom")
    data_alignment = Alignment(wrap_text=False)
    ws.row_dimensions[1].height = 45  # give wrapped header text room to show on 2-3 lines
    for col in range(1, last_col + 1):
        ws.cell(row=1, column=col).alignment = header_alignment
    for row in range(2, last_data_row + 1):
        for col in range(1, last_col + 1):
            ws.cell(row=row, column=col).alignment = data_alignment


def _apply_colour_conditional_formatting_cols(ws, col_letters, last_row):
    """Non-contiguous variant, one column at a time - use for sheets like "overview" where hazard-colour
    columns are interleaved with plain-text "Scenario ID_x" columns (a SEARCH("red",...) sweep across a
    contiguous range spanning both could false-positive on a scenario name containing "red" as a substring)."""
    ws.conditional_formatting._cf_rules.clear()
    for keyword, (fill, font) in _COLOUR_STYLES.items():
        for col in col_letters:
            formula = f'ISNUMBER(SEARCH("{keyword}",{col}2))'
            ws.conditional_formatting.add(f"{col}2:{col}{last_row}", FormulaRule(formula=[formula], fill=fill, font=font, stopIfTrue=False))


def save_c2c_assessment_workbook_static(
    c2c_df, missing_cas_df, output_path, template_path=C2C_ASSESSMENT_TEMPLATE_PATH, write_detailed=True
):
    """
    Copy templates/C2C_assessment_template.xlsx to output_path, but instead of
    generating Excel formulas for "overview"/"percentage_assessed"/
    "risk_assessed", compute their values in pandas here and write them as
    plain data - same sheets, same headers, same colour highlighting, no
    Excel-side calculation at all, so no formula-driven row cap is needed.
    Two extra columns are inserted at the front of the "overview" sheet's
    left block (before "Product", with a spacer column after them):
    "Flagged for % assessed:" (red text if some material is missing its %
    composition, else "OK") and "C2C hazard assessment missing CAS:" (the
    CAS numbers not found in the DB, per product).

    overview/percentage_assessed/risk_assessed are always computed from the
    FULL c2c_df, regardless of write_detailed. Pass write_detailed=False for
    a "summary only" file - the "detailed_overview" sheet/tab is then
    dropped from this file entirely (its data lives in the separate
    detailed_overview file(s) - see save_c2c_assessment_output()).
    """
    if not os.path.exists(template_path):
        raise FileNotFoundError(
            f"C2C assessment template not found at: {template_path}\n"
            "Check C2C_ASSESSMENT_TEMPLATE_PATH at the top of this file."
        )

    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)

    ws_detail = wb["detailed_overview"]
    template_headers = [cell.value for cell in ws_detail[1] if cell.value is not None]
    df_headers = list(c2c_df.columns)
    if template_headers != df_headers:
        print(
            "[WARNING] detailed_overview headers no longer match the template.\n"
            f"  template: {template_headers}\n"
            f"  data:     {df_headers}\n"
            "The static overview/percentage_assessed/risk_assessed builders key off these "
            "exact column names - update them (and/or the template) together."
        )

    if write_detailed:
        _clear_sheet_rows(ws_detail)
        _write_df_to_sheet_by_header(ws_detail, c2c_df)
    else:
        del wb["detailed_overview"]
        ws_detail = None

    # insert 3 new leading columns in "overview": the 2 flag columns + a spacer before
    # "Product" (shifts everything else in that sheet right by 3 - only this in-memory
    # copy is touched, not the shared template file on disk)
    ws_overview = wb["overview"]
    ws_overview.insert_cols(1, amount=3)
    bold = Font(bold=True)
    header_cell_pct = ws_overview.cell(row=1, column=1, value="Flagged for % assessed:")
    header_cell_pct.font = bold
    header_cell_cas = ws_overview.cell(row=1, column=2, value="C2C hazard assessment missing CAS:")
    header_cell_cas.font = bold
    # column 3 stays blank - the spacer before "Product"

    overview_left, overview_right = build_overview_df(c2c_df, missing_cas_df)
    percentage_left, percentage_right = build_percentage_assessed_df(c2c_df)
    risk_df = build_risk_assessed_df(c2c_df)

    # "overview": left block = cols A-G (1-7, incl. the 2 new flag cols, the new spacer,
    # Product/% assessed/Scenario ID, and the original spacer), right block = cols H-AY
    # (8-51) - both reuse header text like "Product"/"Scenario ID", so each write must
    # stay within its own range
    _clear_sheet_rows(ws_overview)
    _write_df_to_sheet_by_header(ws_overview, overview_left, start_col=1, end_col=7)
    _write_df_to_sheet_by_header(ws_overview, overview_right, start_col=8, end_col=51)

    # red text for the "Flagged for % assessed:" column wherever it isn't "OK"
    red_font = Font(color="FF0000")
    for row_offset in range(2, 2 + len(overview_left)):
        cell = ws_overview.cell(row=row_offset, column=1)
        if cell.value and cell.value != PCT_ASSESSED_FLAG_OK:
            cell.font = red_font

    # "percentage_assessed": left block = cols A-C (1-3), right block = cols E-H (5-8) - same
    # header-reuse issue ("Product", "Scenario ID", "% assessed" appear in both blocks)
    ws_pct = wb["percentage_assessed"]
    _clear_sheet_rows(ws_pct)
    _write_df_to_sheet_by_header(ws_pct, percentage_left, start_col=1, end_col=3)
    _write_df_to_sheet_by_header(ws_pct, percentage_right, start_col=5, end_col=8)

    ws_risk = wb["risk_assessed"]
    _clear_sheet_rows(ws_risk)
    _write_df_to_sheet_by_header(ws_risk, risk_df)

    n_rows = max(len(overview_right), len(risk_df), 1)
    last_row = n_rows + 1 + 100
    if write_detailed:
        _apply_colour_conditional_formatting(ws_detail, "N", "AH", len(c2c_df) + 1 + 100)
    # overview's hazard-value columns (now J, L, N, ... AX after the 3-column insert) are
    # interleaved with "Scenario ID_x" text columns (K, M, O, ...) - use the column-list
    # variant so those never get swept in
    overview_hazard_cols = [get_column_letter(c) for c in range(10, 51, 2)]  # J, L, N, ..., AX
    _apply_colour_conditional_formatting_cols(ws_overview, overview_hazard_cols, last_row)
    _apply_colour_conditional_formatting(wb["risk_assessed"], "F", "Z", last_row)

    overview_last_data_row = max(len(overview_left), len(overview_right), 1) + 1
    _style_overview_sheet(ws_overview, last_col=51, last_data_row=overview_last_data_row)

    wb.save(output_path)


def save_detailed_overview_only(detail_df, output_path, template_path=C2C_ASSESSMENT_TEMPLATE_PATH):
    """
    Write just the "detailed_overview" sheet (data + colour conditional formatting) for a
    subset of the full data - the "overview"/"percentage_assessed"/"risk_assessed" sheets
    are dropped entirely from this file, since they live in the separate summary file that
    save_c2c_assessment_output() always builds from the FULL (unsplit) data.
    """
    if not os.path.exists(template_path):
        raise FileNotFoundError(
            f"C2C assessment template not found at: {template_path}\n"
            "Check C2C_ASSESSMENT_TEMPLATE_PATH at the top of this file."
        )

    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)

    for sheet_name in ("overview", "percentage_assessed", "risk_assessed"):
        del wb[sheet_name]

    ws_detail = wb["detailed_overview"]
    _clear_sheet_rows(ws_detail)
    _write_df_to_sheet_by_header(ws_detail, detail_df)

    last_row = len(detail_df) + 1 + 100
    _apply_colour_conditional_formatting(ws_detail, "N", "AH", last_row)

    wb.save(output_path)


def _sanitize_filename_part(text):
    return re.sub(r'[\\/*?:"<>|]', "_", str(text)).strip() or "unnamed"


def _split_scenarios_into_batches(df_p, row_cap):
    """
    Split one product's rows into consecutive scenario batches, each staying under
    row_cap rows where possible. Returns [(scenario_ids, start_idx, end_idx), ...] with
    1-based start_idx/end_idx (this product's own scenario numbering, for filenames like
    "..._scenarios_1-100"). A single scenario that alone exceeds the cap still gets its own
    (oversized) batch - a scenario's rows are never split apart.
    """
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


def save_c2c_assessment_output(c2c_df, missing_cas_df, saving_dir, file_name, date_str, template_path=C2C_ASSESSMENT_TEMPLATE_PATH):
    """
    Save the C2C assessment:
    - "C2C_assessment_<file>_<date>.xlsx", directly in saving_dir: ALWAYS just
      overview/percentage_assessed/risk_assessed (computed from the FULL data) -
      detailed_overview is never in this file.
    - detailed_overview, always in its own file(s), inside a new subfolder
      "detailed_assessment_<file>_<date>" under saving_dir:
        - fits under DETAILED_OVERVIEW_ROW_CAP rows as a single file:
          "C2C_assessment_detailed_overview_<file>_<date>.xlsx" (all products together).
        - otherwise: one "C2C_assessment_detailed_overview_<product>_scenarios_<start>-<end>_<file>_<date>.xlsx"
          per product (further split into multiple scenario-range batches if even a single
          product's own data is still too big for one file).
    Returns the list of saved file paths.
    """
    file_stem = os.path.splitext(file_name)[0]
    saved_paths = []

    # ---- summary file: always just the 3 summary sheets, always from the FULL data ----
    summary_path = os.path.join(saving_dir, f"C2C_assessment_{file_stem}_{date_str}.xlsx")
    save_c2c_assessment_workbook_static(c2c_df, missing_cas_df, summary_path, template_path=template_path, write_detailed=False)
    saved_paths.append(summary_path)

    # ---- detailed_overview: always a separate file (or files), in its own subfolder ----
    detail_dir = os.path.join(saving_dir, f"detailed_assessment_{file_stem}_{date_str}")
    os.makedirs(detail_dir, exist_ok=True)

    total_rows = len(c2c_df)
    if total_rows < DETAILED_OVERVIEW_ROW_CAP:
        detail_path = os.path.join(detail_dir, f"C2C_assessment_detailed_overview_{file_stem}_{date_str}.xlsx")
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
                f"C2C_assessment_detailed_overview_{prod_label}_scenarios_1-{n_scenarios}_{file_stem}_{date_str}.xlsx",
            )
            save_detailed_overview_only(df_p, out_path, template_path=template_path)
            saved_paths.append(out_path)
            continue

        for scenario_ids, start_idx, end_idx in _split_scenarios_into_batches(df_p, DETAILED_OVERVIEW_ROW_CAP):
            df_batch = df_p[df_p[COL_SCENARIO_ID].isin(scenario_ids)]
            out_path = os.path.join(
                detail_dir,
                f"C2C_assessment_detailed_overview_{prod_label}_scenarios_{start_idx}-{end_idx}_{file_stem}_{date_str}.xlsx",
            )
            save_detailed_overview_only(df_batch, out_path, template_path=template_path)
            saved_paths.append(out_path)

    return saved_paths


#################################################################
### Smaller projects: C2C assessment only, no mixture rules, static values
def run_quick_c2c_assessment_static():
    print("--------------------------------------------------------------")
    print("Quick C2C Assessment (no mixture rules, static values - no Excel formulas)")
    print("--------------------------------------------------------------")
    print("Select the Excel file (MAS) to analyse.")
    df, file_name, default_folder = open_excel_file()
    print("--------------------------------------------------------------")
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
    print(f"Detailed rows generated: {len(all_scenarios_df)}")
    print("--------------------------------------------------------------")

    print("Pulling C2C colour assessment hazards from the DB and building the C2C assessment excel...")
    c2c_assessment_all_scenarios_df, missing_cas_df = build_c2c_assessment_df(all_scenarios_df, db_path)
    ### Saving:
    now = datetime.now()
    time = now.strftime("%Y%m%d")
    saved_paths = save_c2c_assessment_output(c2c_assessment_all_scenarios_df, missing_cas_df, saving_dir, file_name, time)
    for p in saved_paths:
        print("Saved: ", p)
    print("--------------------------------------------------------------")
    print("Calculations finished. Have a nice day!")


if __name__ == "__main__":
    run_quick_c2c_assessment_static()
