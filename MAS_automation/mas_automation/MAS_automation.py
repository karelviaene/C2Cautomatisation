### Files to import
import pandas as pd
import numpy as np
import itertools
import tkinter as tk
from tkinter import filedialog
import re
import os
from datetime import datetime

### Adjust cols names if the template changes
#############################################
### MATERIAL: "Tier 1 Material" etc., {i} is always used for the number
### But at all tiers, the format needs to be the same as all functions loop it
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
min_percent_in_hom_mat = "Min % Tier 1 material in Homogenous material"
max_percent_in_hom_mat = "Max % Tier 1 material in Homogenous material"
min_weight_in_hom_mat = "Min weight Tier 1 material in Homogenous material"
max_weight_in_hom_mat = "Max weight Tier 1 material in Homogenous material"
col_mat_tier_1 = "Tier 1 Material"
col_min_perc = "Tier {i} Material Weight% Min"
col_max_perc = "Tier {i} Material Weight% Max"
#############################################
##### FUNCTIONS ####
### Read the file from the selected excel:
def open_excel_file():
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
        return None
### Select folder to save data:
def select_folder():
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    root.attributes("-topmost", True)  # Bring dialog to front

    folder_path = filedialog.askdirectory(title="Select a Folder to save data in")

    if folder_path:
        return folder_path
    else:
        print("No folder selected.")
        return None
### Clean data: add a col row_id for an identifier & normalize Y/N in capital letters etc
def clean_data(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # normalizing yes/no to be case sensitive
    mapping = {
        "yes": "yes",
        "no": "no",
        "Yes": "yes",
        "No": "no"
    }
    df = df.apply(lambda col: col.map(mapping).fillna(col) if col.dtype == "object" else col)
    # add a row with an ID for each material
    df["row_id"] = range(1, len(df) + 1)
    # clean empty spaces (before and after)
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.strip()
    return df
## getting the highest tier available
def get_highest_tier(df):
    col_names = df.columns
    numbers = []
    for col in col_names:
        match = re.search(r"CAS Tier (\d+)", col)
        if match:
            numbers.append(int(match.group(1)))
    highest = max(numbers) if numbers else None
    return highest
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
def add_helper_columns(df):
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

    return " → ".join(path) if path else None
def add_final_map(df):
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
    alt_choices = {}
    # loop over:
    for i in range(1, tier_level + 1):
        group_col = f"t{i}_alt_group"
        material_col = col_mat.format(i=i)

        subset = df.dropna(subset=[group_col])

        for group, grp in subset.groupby(group_col):
            choices = grp[material_col].dropna().unique().tolist()
            alt_choices[group] = sorted(choices)

    # if no alternatives found
    if not alt_choices:
        return [{"scenario_id": "base", "choices": {}}]

    group_names = list(alt_choices.keys())
    scenarios = []

    for i, combo in enumerate(
        itertools.product(*(alt_choices[g] for g in group_names)),
        start=1
    ):
        choices = dict(zip(group_names, combo))
        scenarios.append({
            "scenario_id": f"scenario_{i}",
            "choices": choices
        })

    return scenarios
### Check if the row is active (if the materials are to be included in the scenario or not)
def row_is_active(row, scenario, selected_materials, tier_level=10):
    # make sure if there are substances coupled to an alternative then you find it here:
    for i in range(1, tier_level + 1):
        # Alternative filtering
        alt_group_col = f"t{i}_alt_group"
        material_col = col_mat.format(i=i)

        if pd.notna(row.get(alt_group_col)):
            chosen = scenario["choices"].get(row[alt_group_col])
            if row.get(material_col) != chosen:
                return False, f"Excluded by Tier {i} alternative"

        # Coupling rule
        coupling_col1 = col_coupling_y_n.format(i=i)
        coupling_col2 = col_coupling_material.format(i=i)
        coupled_material = row.get(coupling_col2)
        coupling_yes_no = row.get(coupling_col1)

        if coupling_yes_no == "yes" or coupling_yes_no == "Yes" or coupling_yes_no == "yes " or coupling_yes_no == "Yes ":
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
def evaluate_row_activity(df, scenario):
    df = df.copy()
    selected_materials = set(scenario["choices"].values())
    active_flags = []
    reasons = []

    for _, row in df.iterrows():
        active, reason = row_is_active(row, scenario, selected_materials)
        active_flags.append(active)
        reasons.append(reason)

    df["scenario_id"] = scenario["scenario_id"]
    df["active"] = active_flags
    df["status_reason"] = reasons

    scenario_df = df.copy()
    return scenario_df
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
def analyse_the_dataset(df, scenarios):
    metrics = [
        "min_contribution_prod",
        "max_contribution_prod",
        "min_contribution_hom_mat",
        "max_contribution_hom_mat"
    ]

    summary = {}
    scenario_extremes = {}


    for scenario in scenarios:
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
            ["scenario_id", "CAS", "min_contribution_prod", "max_contribution_prod"]
        ].copy()

        # Convert to numeric (important because of commas)
        for col in ["min_contribution_prod", "max_contribution_prod"]:
            current[col] = pd.to_numeric(
                current[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )

        # Sum per scenario:

        # get the name of each scenario
        scenario_id = current["scenario_id"].iloc[0]

        # Filter out rows where CAS is "not assessed" not to account for them in the % assessed
        current = current[current["CAS"].str.strip().str.lower() != "not assessed"]

        sum_min = current["min_contribution_prod"].sum(skipna=True)
        sum_max = current["max_contribution_prod"].sum(skipna=True)

        rec = scenario_extremes.setdefault("global", {})

        # MIN of min_contribution_prod (worst-case lowest)
        if "abs_min_sum_min_prod" not in rec or sum_min < rec["abs_min_sum_min_prod"]:
            rec["abs_min_sum_min_prod"] = sum_min
            rec["abs_min_sum_min_prod_scenario"] = scenario_id

        # MAX of min_contribution_prod
        if "abs_max_sum_min_prod" not in rec or sum_min > rec["abs_max_sum_min_prod"]:
            rec["abs_max_sum_min_prod"] = sum_min
            rec["abs_max_sum_min_prod_scenario"] = scenario_id

        # MIN of max_contribution_prod
        if "abs_min_sum_max_prod" not in rec or sum_max < rec["abs_min_sum_max_prod"]:
            rec["abs_min_sum_max_prod"] = sum_max
            rec["abs_min_sum_max_prod_scenario"] = scenario_id

        # MAX of max_contribution_prod
        if "abs_max_sum_max_prod" not in rec or sum_max > rec["abs_max_sum_max_prod"]:
            rec["abs_max_sum_max_prod"] = sum_max
            rec["abs_max_sum_max_prod_scenario"] = scenario_id

        scenario_summaries[scenario_id] = {"scenario_id": scenario_id, "sum_min_contribution_prod": sum_min, "sum_max_contribution_prod": sum_max}

    # summary per each CAS
    summary_df = (pd.DataFrame(summary.values()).sort_values("row_id").reset_index(drop=True))
    # best & worst case perecentage assessed:
    perecentage_assessed = pd.DataFrame([scenario_extremes["global"]])

    return summary_df, perecentage_assessed
def analyse_the_dataset(df, scenarios):
    metrics = [
        "min_contribution_prod",
        "max_contribution_prod",
        "min_contribution_hom_mat",
        "max_contribution_hom_mat"
    ]

    summary = {}
    scenario_extremes = {}


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
            ["scenario_id", "CAS", "min_contribution_prod", "max_contribution_prod"]
        ].copy()

        # Convert to numeric (important because of commas)
        for col in ["min_contribution_prod", "max_contribution_prod"]:
            current[col] = pd.to_numeric(
                current[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )

        # Sum per scenario:

        # get the name of each scenario
        scenario_id = scenario["scenario_id"]

        # Normalize CAS column once
        cas_clean = current["CAS"].str.strip().str.lower().fillna("")

        # Split datasets
        not_assessed_df = current[cas_clean == "not assessed"]
        assessed_df = current[cas_clean != "not assessed"]

        # --- CALC WITH NOT ASSESSED ---
        sum_min_not_assessed = not_assessed_df["min_contribution_prod"].sum(skipna=True)
        sum_max_not_assessed = not_assessed_df["max_contribution_prod"].sum(skipna=True)

        sum_min_calc_w_not_assessed = 1 - sum_max_not_assessed
        sum_max_calc_w_not_assessed = 1 - sum_min_not_assessed

        # --- CALC WITH ASSESSED ---
        sum_min_assessed = assessed_df["min_contribution_prod"].sum(skipna=True)
        sum_max_assessed = assessed_df["max_contribution_prod"].sum(skipna=True)

        sum_min_calc_w_assessed = sum_min_assessed
        sum_max_calc_w_assessed = sum_max_assessed

        rec = scenario_extremes.setdefault("global", {})

        # =========================
        # NOT ASSESSED TRACKING
        # =========================

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

        # =========================
        # ASSESSED TRACKING
        # =========================

        if "abs_min_sum_min_prod_calc_w_assessed" not in rec or \
                sum_min_calc_w_assessed < rec["abs_min_sum_min_prod_calc_w_assessed"]:
            rec["abs_min_sum_min_prod_calc_w_assessed"] = sum_min_calc_w_assessed
            rec["abs_min_sum_min_prod_calc_w_assessed_scenario"] = scenario_id

        if "abs_max_sum_min_prod_calc_w_assessed" not in rec or \
                sum_min_calc_w_assessed > rec["abs_max_sum_min_prod_calc_w_assessed"]:
            rec["abs_max_sum_min_prod_calc_w_assessed"] = sum_min_calc_w_assessed
            rec["abs_max_sum_min_prod_calc_w_assessed_scenario"] = scenario_id

        if "abs_min_sum_max_prod_calc_w_assessed" not in rec or \
                sum_max_calc_w_assessed < rec["abs_min_sum_max_prod_calc_w_assessed"]:
            rec["abs_min_sum_max_prod_calc_w_assessed"] = sum_max_calc_w_assessed
            rec["abs_min_sum_max_prod_calc_w_assessed_scenario"] = scenario_id

        if "abs_max_sum_max_prod_calc_w_assessed" not in rec or \
                sum_max_calc_w_assessed > rec["abs_max_sum_max_prod_calc_w_assessed"]:
            rec["abs_max_sum_max_prod_calc_w_assessed"] = sum_max_calc_w_assessed
            rec["abs_max_sum_max_prod_calc_w_assessed_scenario"] = scenario_id

        scenario_summaries[scenario_id] = {
            "scenario_id": scenario_id,

            # Assessed version
            "sum_min_contribution_prod_calc_w_assessed": sum_min_calc_w_assessed,
            "sum_max_contribution_prod_calc_w_assessed": sum_max_calc_w_assessed,

            # Not assessed version
            "sum_min_contribution_prod_calc_w_not_assessed": sum_min_calc_w_not_assessed,
            "sum_max_contribution_prod_calc_w_not_assessed": sum_max_calc_w_not_assessed
        }
        # Filter out rows where CAS is "not assessed" not to account for them in the % assessed
        # current = current[current["CAS"].str.strip().str.lower() != "not assessed"]
        #
        # sum_min = current["min_contribution_prod"].sum(skipna=True)
        # sum_max = current["max_contribution_prod"].sum(skipna=True)

        # rec = scenario_extremes.setdefault("global", {})

        # # MIN of min_contribution_prod (worst-case lowest)
        # if "abs_min_sum_min_prod" not in rec or sum_min < rec["abs_min_sum_min_prod"]:
        #     rec["abs_min_sum_min_prod"] = sum_min
        #     rec["abs_min_sum_min_prod_scenario"] = scenario_id
        #
        # # MAX of min_contribution_prod
        # if "abs_max_sum_min_prod" not in rec or sum_min > rec["abs_max_sum_min_prod"]:
        #     rec["abs_max_sum_min_prod"] = sum_min
        #     rec["abs_max_sum_min_prod_scenario"] = scenario_id
        #
        # # MIN of max_contribution_prod
        # if "abs_min_sum_max_prod" not in rec or sum_max < rec["abs_min_sum_max_prod"]:
        #     rec["abs_min_sum_max_prod"] = sum_max
        #     rec["abs_min_sum_max_prod_scenario"] = scenario_id
        #
        # # MAX of max_contribution_prod
        # if "abs_max_sum_max_prod" not in rec or sum_max > rec["abs_max_sum_max_prod"]:
        #     rec["abs_max_sum_max_prod"] = sum_max
        #     rec["abs_max_sum_max_prod_scenario"] = scenario_id
        #
        # scenario_summaries[scenario_id] = {"scenario_id": scenario_id, "sum_min_contribution_prod": sum_min,
        #                                    "sum_max_contribution_prod": sum_max}
    # summary per each CAS
    summary_df = (pd.DataFrame(summary.values()).sort_values("row_id").reset_index(drop=True))
    # best & worst case perecentage assessed:
    perecentage_assessed = pd.DataFrame([scenario_extremes["global"]])

    return summary_df, perecentage_assessed
# select scenarios (add that it prompts the user to choose which ones
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
### Start the program:
print("--------------------------------------------------------------")
print("Select the Excel file (MAS) to analyse.")
# open the program
df = open_excel_file()
print("--------------------------------------------------------------")
# Select folder for saving:
print("Select a folder you want to save your files in.")
saving = select_folder()
saving_dir = os.path.abspath(saving)
print("--------------------------------------------------------------")
print("Calculating...")
# standardize & clean the df
df = clean_data(df)
# calculate the maximum tier
max_tier = get_highest_tier(df)
print("Max Tier found: ", max_tier)
# max_tier = 5
# add columns for analysis
df = add_helper_columns(df)
df = add_final_map(df)
# identify alternatives & make scenarios
print("Generating scenarios...")
df = identify_alternative_groups(df, max_tier)
scenarios = generate_scenarios(df, max_tier)
scenario_ids = [x['scenario_id'] for x in scenarios]
print("Scenarios generated. Total number of scenarios: ", len(scenarios))
print("Calculating... This might take a while...")
# analyse the dataset: summary for each CAS & product assessed
summary_df, product_assessed_df = analyse_the_dataset(df, scenarios)
# all scenarios
all_scenarios_df = build_selected_scenarios_df(df, scenarios, scenario_ids)
### Saving:
now = datetime.now()
time = now.strftime("%Y%m%d")
saving_summary = os.path.join(saving_dir, f"summary_{time}.xlsx")
saving_percent_assessed = os.path.join(saving_dir, f"percent_assessed_{time}.xlsx")
saving_selected = os.path.join(saving_dir, f"selected_scenarios_{time}.xlsx")
saving_all_scenarios = os.path.join(saving_dir, f"all_scenarios_{time}.xlsx")
print("--------------------------------------------------------------")
summary_df.to_excel(saving_summary, index=False)
print("Saved summary per each CAS to file: ", saving_summary)
product_assessed_df.to_excel(saving_percent_assessed, index=False)
print("Saved percentage assessed to file: ", saving_percent_assessed)
print("--------------------------------------------------------------")
print("Do you want to save all scenarios? (y/n)")
user_input = input("").strip().lower()
if user_input == "y":
    print("Saving...")
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
