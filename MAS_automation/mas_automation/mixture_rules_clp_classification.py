###### Mixture rules #####

### Loading libraries ###
import pandas as pd
import numpy as np

####################################################################################
# FUNCTIONS

### Acute toxicity: oral, dermal, inhal ###
def acute_toxicity_clp_assessment(df_product,df_toxicity_info,endpoints_to_assess):
    """
    Calculate acute toxicity ATE and CLP classification for oral, dermal and inhalation endpoints.
    The function:
    1. Merges product composition with toxicity information.
    2. Calculates worst-case concentration per homogeneous material.
    3. Calculates ATE values for selected endpoints.
    4. Converts ATE values into CLP acute toxicity categories.
    5. Reports chemicals with missing ATE/LD50/LC50 values where classification is present.

    Returns
    final_df : pd.DataFrame
        One row per homogeneous material with calculated ATE and CLP category.

    unknown_chemicals_df : pd.DataFrame
        Chemicals for which no usable LD50/LC50/ATE value was available,
        but which were not explicitly "Not classified".
    """
    # names of the columns:
    cas_col = "CAS"
    hom_material_col = "Homogenous Material"
    min_contribution_col = "min_contribution_hom_mat"
    max_contribution_col = "max_contribution_hom_mat"

    # 1. Endpoint settings
    # Making dictionary that contains all endpoint-specific rules.
    # setting default values if ATE is not given
    # and setting the ATE thresholds for Cat. 1, Cat. 2, Cat. 3 and Cat. 4

    endpoint_settings = {
        "LD50_oral": {
            "clp_col": "CLP oral class",
            "exclusion_value": 2000,
            "default_ate": {
                "Tox. 1": 0.5,
                "Tox. 2": 5,
                "Tox. 3": 100,
                "Tox. 4": 500,
            },
            "ate_col": "ATE_based_on_LD50_oral",
            "classification_col": "Acute toxicity oral",
            "classification_limits": [
                (5, "Cat. 1"),
                (50, "Cat. 2"),
                (300, "Cat. 3"),
                (2000, "Cat. 4"),
            ],
        },

        "LD50_dermal": {
            "clp_col": "CLP dermal class",
            "exclusion_value": 2000,
            "default_ate": {
                "Tox. 1": 5,
                "Tox. 2": 50,
                "Tox. 3": 300,
                "Tox. 4": 1100,
            },
            "ate_col": "ATE_based_on_LD50_dermal",
            "classification_col": "Acute toxicity dermal",
            "classification_limits": [
                (50, "Cat. 1"),
                (200, "Cat. 2"),
                (1000, "Cat. 3"),
                (2000, "Cat. 4"),
            ],
        },

        "LC50_gas": {
            "clp_col": "CLP inhalation class",
            "exclusion_value": 20000,
            "default_ate": {
                "Tox. 1": 10,
                "Tox. 2": 100,
                "Tox. 3": 700,
                "Tox. 4": 4500,
            },
            "ate_col": "ATE_based_on_LC50_gas",
            "classification_col": "Acute toxicity inhalation (gases)",
            "classification_limits": [
                (100, "Cat. 1"),
                (500, "Cat. 2"),
                (2500, "Cat. 3"),
                (20000, "Cat. 4"),
            ],
        },

        "LC50_vapour": {
            "clp_col": "CLP inhalation class",
            "exclusion_value": 20,
            "default_ate": {
                "Tox. 1": 0.05,
                "Tox. 2": 0.5,
                "Tox. 3": 3,
                "Tox. 4": 5,
            },
            "ate_col": "ATE_based_on_LC50_vapour",
            "classification_col": "Acute toxicity inhalation (vapour)",
            "classification_limits": [
                (0.5, "Cat. 1"),
                (2, "Cat. 2"),
                (10, "Cat. 3"),
                (20, "Cat. 4"),
            ],
        },

        "LC50_dust_mist_aerosol": {
            "clp_col": "CLP inhalation class",
            "exclusion_value": 5,
            "default_ate": {
                "Tox. 1": 0.005,
                "Tox. 2": 0.05,
                "Tox. 3": 0.5,
                "Tox. 4": 1.5,
            },
            "ate_col": "ATE_based_on_LC50_dust_mist_aerosol",
            "classification_col": "Acute toxicity inhalation (dust/mist)",
            "classification_limits": [
                (0.05, "Cat. 1"),
                (0.5, "Cat. 2"),
                (1, "Cat. 3"),
                (5, "Cat. 4"),
            ],
        },
    }

    # 2. Function to classify calculated ATE value

    def classify_ate(ate_value, limits):
        '''The classification limits are endpoint-specific.
        Example for oral:
        ATE <= 5       -> Cat. 1
        ATE <= 50      -> Cat. 2
        ATE <= 300     -> Cat. 3
        ATE <= 2000    -> Cat. 4
        The function returns the first matching category.
        If ATE is missing or above Cat. 4 threshold, it returns pd.NA.
        '''
        if pd.isna(ate_value):
            return pd.NA

        for upper_limit, category in limits:
            if ate_value <= upper_limit:
                return category

        return pd.NA


    # 3. Prepare the df for calculations

    df_calc = pd.merge(df_product,df_toxicity_info,on=cas_col,how="left").copy()
    # choose worst case concentration
    df_calc["conc_hom_mat"] = df_calc[[min_contribution_col, max_contribution_col]].max(axis=1)

    # Convert concentration fraction to percentage
    df_calc["conc_hom_mat_percent"] = df_calc["conc_hom_mat"] * 100
    #print(df_calc)
    # Get all homogeneous materials to assess
    hom_materials = df_calc[hom_material_col].dropna().unique().tolist()

    # Prepare for output
    results = []
    unknown_chemicals = []

    # 4. Loop over each homogeneous material
    for hom_material in hom_materials:
        #print(hom_material)
        # Start the output row for this homogeneous material
        row_result = {"hom_material": hom_material}

        # Select only substances belonging to this homogeneous material
        df_hom_base = df_calc.loc[df_calc[hom_material_col] == hom_material].copy()

        # 5. Loop over selected toxicity endpoints: Example endpoints: LD50_oral, LD50_dermal, etc.
        for endpoint in endpoints_to_assess:
            #print(endpoint)
            # Skip if the endpoint
            if endpoint not in endpoint_settings:
                #print(f"Skipping unknown endpoint: {endpoint}")
                continue

            # Load all endpoint-specific settings
            settings = endpoint_settings[endpoint]

            clp_col = settings["clp_col"]
            default_ate = settings["default_ate"]
            ate_col = settings["ate_col"]
            classification_col = settings["classification_col"]
            classification_limits = settings["classification_limits"]

            # cleand the df and force the values to numeric
            df_hom = df_hom_base.copy()
            df_hom[endpoint] = pd.to_numeric(df_hom[endpoint],errors="coerce")
            # remove treshold value of 0.1%
            df_hom = df_hom.loc[df_hom["conc_hom_mat"] >= 0.001].copy()


            # Fill missing LD50/LC50 using CLP category
            for tox_cat, replacement_value in default_ate.items():
                mask = (
                    df_hom[endpoint].isna()
                    & df_hom[clp_col].astype(str).str.contains(
                        tox_cat,
                        na=False,
                        regex=False
                    )
                )

                df_hom.loc[mask, endpoint] = replacement_value

            # Identify unknown but relevant substances
            # These are substances where:
            # - concentration is above 10%
            # - LD50/LC50 is still missing
            # - CLP class is not "Not classified"
            unknown_mask = ((df_hom["conc_hom_mat"] > 0.1)& df_hom[endpoint].isna()& (df_hom[clp_col] != "Not classified"))

            unknown_df = df_hom.loc[unknown_mask,[cas_col, hom_material_col, clp_col]].copy()

            if not unknown_df.empty:
                unknown_df["endpoint"] = endpoint
                unknown_chemicals.append(unknown_df)

            # 6. Calculate the ATE
            # Formula:
            #
            # ATE_mix =
            # [100 - sum(% unknown substances)] /
            # sum(% known substances / LD50_or_LC50)
            #
            # This block calculates:
            # 100 - sum(% unknown substances)
            # ------------------------------------------------------------------

            sum_unknown_percent = df_hom.loc[unknown_mask,"conc_hom_mat_percent"].sum()

            adjusted_100 = 100 - sum_unknown_percent
            #print(adjusted_100)

            # For each valid substance:
            # concentration_percent / LD50_or_LC50
            # Then sum over all valid substances.
            valid_mask = df_hom[endpoint].notna() & (df_hom[endpoint] != 0)

            sum_constituents = (df_hom.loc[valid_mask, "conc_hom_mat_percent"]/ df_hom.loc[valid_mask, endpoint]).sum()
            #print(df_hom.loc[valid_mask, "CAS"], df_hom.loc[valid_mask, "conc_hom_mat_percent"], df_hom.loc[valid_mask, "conc_hom_mat_percent"]/ df_hom.loc[valid_mask, endpoint])
            #print(sum_constituents)

            # 7. Calculate ATE mixture
            # If the denominator is zero, ATE cannot be calculated.
            # Otherwise:
            # ATE = adjusted_100 / sum_constituents

            if sum_constituents != 0:
                ate = adjusted_100 / sum_constituents
                ate = round(float(ate), 2)
                #print(ate)
            else:
                ate = pd.NA
            # Save ATE and CLP classification into the output row

            row_result[ate_col] = ate
            # classify CLP Clas based on ATE
            row_result[classification_col] = classify_ate(ate,classification_limits)

        # Save the completed row for this homogeneous material
        results.append(row_result)

    # 8. Convert final results into a DataFrame
    final_df = pd.DataFrame(results)

    if unknown_chemicals:
        unknown_chemicals_df = pd.concat(
            unknown_chemicals,
            ignore_index=True
        )
    else:
        unknown_chemicals_df = pd.DataFrame(
            columns=[cas_col, hom_material_col, "endpoint"]
        )

    return final_df, unknown_chemicals_df

### Skin Corrosion & Irritation ###
def skin_corr_irr_clp(df_product, df_toxicity_info):
    '''
    Classification of skin corrosion and irritation.
    :param df_product: Dataframe with the compounds and their concentration in the homogenous mixture.
    :param df_toxicity_info: Information about the toxicity of each compound
    :return: The CLP classification for Skin Corrosion and Irritation.
    '''
    ### Step 1: prepare the dataset ###
    # merge the data on CAS
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    # columns for each of the four endpoints (to be filled with SCLs below and later with generic CLP limits where no SCL is found)
    df_calculation["conc_limit_for_assessment_Skin Corr. 1A"] = None
    df_calculation["conc_limit_for_assessment_Skin Corr. 1B"] = None
    df_calculation["conc_limit_for_assessment_Skin Corr. 1C"] = None
    df_calculation["conc_limit_for_assessment_Skin Irrit. 2"] = None
    # List of endpoints for sensitization:
    endpoints = ['Skin Corr. 1A', 'Skin Corr. 1B','Skin Corr. 1C','Skin Irrit. 2']

    ### Step 2: Create SCL columns (from the DB lowest of Lower/Upper Limits) ###
    for ep in endpoints:
        lower_col = f"{ep} - Lower Limit: (%)"
        upper_col = f"{ep} - Upper Limit: (%)"

        # Check if at least one of the columns exists
        if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
            # Use min row-wise, ignoring missing columns
            df_calculation[f"SCL {ep}"] = df_calculation[[c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)
            df_calculation[f"conc_limit_for_assessment_{ep}"] = df_calculation[f"conc_limit_for_assessment_{ep}"].fillna(df_calculation[f"SCL {ep}"])

    ### Step 3: Loop over homogenous materials and apply CLP rules ###
    skin_corrosion_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
        assessment_col = "skin corrosion and irritation assessment"
        col = "skin_corr_irr"
        df[assessment_col] = None

        ## Assess if it's cat 1A, 1B, 1C or 2
        # to take into account also SCL equation that will be used: C1/(5% or SCL) + C2/(5% or SCL) + .. >= 1

        # for skin cat 1A
        # for conc limit 5% for 1A
        # if SCL is not given fill the denominator as 5%
        df["conc_limit_for_assessment_Skin Corr. 1A_limit_5_percent"] = df["conc_limit_for_assessment_Skin Corr. 1A"].fillna(0.05)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1A", "calculation_concentration_over_conc_limit_1A_limit_5_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1A_limit_5_percent"]
        # calculate C1/(5% or SCL) + C2/(5% or SCL) + ... for all
        sum_1A_limit_5_percent = df["calculation_concentration_over_conc_limit_1A_limit_5_percent"].sum()

        # for conc limit 1% for 1A
        # if SCL is not given fill the denominator as 1%
        df["conc_limit_for_assessment_Skin Corr. 1A_limit_1_percent"] = df["conc_limit_for_assessment_Skin Corr. 1A"].fillna(0.01)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1A", "calculation_concentration_over_conc_limit_1A_limit_1_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1A_limit_1_percent"]
        # calculate C1/(1% or SCL) + C2/(1% or SCL) + ... for all
        sum_1A_limit_1_percent = df["calculation_concentration_over_conc_limit_1A_limit_1_percent"].sum()

        # for skin cat 1B
        # for conc limit 5% for 1B
        # if SCL is not given fill the denominator as 5%
        df["conc_limit_for_assessment_Skin Corr. 1B_limit_5_percent"] = df["conc_limit_for_assessment_Skin Corr. 1B"].fillna(0.05)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1B", "calculation_concentration_over_conc_limit_1B_limit_5_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1B_limit_5_percent"]
        # calculate C1/(5% or SCL) + C2/(5% or SCL) + ... for all
        sum_1B_limit_5_percent = df["calculation_concentration_over_conc_limit_1B_limit_5_percent"].sum()
        # for conc limit 1% for 1B
        # if SCL is not given fill the denominator as 1%
        df["conc_limit_for_assessment_Skin Corr. 1B_limit_1_percent"] = df["conc_limit_for_assessment_Skin Corr. 1B"].fillna(0.01)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1B", "calculation_concentration_over_conc_limit_1B_limit_1_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1B_limit_1_percent"]
        # calculate C1/(1% or SCL) + C2/(1% or SCL) + ... for all
        sum_1B_limit_1_percent = df["calculation_concentration_over_conc_limit_1B_limit_1_percent"].sum()

        # for skin cat 1C
        # for conc limit 5% for 1C
        # if SCL is not given fill the denominator as 5%
        df["conc_limit_for_assessment_Skin Corr. 1C_limit_5_percent"] = df["conc_limit_for_assessment_Skin Corr. 1C"].fillna(0.05)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1C", "calculation_concentration_over_conc_limit_1C_limit_5_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1C_limit_5_percent"]
        # calculate C1/(5% or SCL) + C2/(5% or SCL) + ... for all
        sum_1C_limit_5_percent = df["calculation_concentration_over_conc_limit_1C_limit_5_percent"].sum()
        # for conc limit 1% for 1C
        # if SCL is not given fill the denominator as 1%
        df["conc_limit_for_assessment_Skin Corr. 1C_limit_1_percent"] = df["conc_limit_for_assessment_Skin Corr. 1C"].fillna(0.01)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1C", "calculation_concentration_over_conc_limit_1C_limit_1_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1C_limit_1_percent"]
        # calculate C1/(1% or SCL) + C2/(1% or SCL) + ... for all
        sum_1C_limit_1_percent = df["calculation_concentration_over_conc_limit_1C_limit_1_percent"].sum()

        # for skin irr cat 2
        # if SCL is not given fill the denominator as 10%
        df["conc_limit_for_assessment_Skin Irrit. 2"] = df["conc_limit_for_assessment_Skin Irrit. 2"].fillna(0.1)
        df.loc[df["skin_corr_irr"] == "Skin Irrit. 2", "calculation_concentration_over_conc_limit_2"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Irrit. 2"]
        # calculate C1/(10% or SCL) + C2/(10% or SCL) + ... for all
        sum_2 = df["calculation_concentration_over_conc_limit_2"].sum()

        # assessment:
        # cat 1A
        # check: C1/(5% or SCL) + C2/(5% or SCL) + .. >= 1
        if sum_1A_limit_5_percent >= 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 1A"
        # cat 1B
        # check: C1/(5% or SCL) + C2/(5% or SCL) + .. >= 1
        if sum_1B_limit_5_percent + sum_1A_limit_5_percent >= 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 1B"
        # cat 1C
        # check: C1/(5% or SCL) + C2/(5% or SCL) + .. >= 1
        if sum_1A_limit_5_percent + sum_1B_limit_5_percent + sum_1C_limit_5_percent >= 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 1C"
        # cat 2
        # check: C1/(1% or SCL) + C2/(1% or SCL) + .. >= 1
        if (sum_1A_limit_5_percent + sum_1B_limit_5_percent + sum_1C_limit_5_percent < 1) and (sum_1A_limit_1_percent + sum_1B_limit_1_percent + sum_1C_limit_1_percent >=1) :
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"

        # check: C1/(10% or SCL) + C2/(10% or SCL) + .. >= 1
        if  sum_2 > 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"
        # check: C1/(10% or SCL) (for cat 2) + C2/(1% or SCL) (for cat 1, in the rule you multiply by 10, but if you use 1% not 10%, it balances out) >= 1
        if sum_1A_limit_1_percent + sum_1B_limit_1_percent + sum_1C_limit_1_percent + sum_2 > 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"

        # save the results for each classification and give the worst category a priority
        rating_col = "skin corrosion and irritation assessment"
        rank = { "cat. 1A": 0 ,"cat. 1B": 1, "cat. 1C": 2, "cat. 2": 3, None:4}
        rating = min(df[rating_col], key=lambda x: rank[x])
        skin_corrosion_for_each_material.append({
            "hom_material": hom_material,
            f"Skin corrosion irritation": rating})
    # return results as a dataframe
    return(pd.DataFrame(skin_corrosion_for_each_material))
### Eye Corrosion & Irritation ###
def eye_corr_irr_clp(df_product,df_toxicity_info):
    '''
    Classification of eye corrosion and irritation.
    :param df_product: Dataframe with the compounds and their concentration in the homogenous mixture.
    :param df_toxicity_info: Information about the toxicity of each compound
    :return: The CLP classification for Skin Corrosion and Irritation.
    '''
    ### Eye Corrosion & Irritation ###
    ### Step 1: prepare the dataset ###
    # merge the data on CAS
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    # columns for each of the four endpoints (to be filled with SCLs below and later with generic CLP limits where no SCL is found)
    df_calculation["conc_limit_for_assessment_Skin Corr. 1A"] = None
    df_calculation["conc_limit_for_assessment_Skin Corr. 1B"] = None
    df_calculation["conc_limit_for_assessment_Skin Corr. 1C"] = None
    df_calculation["conc_limit_for_assessment_Eye Dam. 1"] = None
    df_calculation["conc_limit_for_assessment_Eye Irrit. 2"] = None
    # List of endpoints for sensitization:
    endpoints = ['Skin Corr. 1A', 'Skin Corr. 1B', 'Eye Dam. 1', 'Eye Irrit. 2']

    ### Step 2: Create SCL columns (from the DB lowest of Lower/Upper Limits) ###
    for ep in endpoints:
        lower_col = f"{ep} - Lower Limit: (%)"
        upper_col = f"{ep} - Upper Limit: (%)"

        # Check if at least one of the columns exists
        if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
            # Use min row-wise, ignoring missing columns
            df_calculation[f"SCL {ep}"] = df_calculation[
                [c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)
            df_calculation[f"conc_limit_for_assessment_{ep}"] = df_calculation[f"conc_limit_for_assessment_{ep}"].fillna(
                df_calculation[f"SCL {ep}"])
    ### Step 3: Loop over homogenous materials and apply CLP rules ###
    skin_corrosion_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
        assessment_col = "eye corrosion and irritation assessment"
        col = "eye_corr_irr"
        df[assessment_col] = None

        ## Assess if it's cat 1A, 1B, 1C or 2
        # to take into account also SCL equation that will be used: C1/(3% or SCL) + C2/(3% or SCL) + .. >= 1


        # for skin cat 1A
        # for conc limit 3% for 1A
        df["conc_limit_for_assessment_Skin Corr. 1A_limit_3_percent"] = df["conc_limit_for_assessment_Skin Corr. 1A"].fillna(0.03)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1A", "calculation_concentration_over_conc_limit_1A_limit_3_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1A_limit_3_percent"]
        # calculate C1/(3% or SCL) + C2/(3% or SCL) + ... for all
        sum_1A_limit_3_percent = df["calculation_concentration_over_conc_limit_1A_limit_3_percent"].sum()

        # for conc limit 1% for 1A
        df["conc_limit_for_assessment_Skin Corr. 1A_limit_1_percent"] = df["conc_limit_for_assessment_Skin Corr. 1A"].fillna(0.01)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1A", "calculation_concentration_over_conc_limit_1A_limit_1_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1A_limit_1_percent"]
        # calculate C1/(1% or SCL) + C2/(1% or SCL) + ... for all
        sum_1A_limit_1_percent = df["calculation_concentration_over_conc_limit_1A_limit_1_percent"].sum()

        # for skin cat 1B
        # for conc limit 3% for 1B
        df["conc_limit_for_assessment_Skin Corr. 1B_limit_3_percent"] = df["conc_limit_for_assessment_Skin Corr. 1B"].fillna(0.03)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1B", "calculation_concentration_over_conc_limit_1B_limit_3_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1B_limit_3_percent"]
        # calculate C1/(3% or SCL) + C2/(3% or SCL) + ... for all
        sum_1B_limit_3_percent = df["calculation_concentration_over_conc_limit_1B_limit_3_percent"].sum()
        # for conc limit 1% for 1B
        df["conc_limit_for_assessment_Skin Corr. 1B_limit_1_percent"] = df["conc_limit_for_assessment_Skin Corr. 1B"].fillna(0.01)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1B", "calculation_concentration_over_conc_limit_1B_limit_1_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1B_limit_1_percent"]
        # calculate C1/(1% or SCL) + C2/(1% or SCL) + ... for all
        sum_1B_limit_1_percent = df["calculation_concentration_over_conc_limit_1B_limit_1_percent"].sum()

        # for skin cat 1C
        # for conc limit 3% for 1C
        df["conc_limit_for_assessment_Skin Corr. 1C_limit_3_percent"] = df["conc_limit_for_assessment_Skin Corr. 1C"].fillna(0.03)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1C", "calculation_concentration_over_conc_limit_1C_limit_3_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1C_limit_3_percent"]
        # calculate C1/(3% or SCL) + C2/(3% or SCL) + ... for all
        sum_1C_limit_3_percent = df["calculation_concentration_over_conc_limit_1C_limit_3_percent"].sum()
        # for conc limit 1% for 1C
        df["conc_limit_for_assessment_Skin Corr. 1C_limit_1_percent"] = df["conc_limit_for_assessment_Skin Corr. 1C"].fillna(0.01)
        df.loc[df["skin_corr_irr"] == "Skin Corr. 1C", "calculation_concentration_over_conc_limit_1C_limit_1_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Skin Corr. 1C_limit_1_percent"]
        # calculate C1/(1% or SCL) + C2/(1% or SCL) + ... for all
        sum_1C_limit_1_percent = df["calculation_concentration_over_conc_limit_1C_limit_1_percent"].sum()

        # for eye dam 1
        # for 3 % limit
        df["conc_limit_for_assessment_Eye Dam. 1_limit_3_percent"] = df["conc_limit_for_assessment_Eye Dam. 1"].fillna(0.03)
        df.loc[df["eye_corr_irr"] == "Eye Dam. 1", "calculation_concentration_over_conc_limit_eye_1_limit_3_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Eye Dam. 1_limit_3_percent"]
        # calculate C1/(3% or SCL) + C2/(3% or SCL) + ... for all
        sum_eye_1_limit_3_percent = df["calculation_concentration_over_conc_limit_eye_1_limit_3_percent"].sum()
        # for 1 % limit
        df["conc_limit_for_assessment_Eye Dam. 1_limit_1_percent"] = df["conc_limit_for_assessment_Eye Dam. 1"].fillna(0.01)
        df.loc[df["eye_corr_irr"] == "Eye Dam. 1", "calculation_concentration_over_conc_limit_eye_1_limit_1_percent"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Eye Dam. 1_limit_1_percent"]
        # calculate C1/(1% or SCL) + C2/(1% or SCL) + ... for all
        sum_eye_1_limit_1_percent = df["calculation_concentration_over_conc_limit_eye_1_limit_1_percent"].sum()

        # for eye irr 2
        df["conc_limit_for_assessment_Eye Irrit. 2"] = df["conc_limit_for_assessment_Eye Irrit. 2"].fillna(0.1)
        df.loc[df["eye_corr_irr"] == "Eye Irrit. 2", "calculation_concentration_over_conc_limit_eye_2"] = df["conc_hom_mat"] / df["conc_limit_for_assessment_Eye Irrit. 2"]
        # calculate C1/(10% or SCL) + C2/(10% or SCL) + ... for all
        sum_eye_2 = df["calculation_concentration_over_conc_limit_eye_2"].sum()

        # assessment:
        # cat 1
        # check: C1/(5% or SCL) + C2/(5% or SCL) + .. >= 1
        if sum_eye_1_limit_3_percent >= 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 1"
        # check: C1/(5% or SCL) + C2/(5% or SCL) + .. >= 1
        if sum_1A_limit_3_percent + sum_1B_limit_3_percent + sum_1C_limit_3_percent >= 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 1"

        # cat 2
        # check: C1/(1% or SCL) + C2/(1% or SCL) + .. >= 1
        if (sum_eye_1_limit_3_percent < 1) and (sum_eye_1_limit_1_percent >= 1):
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"
        # check: C1/(1% or SCL) + C2/(1% or SCL) + .. >= 1
        if (sum_1A_limit_3_percent + sum_1B_limit_3_percent + sum_1C_limit_3_percent < 1) and (sum_1A_limit_1_percent + sum_1B_limit_1_percent + sum_1C_limit_1_percent >=1) :
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"

        # check: C1/(10% or SCL) + C2/(10% or SCL) + .. >= 1
        if sum_eye_2 > 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"
        # check: C1/(10% or SCL) (for cat 2) + C2/(1% or SCL) (for cat 1, in the rule you multiply by 10, but if you use 1% not 10%, it balances out) >= 1
        if sum_eye_1_limit_1_percent + sum_eye_2> 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"
        # check: C1/(10% or SCL) (for cat 2) + C2/(1% or SCL) (for cat 1, in the rule you multiply by 10, but if you use 1% not 10%, it balances out) >= 1
        if sum_eye_1_limit_1_percent + sum_1A_limit_1_percent + sum_1B_limit_1_percent + sum_1C_limit_1_percent> 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"
        # check: C1/(10% or SCL) (for cat 2) + C2/(1% or SCL) (for cat 1, in the rule you multiply by 10, but if you use 1% not 10%, it balances out) >= 1
        if sum_eye_1_limit_1_percent + sum_1A_limit_1_percent + sum_1B_limit_1_percent + sum_1C_limit_1_percent + sum_eye_2> 1:
            df.loc[df[assessment_col].isna(), assessment_col] = "cat. 2"

        # save the results for each classification and give the worst category a priority
        rating_col = "eye corrosion and irritation assessment"
        rank = {"cat. 1": 0, "cat. 2": 1,  None: 3}
        rating = min(df[rating_col], key=lambda x: rank[x])
        skin_corrosion_for_each_material.append({
            "hom_material": hom_material,
            f"Eye corrosion irritation": rating})
    # return results as a dataframe
    return(pd.DataFrame(skin_corrosion_for_each_material))

### Skin Sensitization ###
def skin_sens_clp(df_product, df_toxicity_info):
    '''
    Classification of Skin Sensitization.
    :param df_product: Dataframe with the compounds and their concentration in the homogenous mixture.
    :param df_toxicity_info: Information about the toxicity of each compound
    :return: The CLP classification for Skin Sensitization.
    '''
    ### Step 1: prepare the dataset ###
    # merge the data on CAS
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)

    ### Step 2: Clean SCL info ###
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

    ### Step 3: Create check columns comparing concentration in the mixture with SCL
    for ep in endpoints:
        scl_col = f"SCL {ep}"
        check_col = f"{scl_col} - check"

        if scl_col in df_calculation.columns:
            df_calculation[check_col] = np.where(
                df_calculation[scl_col].isna(),
                None,  # SCL missing
                np.where(df_calculation["conc_hom_mat"] > df_calculation[scl_col], "Yes", "No")
            )

    # Step 4: assess per homogenous material
    sensitization_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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
        #get the results
        rating_col = "sensitization assessment"
        rank = { "cat. 1A": 0 ,"cat. 1B": 1, "cat. 1": 2, None: 3}
        rating = min(df[rating_col], key=lambda x: rank[x])
        sensitization_for_each_material.append({
            "hom_material": hom_material,
            f"Skin Sensitization": rating})
    # create a dataframe with them
    return pd.DataFrame(sensitization_for_each_material)
### Respiratory Sensitization ###
def resp_sens_clp(df_product, df_toxicity_info, state = "solid/liquid" or "gas"):
    '''
    Classification of Respiration Sensitization.
    :param df_product: Dataframe with the compounds and their concentration in the homogenous mixture.
    :param df_toxicity_info: Information about the toxicity of each compound
    :return: The CLP classification for Respiration Sensitization.
    '''
    ### step 1: set specific limits if a substance is solid/liquid or gas:
    if state == "solid/liquid":
        lim_1a = 0.001
        lim_1b = 0.01
        lim_1 = 0.01
    elif state == "gas":
        lim_1a = 0.001
        lim_1b = 0.002
        lim_1 = 0.002

    ### step 2: prepare the dataset ###
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)

    ### Step 3: Clean SCL info ###
    # List of endpoints for sensitization:
    endpoints = ["Resp. Sens. 1A", "Resp. Sens. 1B", "Resp. Sens. 1"]
    # Create SCL columns (from the DB lowest of Lower/Upper Limits)
    for ep in endpoints:
        lower_col = f"{ep} - Lower Limit: (%)"
        upper_col = f"{ep} - Upper Limit: (%)"

        # Check if at least one of the columns exists
        if lower_col in df_calculation.columns or upper_col in df_calculation.columns:
            # Use min row-wise, ignoring missing columns
            df_calculation[f"SCL {ep}"] = df_calculation[[c for c in [lower_col, upper_col] if c in df_calculation.columns]].min(axis=1)

    # Step 4: Create check columns comparing concentration in the mixture with SCL
    for ep in endpoints:
        scl_col = f"SCL {ep}"
        check_col = f"{scl_col} - check"

        if scl_col in df_calculation.columns:
            df_calculation[check_col] = np.where(
                df_calculation[scl_col].isna(),
                None,  # SCL missing
                np.where(df_calculation["conc_hom_mat"] > df_calculation[scl_col], "Yes", "No")
            )

    # Step 5: assess per homogenous material
    sensitization_for_each_material = []
    for hom_material in hom_materials:
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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

        # append to results
        rating_col = "sensitization assessment"
        rank = { "cat. 1A": 0 ,"cat. 1B": 1, "cat. 1": 2, None: 3}
        rating = min(df[rating_col], key=lambda x: rank[x])
        sensitization_for_each_material.append({
            "hom_material": hom_material,
            f"Respiration Sensitization": rating})

    return pd.DataFrame(sensitization_for_each_material)

### Acute Aquatic toxicity ###
def acute_aquatic_clp(df_product, df_toxicity_info):
    '''
    Classification of Hazardous to aquatic env - acute hazard.
    :param df_product: Dataframe with the compounds and their concentration in the homogenous mixture.
    :param df_toxicity_info: Information about the toxicity of each compound
    :return: The CLP classification for Hazardous to aquatic env - acute hazard.
    '''
    ### Step 1: prepare the dataset ###
    # merge the data on CAS
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # set the hazard and m_factor columns
    hazard_class = "aquatic_tox_acute"
    m_factor = "m_factor"
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    # set the col names
    hazard_col = hazard_class
    conc_col = "conc_hom_mat"
    m_col = m_factor
    ### Step 2: Start the assessment per homogenous material ###
    results_for_each_material = []
    # assessment for each hom mat
    for hom_material in hom_materials:
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
        # Compute the sums for each category based on concentration thresholds
        sum_acute1_x_m_factor = (df.loc[(df_calculation[hazard_col] == 'Aqua. Acute 1: H400') & (df[conc_col] >= 0.001), conc_col] *
                      df.loc[(df_calculation[hazard_col] == 'Aqua. Acute 1: H400') & (df[conc_col] >= 0.001), m_col]).sum()

        # Assign hazard rating based on M_factor*acute1>= 25%
        if (sum_acute1_x_m_factor) >= 0.25:
            mixture_hazard = 'Cat. 1'
        else:
            mixture_hazard = None
        # append resuls
        results_for_each_material.append({
            "hom_material": hom_material,
            f"Aquatic acute tox": mixture_hazard})
    # return results as a dataframe
    return pd.DataFrame(results_for_each_material)

### Chronic Aquatic toxicity ###
def chronic_aquatic_clp(df_product, df_toxicity_info):
    '''
    Classification of Hazardous to aquatic env - chronic hazard.
    :param df_product: Dataframe with the compounds and their concentration in the homogenous mixture.
    :param df_toxicity_info: Information about the toxicity of each compound
    :return: The CLP classification for Hazardous to aquatic env - chronic hazard.
    '''
    ### Step 1: prepare the dataset ###
    # merge the data on CAS
    df_calculation = pd.merge(df_product, df_toxicity_info, on="CAS", how="left")
    # set the hazard and m_factor columns
    hazard_class = "aquatic_tox_chronic"
    m_factor = "m_factor"
    # save the highest value of contribution of hom mat
    df_calculation["conc_hom_mat"] = df_calculation[["min_contribution_hom_mat", "max_contribution_hom_mat"]].max(axis=1)
    # get the unique hom materials
    hom_materials = df_product["Homogenous Material"].unique().tolist()
    # set the col names
    hazard_col = hazard_class
    conc_col = "conc_hom_mat"
    m_col = m_factor

    ### Step 2: Start the assessment per homogenous material ###
    results_for_each_material = []
    # assessment for each hom mat
    for hom_material in hom_materials:
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
        # Compute the sums for each category based on concentration thresholds (###!!! CHECK THRESHOLDS !!! ###)
        sum_chronic1_x_m_factor = (
                df.loc[(df_calculation[hazard_col] == 'Aqua. Chronic 1: H410') &(df[conc_col] >= 0.001),conc_col] *
                df.loc[(df_calculation[hazard_col] == 'Aqua. Chronic 1: H410') &(df[conc_col] >= 0.001), m_col]).sum()

        sum_chronic1 = df.loc[(df_calculation[hazard_col] == 'Aqua. Chronic 1: H410') &(df[conc_col] >= 0.001),conc_col].sum()

        sum_chronic2 = df.loc[(df_calculation[hazard_col] == 'Aqua. Chronic 2: H411') &(df[conc_col] >= 0.01), conc_col].sum()

        sum_chronic3 = df.loc[(df_calculation[hazard_col] == 'Aqua. Chronic 3: H412') &(df[conc_col] >= 0.01),conc_col].sum()

        sum_chronic4 = df.loc[(df_calculation[hazard_col] == 'Aqua. Chronic 4: H413') &(df[conc_col] >= 0.01),conc_col].sum()

        # Assign hazard rating based on CLP rules
        if sum_chronic1_x_m_factor >= 0.25:
            mixture_hazard = 'Cat. 1'
        elif (sum_chronic1_x_m_factor*10 + sum_chronic2) >= 0.25:
            mixture_hazard = 'Cat. 2'
        elif (sum_chronic1_x_m_factor*100 + sum_chronic2*10 + sum_chronic3) >= 0.25:
            mixture_hazard = 'Cat. 3'
        elif (sum_chronic1 + sum_chronic2 + sum_chronic3 + sum_chronic4) >= 0.25:
            mixture_hazard = 'Cat. 4'
        else:
            mixture_hazard = None

        # append results
        results_for_each_material.append({
            "hom_material": hom_material,
            f"Aquatic chronic tox": mixture_hazard})
    # return results as a dataframe
    return pd.DataFrame(results_for_each_material)

### Combining assessments ###
def mixture_rules_clp_assessment(df_product, df_toxicity_info):
    """
    Combining all mixture rules together.
    :param df_product: Dataframe with the compounds and their concentration in the homogenous mixture.
    :param df_toxicity_info: Information about the toxicity of each compound
    :return: The CLP classification for all end points
    """
    # ACUTE TOX: ORAL, DRMAL, INHAL
    # decide which toxicological information to assess
    ld_lc_to_assess = ["LD50_oral", "LC50_gas", "LC50_vapour", "LC50_dust_mist_aerosol", "LD50_dermal"]
    acute_tox, unknown_chemicals_df = acute_toxicity_clp_assessment(df_product=df_product,df_toxicity_info=df_toxicity_info,endpoints_to_assess=ld_lc_to_assess)
    #acute_tox = acute_tox[['hom_material','Acute toxicity oral','Acute toxicity dermal', 'Acute toxicity inhalation (gases)','Acute toxicity inhalation (vapour)','Acute toxicity inhalation (dust/mist)']]
    # CORROSION AND IRRITATION
    skin_corr_n_irr = skin_corr_irr_clp(df_product, df_toxicity_info)
    eye_corr_n_irr = eye_corr_irr_clp(df_product, df_toxicity_info)

    ## SENSITIZATION
    skin_sens = skin_sens_clp(df_product, df_toxicity_info)
    resp_sens_solid_liquid = resp_sens_clp(df_product, df_toxicity_info, "solid/liquid")
    resp_sens_solid_liquid = resp_sens_solid_liquid.rename(columns={"Respiration Sensitization": "Respiration Sensitization solid/liquid"})
    resp_sens_gas = resp_sens_clp(df_product, df_toxicity_info, "gas")
    resp_sens_gas = resp_sens_gas.rename(columns={"Respiration Sensitization": "Respiration Sensitization gas"})

    ## AQUATIC
    aquatic_acute = acute_aquatic_clp(df_product, df_toxicity_info)
    aquatic_chronic = chronic_aquatic_clp(df_product, df_toxicity_info)

    # ## Merge all
    final_c2c_results = (acute_tox.merge(skin_corr_n_irr, on="hom_material", how="outer")
                         .merge(eye_corr_n_irr,on="hom_material",how="outer")
                         .merge(skin_sens, on="hom_material", how="outer")
                         .merge(resp_sens_solid_liquid, on="hom_material", how="outer")
                         .merge(resp_sens_gas, on="hom_material", how="outer")
                         .merge(aquatic_acute, on="hom_material", how="outer")
                         .merge(aquatic_chronic, on="hom_material", how="outer"))


    return final_c2c_results


df_toxicity_info = pd.read_excel('/Users/juliakulpa/Desktop/Mixture_rule_tests/toxicity_info.xlsx')
df_product = pd.read_excel('/Users/juliakulpa/Desktop/Mixture_rule_tests/Test_for_mixture_rules_v2.xlsx')
df = mixture_rules_clp_assessment(df_product, df_toxicity_info)
df.to_excel('/Users/juliakulpa/Desktop/Mixture_rule_tests/CLP_mixture_rules_results.xlsx')
print(df)



