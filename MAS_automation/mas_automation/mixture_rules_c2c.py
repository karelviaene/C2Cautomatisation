###### Mixture rules #####

### Loading libraries ###
import pandas as pd
import sqlite3
import numpy as np
import re


### Step 0: Retrieving the information from the C2C database
## Functions to extract info from DB
def extract_info_from_DB(cas_list, db_path):
    ''' The output is a df with cols: CAS and each CPL/tox info, for each CAS in the row
    Works on:
    database: maindb="C2C_DATABASE"
    cas is always in the col named "ref"
    extracts from the DB:
        1. SLC
        2. Toxicity
        3. Corrosion/irritation
        4. Sensitization
        5. Aquatic toxicity (acute and chronic)
     SCl is saved as a separate df
     Other info is saved in a separate df
     at the end: both df are merged on outer (keeping all values)
     '''
    # helper function for cleaning manual rating
    def clean_manual_rating(value):
        if pd.isna(value):
            return pd.NA

        text = str(value).strip().upper()

        match = re.search(r"\b(RED|GREY|YELLOW|GREEN)\b", text)

        if not match:
            return pd.NA

        colour = match.group(1)
        return colour

    rating_rank = {
        "GREEN": 1,
        "YELLOW": 2,
        "GREY": 3,
        "RED": 4,
    }

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

    # Connect to the Db and establish cursor
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Store results
    results = []
    SCL_results = []
    assessment_dfs = []

    # Gen information which cols are in SCL:
    table_name = 'SCONCLIM'
    exclude_cols = {"ID", "ref"}
    cursor.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cursor.fetchall()]
    selected_cols_SCL = [col for col in cols if col not in exclude_cols]
    selected_cols_SCL_sql = ", ".join([f'"{col}"' for col in selected_cols_SCL])

    # Loop over each CAS in the provided list
    for cas in cas_list:
        ### 1. SCL ####
        # SCL:
        query = f'''
        SELECT {selected_cols_SCL_sql}
        FROM "{table_name}"
        WHERE "ref" = ?
        '''

        cursor.execute(query, (cas,))
        rows = cursor.fetchall()
        df_SCL = pd.DataFrame(rows, columns=selected_cols_SCL)
        df_SCL.insert(0, "CAS", cas)
        SCL_results.append(df_SCL)

        ### 2. Toxicity ####
        # ORALTOX table for LD50_oral
        cursor.execute(f"""
            SELECT
                "Oral Acute: LD50 =",
                "Oral toxicity Acute Tox classified"
            FROM ORALTOX WHERE ref = ?
        """, (cas,))
        oral_data = cursor.fetchone()
        oral_ld50 = oral_data[0] if oral_data else None
        oral_CLP_class = oral_data[1] if oral_data else None

        # INHALTOX table for LC50 (gas, vapour, dust/mist/aerosol)
        cursor.execute(f"""
            SELECT
                "Inhalative toxicity Acute: LC50 (gas) =",
                "Inhalative toxicity Acute: LC50 (vapor) =",
                "Inhalative toxicity Acute: LC50 (dust/mist/aerosol) =",
                "Inhalative toxicity Acute Tox classification"
            FROM INHALTOX WHERE ref = ?
        """, (cas,))
        inhalation_data = cursor.fetchone()
        lc50_gas = inhalation_data[0] if inhalation_data else None
        lc50_vapour = inhalation_data[1] if inhalation_data else None
        lc50_dust_mist_aerosol = inhalation_data[2] if inhalation_data else None
        inhal_CLP_class = inhalation_data[3] if inhalation_data else None

        # DERMALTOX table for LD50_dermal
        cursor.execute(f"""
            SELECT
                "Dermal Acute: LD50 =",
                "Dermal toxicity Acute Tox classified"
            FROM DERMALTOX WHERE ref = ?
        """, (cas,))
        dermal_data = cursor.fetchone()
        dermal_ld50 = dermal_data[0] if dermal_data else None
        dermal_CLP_class = dermal_data[1] if dermal_data else None

        ### 3. CORR / IRR ###
        cursor.execute(f"""
            SELECT
                "Skin irritation classification",
                "Eye irritation classification",
                "Respiratory irritation classification"
            FROM IRRITCOR WHERE ref = ?
        """, (cas,))
        irritation_data = cursor.fetchone()
        skin_irr = irritation_data[0] if irritation_data else None
        eye_irr = irritation_data[1] if irritation_data else None
        reps_irr = irritation_data[2] if irritation_data else None

        ### 4. SENSITISATION ###
        cursor.execute(f"""
            SELECT
                "Skin sensitization CLP classification",
                "Respiratory sensitization CLP classification"
            FROM SENSITISATION WHERE ref = ?
        """, (cas,))
        sensitisation_data = cursor.fetchone()
        skin_sensitisation = sensitisation_data[0] if sensitisation_data else None
        resp_sensitisation = sensitisation_data[1] if sensitisation_data else None

        ### 5. AQUATIC TOX ###

        # M factor, Aquatic tox acute & chronic
        cursor.execute(f"""
            SELECT
                "Aquatic toxicity Acute Tox classified",
                "Aquatic toxicity Chronic Tox classified",
                "M factor"
            FROM AQUATOX WHERE ref = ?
        """, (cas,))
        aquatic_tox_data = cursor.fetchone()
        aquatic_tox_acute = aquatic_tox_data[0] if aquatic_tox_data else None
        aquatic_tox_chronic = aquatic_tox_data[1] if aquatic_tox_data else None
        m_factor = aquatic_tox_data[2] if aquatic_tox_data else None

        # Fish toxicity
        cursor.execute(f"""
            SELECT
                "Fish toxicity Acute: LC50 (96h) =",
                "Fish toxicity Chronic: NOEC =",
                "Fish toxicity Acute QSAR: LC50 =",
                "Fish toxicity Chronic QSAR: NOEC ="
            FROM FISHTOX WHERE ref = ?
        """, (cas,))
        fish_tox_data = cursor.fetchone()
        fish_lc50 = fish_tox_data[0] if fish_tox_data else None
        fish_noec = fish_tox_data[1] if fish_tox_data else None
        fish_lc50_qsar = fish_tox_data[2] if fish_tox_data else None
        fish_noec_qsar = fish_tox_data[3] if fish_tox_data else None

        # Daphnae / invertebrate toxicity
        cursor.execute(f"""
            SELECT
                "Invertebrate toxicity Acute: L(E)C50 (48h) =",
                "Invertebrae toxicity Chronic: NOEC =",
                "Invertebrae toxicity Acute QSAR: LC50 =",
                "Invertebrae toxicity Chronic QSAR: NOEC ="
            FROM INVTOX WHERE ref = ?
        """, (cas,))
        daph_tox_data = cursor.fetchone()
        daph_lc50 = daph_tox_data[0] if daph_tox_data else None
        daph_noec = daph_tox_data[1] if daph_tox_data else None
        daph_lc50_qsar = daph_tox_data[2] if daph_tox_data else None
        daph_noec_qsar = daph_tox_data[3] if daph_tox_data else None

        # Algae toxicity
        cursor.execute(f"""
            SELECT
                "Algae toxicity Acute: L(E)C50 (72/96h) =",
                "Algae toxicity Chronic: NOEC =",
                "Algae toxicity Acute QSAR: LC50 =",
                "Algae toxicity Chronic QSAR: NOEC ="
            FROM ALGAETOX WHERE ref = ?
        """, (cas,))
        algae_tox_data = cursor.fetchone()
        algae_lc50 = algae_tox_data[0] if algae_tox_data else None
        algae_noec = algae_tox_data[1] if algae_tox_data else None
        algae_lc50_qsar = algae_tox_data[2] if algae_tox_data else None
        algae_noec_qsar = algae_tox_data[3] if algae_tox_data else None

        ### 6. C2C COLOUR ASSESSMENTS ###

        # Manual assessment
        df_manual = pd.read_sql_query("""
            SELECT *
            FROM "MANUAL ASSESSMENT C2C"
            WHERE ref = ?
            """, conn, params=(cas,))
        manual_cols = [
            col for col in df_manual.columns
            if col.startswith("manual_assessment_")
        ]

        for col in manual_cols:
            df_manual[col] = df_manual[col].map(clean_manual_rating)

        df_automatic = pd.read_sql_query("""
                    SELECT *
                    FROM "AUTOMATIC_ASSESSMENT"
                    WHERE ref = ?
                    """, conn, params=(cas,))
        df_assessment = df_automatic.merge(df_manual, on='ref', how='outer')
        df_assessment = df_assessment.rename(columns={"ref":"CAS"})
        endpoint_mapping = {
            "oral toxicity": {
                "auto": "hazard assessment oral toxicity",
                "manual": "manual_assessment_oral_toxicity",
            },
            "dermal toxicity": {
                "auto": "hazard assessment dermal toxicity",
                "manual": "manual_assessment_dermal_toxicity",
            },
            "inhalative toxicity": {
                "auto": "hazard assessment inhalative toxicity",
                "manual": "manual_assessment_inhalative_toxicity",
            },
            "skin eye respiratory corrosion irritation": {
                "auto": "hazard assessment skin eye respiratory corrosion irritation",
                "manual": "manual_assessment_skin_eye_respiratory_corrosion_irritation",
            },
            "sensitization": {
                "auto": "hazard assessment sensitization",
                "manual": "manual_assessment_sensitization",
            },
        }
        for endpoint, cols in endpoint_mapping.items():
            auto_col = cols["auto"]
            manual_col = cols["manual"]

            final_col = f"{endpoint} C2C assessment"

            df_assessment[final_col] = df_assessment.apply(
                lambda row: worst_rating(
                    row[auto_col] if auto_col in df_assessment.columns else pd.NA,
                    row[manual_col] if manual_col in df_assessment.columns else pd.NA,
                ),
                axis=1
            )

        assessment_dfs.append(df_assessment)


        ### Append results for each CAS:
        # Add the data for the current CAS to the results list
        results.append({
            "CAS": cas,
            "LD50_oral": oral_ld50,
            "LC50_gas": lc50_gas,
            "LC50_vapour": lc50_vapour,
            "LC50_dust_mist_aerosol": lc50_dust_mist_aerosol,
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
            "fish_lc50": fish_lc50,
            "fish_noec": fish_noec,
            "fish_lc50_qsar": fish_lc50_qsar,
            "fish_noec_qsar": fish_noec_qsar,
            "daph_lc50": daph_lc50,
            "daph_noec": daph_noec,
            "daph_lc50_qsar": daph_lc50_qsar,
            "daph_noec_qsar": daph_noec_qsar,
            "algae_lc50": algae_lc50,
            "algae_noec": algae_noec,
            "algae_lc50_qsar": algae_lc50_qsar,
            "algae_noec_qsar": algae_noec_qsar
        })

    # obtain results for SCL
    df_final_SCL = pd.concat(SCL_results, ignore_index=True)

    # Convert the results into a df
    df_info = pd.DataFrame(results)

    # df assessment for colours

    final_assessment_df = pd.concat(assessment_dfs, ignore_index=True)

    # connect both df
    df = df_info.merge(df_final_SCL, on="CAS", how="outer").merge(final_assessment_df, on="CAS", how="outer")

    # Close the database connection
    conn.close()

    # df toxicity info to numeric:
    numeric_cols = [
        "LD50_oral",
        "LC50_gas",
        "LC50_vapour",
        "LC50_dust_mist_aerosol",
        "LD50_dermal",
        "m_factor",
        "fish_lc50",
        "fish_noec",
        "fish_lc50_qsar",
        "fish_noec_qsar",
        "daph_lc50",
        "daph_noec",
        "daph_lc50_qsar",
        "daph_noec_qsar",
        "algae_lc50",
        "algae_noec",
        "algae_lc50_qsar",
        "algae_noec_qsar",
    ]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Return the DataFrame
    return df
####################################################################################
# FUNCTIONS
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
        df_calc_hom_material = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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
        df_calc_hom_material = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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
        df = df_calculation.loc[df_product["Homogenous Material"] == hom_material]
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

    # ACUTE TOX: ORAL, DRMAL, INHAL
    # decide which toxicological information to assess
    all_ld_50_or_lc_50_options = ["LD50_oral", "LC50_gas", "LC50_vapour", "LC50_dust_mist_aerosol", "LD50_dermal"]
    acute_tox_C2C_df, unknown_chemicals_df = C2C_acute_toxicity(df_product, df_toxicity_info, all_ld_50_or_lc_50_options)

    # CORROSION AND IRRITATION
    corr_n_irr_C2C_df = corr_n_irr_mixture_rule_c2c(df_product, df_toxicity_info)

    ## SENSITIZATION
    sens_C2C_df = skin_and_resp_sens_c2c(df_product, df_toxicity_info)

    ## AQUATIC
    final_aquatic_results = final_aquatic_c2c(df_product, df_toxicity_info)

    ## Merge all
    final_c2c_results = acute_tox_C2C_df.merge(corr_n_irr_C2C_df, on="hom_material", how="outer").merge(sens_C2C_df,
                                                                                                        on="hom_material",
                                                                                                        how="outer").merge(
        final_aquatic_results, on="hom_material", how="outer")

    final_c2c_results_summary = final_c2c_results[
        ["hom_material", "C2C acute toxicity", "C2C Skin, Eye, and Respiratory Irritation",
         "C2C Skin and Respiratory Sensitization", "C2C Acute and Chronic Aquatic Toxicity"]]

    print(unknown_chemicals_df)
    return final_c2c_results_summary, final_c2c_results

####################################################################################
# RUNNING THE PROGRAMME

## Running the toxicity assessment
# read the Excel with the product to assess: it needs to hava CAS and homogenous materials specified
df_product = pd.read_excel('/Users/juliakulpa/Desktop/DB_tests_mixture_rules/Test_for_mixture_rules_v2.xlsx')

# read the unique CAS numbers in the product
cas_list = df_product["CAS"].unique().tolist()

# access toxicological info from the DB for the given CAS list
db_path = '/Users/juliakulpa/Desktop/DB_tests_mixture_rules/C2Cdatabase.db'

# extract the relevant info from the DB
df_toxicity_info = extract_info_from_DB(cas_list, db_path)

df_toxicity_info.to_excel('/Users/juliakulpa/Desktop/DB_tests_mixture_rules/toxicity_10_06.xlsx')

# for now since the DB does not have colours, use an excel:
# df_toxicity_info = pd.read_excel('/Users/juliakulpa/Desktop/DB_tests_mixture_rules/toxicity_1.xlsx')

# make the assessment
summary, c2c_full = mixture_rules_C2C_assessment(df_product, df_toxicity_info)

print(summary)

# save to excel
summary.to_excel("/Users/juliakulpa/Desktop/DB_tests_mixture_rules/assessment_results_1.xlsx")
c2c_full.to_excel("/Users/juliakulpa/Desktop/DB_tests_mixture_rules/assessment_results_2.xlsx")

###