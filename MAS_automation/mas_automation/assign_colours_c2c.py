### IMPORT
### Loading libraries ###
import pandas as pd
from openpyxl import load_workbook
import sqlite3
from pathlib import Path

### Functions ###
# Extract all information needed from the database for all CAS on a list
def extract_info_from_DB(cas_list, db_path):
    ''' The output is a df with cols: CAS and each CPL/tox info, for each CAS in the row and returns a datframe
     '''
    # Connect to the Db and establish cursor
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Store results
    results = []

    for cas in cas_list:
        cursor.execute("""
            SELECT
                "pH",
                "Boiling point"
            FROM OTHERINFO
            WHERE ref = ?
        """, (cas,))

        irritation_data = cursor.fetchone()

        ph = irritation_data[0] if irritation_data else None
        boiling_point = irritation_data[1] if irritation_data else None

        ### Toxicity ####
        # ORALTOX table for LD50_oral
        cursor.execute("""
                SELECT
                    "Oral toxicity Acute Tox classified",
                    "Oral toxicity Asp Tox classified",
                    "Oral toxicity STOT classified",
                    "Oral Acute: LD50 =",
                    "Oral Chronic: LOAEL ="
                FROM ORALTOX
                WHERE ref = ?
            """, (cas,))

        oral_data = cursor.fetchone()

        oral_acute_tox_class = oral_data[0] if oral_data else None
        oral_asp_tox_class = oral_data[1] if oral_data else None
        oral_stot_tox_class = oral_data[2] if oral_data else None
        oral_ld50 = oral_data[3] if oral_data else None
        oral_loael = oral_data[4] if oral_data else None

        # INHALTOX table for LC50 (gas, vapour, dust/mist/aerosol)
        cursor.execute("""
                SELECT
                    "Inhalative toxicity Acute Tox classification",
                    "Inhalative toxicity STOT classified",
                    "Inhalative toxicity Acute: LC50 (gas) =",
                    "Inhalative toxicity Acute: LC50 (vapor) =",
                    "Inhalative toxicity Acute: LC50 (dust/mist/aerosol) =",
                    "Inhalative toxicity Chronic: LOAEL (gas) =",
                    "Inhalative toxicity Chronic: LOAEL (vapor) =",
                    "Inhalative toxicity Chronic: LOAEL (dust/mist/aerosol) ="
                FROM INHALTOX
                WHERE ref = ?
            """, (cas,))

        inhalation_data = cursor.fetchone()

        inhal_acute_tox_class = inhalation_data[0] if inhalation_data else None
        inhal_stot_tox_class = inhalation_data[1] if inhalation_data else None
        lc50_gas = inhalation_data[2] if inhalation_data else None
        lc50_vapour = inhalation_data[3] if inhalation_data else None
        lc50_dust_mist_aerosol = inhalation_data[4] if inhalation_data else None
        loael_gas = inhalation_data[5] if inhalation_data else None
        loael_vapour = inhalation_data[6] if inhalation_data else None
        loael_dust_mist_aerosol = inhalation_data[7] if inhalation_data else None

        # DERMALTOX table for LD50_dermal
        cursor.execute("""
                SELECT
                    "Dermal toxicity Acute Tox classified",
                    "Dermal toxicity STOT classified",
                    "Dermal Acute: LD50 =",
                    "Dermal Chronic: LOAEL ="
                FROM DERMALTOX
                WHERE ref = ?
            """, (cas,))

        dermal_data = cursor.fetchone()

        dermal_acute_tox_class = dermal_data[0] if dermal_data else None
        dermal_stot_tox_class = dermal_data[1] if dermal_data else None
        dermal_ld50 = dermal_data[2] if dermal_data else None
        dermal_loael = dermal_data[3] if dermal_data else None

        ### 3. CORR / IRR ###
        cursor.execute("""
                SELECT
                    "Skin irritation classification",
                    "Skin testing: conclusion",
                    "Eye irritation classification",
                    "Eye testing conclusion",
                    "Respiratory irritation classification",
                    "Respiratory testing conclusion"
                FROM IRRITCOR
                WHERE ref = ?
            """, (cas,))

        irritation_data = cursor.fetchone()

        skin_irr_class = irritation_data[0] if irritation_data else None
        skin_irr_conclusion = irritation_data[1] if irritation_data else None

        eye_irr_class = irritation_data[2] if irritation_data else None
        eye_irr_conclusion = irritation_data[3] if irritation_data else None

        resp_irr_class = irritation_data[4] if irritation_data else None
        resp_irr_conclusion = irritation_data[5] if irritation_data else None

        ### 4. SENSITISATION ###
        cursor.execute("""
                SELECT
                    "Skin sensitization CLP classification",
                    "Skin sensitization MAK classification",
                    "Skin sensitization testing conclusion",
                    "Respiratory sensitization CLP classification",
                    "Respiratory sensitization MAK classification",
                    "Respiratory sensitization testing conclusion"
                FROM SENSITISATION
                WHERE ref = ?
            """, (cas,))

        sensitisation_data = cursor.fetchone()

        skin_sens_clp_class = sensitisation_data[0] if sensitisation_data else None
        skin_sens_mak_class = sensitisation_data[1] if sensitisation_data else None
        skin_sens_conclusion = sensitisation_data[2] if sensitisation_data else None

        resp_sens_clp_class = sensitisation_data[3] if sensitisation_data else None
        resp_sens_mak_class = sensitisation_data[4] if sensitisation_data else None
        resp_sens_conclusion = sensitisation_data[5] if sensitisation_data else None


        ### Append results for each CAS:
        # Add the data for the current CAS to the results list
        results.append({
            "CAS": cas,
            "pH": ph,
            "boiling_point": boiling_point,

            "oral_acute_tox_class": oral_acute_tox_class,
            "oral_asp_tox_class": oral_asp_tox_class,
            "oral_stot_tox_class": oral_stot_tox_class,
            "LD50_oral": oral_ld50,
            "oral_LOAEL": oral_loael,

            "inhal_acute_tox_class": inhal_acute_tox_class,
            "inhal_stot_tox_class": inhal_stot_tox_class,
            "LC50_gas": lc50_gas,
            "LC50_vapour": lc50_vapour,
            "LC50_dust_mist_aerosol": lc50_dust_mist_aerosol,
            "inhal_LOAEL_gas": loael_gas,
            "inhal_LOAEL_vapour": loael_vapour,
            "inhal_LOAEL_dust_mist_aerosol": loael_dust_mist_aerosol,

            "dermal_acute_tox_class": dermal_acute_tox_class,
            "dermal_stot_tox_class": dermal_stot_tox_class,
            "LD50_dermal": dermal_ld50,
            "dermal_LOAEL": dermal_loael,

            "skin_irr_class": skin_irr_class,
            "skin_irr_conclusion": skin_irr_conclusion,
            "eye_irr_class": eye_irr_class,
            "eye_irr_conclusion": eye_irr_conclusion,
            "resp_irr_class": resp_irr_class,
            "resp_irr_conclusion": resp_irr_conclusion,

            "skin_sens_clp_class": skin_sens_clp_class,
            "skin_sens_mak_class": skin_sens_mak_class,
            "skin_sens_conclusion": skin_sens_conclusion,
            "resp_sens_clp_class": resp_sens_clp_class,
            "resp_sens_mak_class": resp_sens_mak_class,
            "resp_sens_conclusion": resp_sens_conclusion,
        })



    # Convert the results into a df
    df = pd.DataFrame(results)

    # Close the database connection
    conn.close()

    # Return the DataFrame
    return df

### C2C Assessment for each end point

#01 CARCINOGENICITY -> need revision
# -> for green how to mark "not a known,
# presumed or suspected
# carcinogen or reliable
# negative long-term cancer
# studies?" -> no field like this in the template?
def carcinogenicity_rating(carcinogenicity_clp=None,carcinogenicity_mak=None,carcinogenicity_IARC=None,carcinogenicity_TLV=None,carcinogenicity_exp_evidence=None):

    # RED
    if carcinogenicity_clp in [
        "Carc. 1A: H350: May cause cancer",
        "Carc. 1B: H350: May cause cancer",
        "Carc. 2: H351: Suspected of causing cancer",
    ] or carcinogenicity_mak in [
        "MAK III 1",
        "MAK III 2",
        "MAK III 3B",
    ] or carcinogenicity_IARC in [
        "IARC 1",
        "IARC 2A",
        "IARC 2B",
    ] or carcinogenicity_TLV in [
        "TLV A1",
        "TLV A2",
        "TLV A3",
    ]:
        return "RED"

    # YELLOW

    if  carcinogenicity_mak in [
        "MAK III 3A",
        "MAK III 4",
        "MAK III 5",
    ]:
        return "YELLOW"

    # GREEN

    if carcinogenicity_IARC in [
        "IARC 4",
    ] or carcinogenicity_TLV in [
        "TLV A5",
    ]:
        return "GREEN"

    # else return GREY
    return "GREY"

#02 ENDOCRINE -> need revision!!!
def endocrine_rating(endocrine_clp=None,endocrine_evidence=None):
    # RED
    if endocrine_clp in [
        "ED ENV 1",
        "ED ENV 2",
        "ED HH 1",
        "ED HH 2",
    ] or endocrine_evidence in [
        "Sufficient evidence of endorcrine effects in humans",
        "Evidence of endocrine effects in animals/micro organisms",
    ]:
        return "RED"

    # YELLOW
    if endocrine_evidence in [
        "Endocrine activity observed but no endocrine disruption",
    ]:
        return "YELLOW"

    # else return GREY
    return "GREY"

#03 REPRODUCTIVE TOXICITY
def reproductive_toxicity_rating(reproductive_toxicity_CLP=None,reproductive_toxicity_MAK=None,reproductive_toxicity_oral_noael=None,reproductive_toxicity_inhalation_noael=None):
    """
    Rating criteria for reproductive toxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """
    # RED

    if reproductive_toxicity_CLP in [
        "Repr. 1A: H360: May damage fertility or the unborn child",
        "Repr. 1B: H360: May damage fertility or the unborn child",
        "Repr. 2: H361: Suspected of damaging fertility or the unborn child",
        "H362: Reproductive toxic through lactation"
    ] or reproductive_toxicity_MAK in [
        "MAK A",
        "MAK B",
    ]:
        return "RED"

    if reproductive_toxicity_oral_noael is not None and reproductive_toxicity_oral_noael <= 50:
        return "RED"

    if reproductive_toxicity_inhalation_noael is not None and reproductive_toxicity_inhalation_noael <= 0.25:
        return "RED"

     # GREY
    if reproductive_toxicity_MAK in [
        "MAK D",
    ]:
        return "GREY"

    # YELLOW
    if reproductive_toxicity_MAK in [
        "MAK C",
    ]:
        return "YELLOW"

    if reproductive_toxicity_oral_noael is not None and 50 <= reproductive_toxicity_oral_noael <= 500:
        return "YELLOW"

    if reproductive_toxicity_inhalation_noael is not None and 0.25 <= reproductive_toxicity_inhalation_noael <= 2.5:
        return "YELLOW"

    # GREEN
    if reproductive_toxicity_oral_noael is not None and reproductive_toxicity_oral_noael > 500:
        return "GREEN"

    if reproductive_toxicity_inhalation_noael is not None and reproductive_toxicity_inhalation_noael > 2.5:
        return "GREEN"

    # ELSE GREY
    return "GREY"

#04 DEVELOPMENTAL TOXICITY
def developmental_toxicity(developmental_toxicity_CLP=None,developmental_toxicity_MAK=None,developmental_toxicity_oral_noael=None,developmental_toxicity_inhalation_noael=None):
    """
    Rating criteria for reproductive toxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """
    # RED

    if developmental_toxicity_CLP in [
        "Repr. 1A: H360: May damage fertility or the unborn child",
        "Repr. 1B: H360: May damage fertility or the unborn child",
        "Repr. 2: H361: Suspected of damaging fertility or the unborn child",
    ] or developmental_toxicity_MAK in [
        "MAK A",
        "MAK B",
    ]:
        return "RED"

    if developmental_toxicity_oral_noael is not None and developmental_toxicity_oral_noael <= 50:
        return "RED"

    if developmental_toxicity_inhalation_noael is not None and developmental_toxicity_inhalation_noael <= 0.25:
        return "RED"

     # GREY
    if developmental_toxicity_MAK in [
        "MAK D",
    ]:
        return "GREY"

    # YELLOW
    if developmental_toxicity_MAK in [
        "MAK C",
    ]:
        return "YELLOW"

    if developmental_toxicity_oral_noael is not None and 50 <= developmental_toxicity_oral_noael <= 500:
        return "YELLOW"

    if developmental_toxicity_inhalation_noael is not None and 0.25 <= developmental_toxicity_inhalation_noael <= 2.5:
        return "YELLOW"

    # GREEN
    if developmental_toxicity_oral_noael is not None and developmental_toxicity_oral_noael > 500:
        return "GREEN"

    if developmental_toxicity_inhalation_noael is not None and developmental_toxicity_inhalation_noael > 2.5:
        return "GREEN"

    # ELSE GREY
    return "GREY"

# 05 ORAL TOX
def oral_toxicity_rating(acute_tox_class=None,asp_tox_class=None,stot_tox_class=None,ld50=None,loael=None):
    """
    Rating criteria for oral toxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """
    # RED
    if acute_tox_class in [
        "Acute Tox. 1: H300: Fatal if swallowed",
        "Acute Tox. 2: H300: Fatal if swallowed",
        "Acute Tox. 3: H301: Toxic if swallowed",
    ] or asp_tox_class in [
        "Asp. Tox. 1: H304: May be fatal if swallowed and enters airways",
    ] or stot_tox_class in [
        "STOT SE 1: H370: Causes damage to organs via oral exposure",
        "STOT RE 1: H372: Causes damage via repeated oral exposure",
    ]:
        return "RED"

    if ld50 is not None and ld50 <= 300:
        return "RED"

    if loael is not None and loael <= 10:
        return "RED"

    if ld50 is not None and ld50 <= 2000:
        if loael is None or loael <= 10:
            return "RED"

    # YELLOW
    if acute_tox_class in [
        "Acute Tox. 4: H302: Harmful if swallowed",
    ] or stot_tox_class in [
        "STOT SE 2: H371: May cause damage to organs via oral exposure",
        "STOT RE 2: H373: May cause damage via repeated oral exposure",
    ]:
        return "YELLOW"

    if ld50 is not None and ld50 <= 2000:
        if loael is not None and loael > 10:
            return "YELLOW"

    if ld50 is not None and ld50 > 2000:
        if loael is not None and loael <= 100:
            if loael > 10:
                return "YELLOW"
    # GREEN
    if ld50 is not None and ld50 > 2000:
        if loael is not None and loael > 100:
            return "GREEN"

    # ELSE GREY
    return "GREY"

# 06 DERMAL TOX
def dermal_toxicity_rating(acute_tox_class=None,stot_tox_class=None,ld50=None,loael=None,):
    """
    Rating criteria for oral toxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """
    # RED
    if acute_tox_class in [
        "Acute Tox. 1: H310: Fatal in contact with skin",
        "Acute Tox. 2: H310: Fatal in contact with skin",
        "Acute Tox. 3: H311: Toxic in contact with skin",
    ] or stot_tox_class in [
        "STOT SE 1: H370: Causes damage to organs via dermal exposure",
        "STOT RE 1: H372: Causes damage via repeated dermal exposure",
    ]:
        return "RED"

    if ld50 is not None and ld50 <= 1000:
        return "RED"

    if loael is not None and loael <= 20:
        return "RED"

    # YELLOW
    if acute_tox_class in [
        "Acute Tox. 4: H312: Harmful in contact with skin",
    ] or stot_tox_class in [
        "STOT SE 2: H371: May cause damage to organs via dermal exposure",
        "STOT RE 2: H373: May cause damage via repeated dermal exposure",
    ]:
        return "YELLOW"

    if loael is not None and loael <= 200:
        return "YELLOW"

    if ld50 is not None and ld50 > 1000:
        if loael is None or loael <= 2000:
            return "YELLOW"


    # GREEN
    if ld50 is not None and ld50 > 2000:
        if loael is not None and loael > 200:
            return "GREEN"

    # ELSE GREY
    return "GREY"

# 07 INHALATION TOX
def inhal_toxicity_rating(acute_tox_class=None,stot_tox_class=None,
                          ld50_gas=None,ld50_vapour=None,ld50_dust_mist=None,
                          loael_gas=None,loael_vapour=None,loael_dust_mist=None,
                          boiling_point=None):
    """
    Rating criteria for oral toxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """
    # RED
    if acute_tox_class in [
        "Acute Tox. 1: H330: Fatal if inhaled",
        "Acute Tox. 2: H330: Fatal if inhaled",
        "Acute Tox. 3: H331: Toxic if inhaled",
    ] or stot_tox_class in [
        "STOT SE 1: H370: Damages organs via inhal. exp.",
        "STOT RE 1: H372: Damages organs via repeated inhal. exp.",
    ]:
        return "RED"

    if ld50_gas is not None and ld50_gas <= 2500:
        return "RED"

    if ld50_vapour is not None and ld50_vapour <= 10:
        return "RED"

    if ld50_dust_mist is not None and ld50_dust_mist <= 1:
        return "RED"

    if loael_gas is not None and loael_gas <= 50:
        return "RED"

    if loael_vapour is not None and loael_vapour <= 0.2:
        return "RED"

    if loael_dust_mist is not None and loael_dust_mist <= 0.02:
        return "RED"

    # GREY

    if boiling_point is not None and boiling_point < 0:
        if ld50_gas is not None and loael_gas is not None:
            return "GREY"


    # YELLOW

    if acute_tox_class in [
        "Acute Tox. 4: H332: Harmful if inhaled",
    ] or stot_tox_class in [
        "STOT SE 2: H371: May damage organs via inhal. exp.",
        "STOT RE 2: H373: May damage organs via repeated inhal. exp.",
        "STOT SE 3: H336: May cause drowsiness",
    ]:
        return "YELLOW"

    if loael_gas is not None and loael_gas <= 250:
        return "YELLOW"

    if loael_vapour is not None and loael_vapour <= 1:
        return "YELLOW"

    if loael_dust_mist is not None and loael_dust_mist <= 0.2:
        return "YELLOW"

    if ld50_gas is not None and ld50_gas <= 20000:
        return "YELLOW"

    if ld50_dust_mist is not None and ld50_dust_mist <= 5:
        return "YELLOW"

    if ld50_vapour is not None and ld50_vapour <= 20:
        return "YELLOW"


    # GREEN

    if loael_gas is not None and loael_gas > 250:
        return "GREEN"

    if loael_vapour is not None and loael_vapour > 1:
        return "GREEN"

    if loael_dust_mist is not None and loael_dust_mist > 0.2:
        return "GREEN"

    if ld50_gas is not None and ld50_gas > 20000:
        return "GREEN"

    if ld50_dust_mist is not None and ld50_dust_mist > 5:
        return "GREEN"

    if ld50_vapour is not None and ld50_vapour > 20:
        return "GREEN"


    # ELSE GREY

    return "GREY"
#08 NEUROTOXICITY -> need revision
# this info doesn't match the decision tree
def neurotoxicity(neurotoxicity_CLP=None,neurotoxicity_on_list=None,neurotoxicity_evidence=None,neurotoxicity_chronic_loael=None, neurotoxicity_stot_loael=None):
    """
    Rating criteria for rNeurotoxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """
    # RED

    if neurotoxicity_on_list.lower() in [
        "yes"
    ]:
        return "RED"

    if neurotoxicity_CLP in [
        "STOT SE 1",
        "STOT RE 1",
    ]:
        return "RED"

    # ELSE GREY
    return "GREY"

# 09 Skin, Eye, Respiratory corrosion/irritation
def corr_irrit_rating(skin_irr_class=None,eye_irr_class=None,resp_irr_class=None,
                      skin_testing_conclusion=None,eye_testing_conclusion=None, resp_testing_conclusion=None,
                      ph=None):
    """
    Rating criteria for oral toxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """

    # RED
    if skin_irr_class in [
        "Skin Corr. 1: H314 Causes severe skin burns and eye damage",
    ] or eye_irr_class in [
        "Eye Dam. 1: H318 Causes serious eye damage",
    ]:
        return "RED"

    if ph is not None and not (2 <= ph <= 11.5):
        return "RED"

    if skin_testing_conclusion in [
        "Skin corrosive according to testing",
    ] or eye_testing_conclusion in [
        "Eye damaging according to testing",
    ] or resp_testing_conclusion in [
        "Corrosive according to testing",
    ]:
        return "RED"

    # YELLOW

    if skin_irr_class in [
        "Skin Irrit. 2: H315 Causes skin irritation",
    ] or eye_irr_class in [
        "Eye irrit. 2a: H319 Causes serious eye irritation",
        'Eye irrit. 2b: H320 Causes eye irritation'
    ] or resp_irr_class in [
        "STOT SE 3: H335 May cause respiratory tract irritation",
    ]:
        return "YELLOW"

    if skin_testing_conclusion in [
        "Skin irritating according to testing",
    ] or eye_testing_conclusion in [
        "Eye irritating according to testing",
    ] or resp_testing_conclusion in [
        "Irritating according to testing",
    ]:
        return "YELLOW"

    # GREEN

    if skin_testing_conclusion in [
        "Not irritating according to testing",
    ] or eye_testing_conclusion in [
        "Not irritating according to testing",
    ] or resp_testing_conclusion in [
        "Not irritating according to testing",
    ]:
        return "GREEN"

    # ELSE GREY
    return "GREY"
# 10 SENSITIZATION
def sensitization_rating(skin_sens_clp_class=None,resp_sens_clp_class=None,
                         skin_sens_mak_class=None,resp_sens_mak_class=None,
                         skin_testing_conclusion=None,resp_testing_conclusion=None):
    """
    Rating criteria for oral toxicity:
    returns 'RED', 'YELLOW', 'GREEN', or 'GREY'
    """
    # RED

    if skin_sens_clp_class in [
        "Skin Sens. 1: H317 May cause an allergic skin reaction",
    ] or resp_sens_clp_class in [
        "Resp. Sens. 1: H334 May cause allergy or asthma symptoms or breathing difficulties if inhaled",
        "Resp. Sens. 1A: H334 May cause allergy or asthma symptoms or breathing difficulties if inhaled",
        "Resp. Sens. 1B: H334 May cause allergy or asthma symptoms or breathing difficulties if inhaled"
    ] or skin_sens_mak_class in [
        "MAK Sh",
    ] or resp_sens_mak_class in [
        "MAK Sa",
    ]:
        return "RED"

    if skin_testing_conclusion in [
        "Sensitizing according to testing",
    ] or resp_testing_conclusion in [
        "Sensitizing according to testing",
    ]:
        return "RED"

    # GREEN

    if skin_testing_conclusion in [
        "Not sensitzing according to testing",
    ] or resp_testing_conclusion in [
        "Not sensitzing according to testing",
    ]:
        return "GREEN"

    # ELSE GREY
    return "GREY"

# 11 HUMAN HEALTH -> needs revision

# 19 ENVIRONMENTAL HEALTH

# 20 ORGANOHAlOGENS
def organohalogens(organohalogens_class=None):
    if organohalogens_class.lower() in [
        "yes"
    ]:
        return "RED"

    else:
        return "GREEN"

# 21 TOXIC METALS
def toxic_metals(toxic_metals_class=None):
    if toxic_metals_class.lower() in [
        "yes"
    ]:
        return "RED"

    else:
        return "GREEN"


# SAVE ALL INDIVIDUAL ASSESSMENTS TO ONE DATAFRAME
def save_assessment_df_to_sql(df,db_path,table_name="DATA",source_key_col="CAS",db_key_col="ref",keep_cols=None):
    if keep_cols is None:
        keep_cols = [
            "CAS",
            "hazard assessment oral toxicity",
            "hazard assessment dermal toxicity",
            "hazard assessment inhalative toxicity",
            "hazard assessment skin eye respiratory corrosion irritation",
            "hazard assessment sensitization",
        ]

    df = df.copy()

    # Keep only selected columns
    df = df[keep_cols]

    # Rename CAS column to ref for the database
    df = df.rename(columns={source_key_col: db_key_col})

    conn = sqlite3.connect(db_path)

    # Check if table exists
    table_exists = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        conn,
        params=(table_name,)
    )

    # If table does not exist, create it
    if table_exists.empty:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print("new table created")
        conn.close()
        return

    # Read existing table columns
    existing_table = pd.read_sql_query(
        f'SELECT * FROM "{table_name}" LIMIT 0',
        conn
    )

    existing_cols = existing_table.columns.tolist()

    # Add missing columns to df
    for col in existing_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Add new df columns to SQL table if needed
    new_cols = [col for col in df.columns if col not in existing_cols]

    for col in new_cols:
        conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')
        existing_cols.append(col)

    # Reorder df to match SQL table
    df = df[existing_cols]

    # Loop through rows and update/append per ref
    for _, new_row in df.iterrows():
        ref = new_row[db_key_col]

        old_df = pd.read_sql_query(
            f'SELECT * FROM "{table_name}" WHERE "{db_key_col}" = ?',
            conn,
            params=(ref,)
        )

        # New ref: append
        if old_df.empty:
            pd.DataFrame([new_row]).to_sql(
                table_name,
                conn,
                if_exists="append",
                index=False
            )
            print(f"new record added: {ref}")

        else:
            old_row = old_df.iloc[0]

            # Align and compare
            new_row = new_row[old_row.index]

            old_compare = old_row.fillna("").astype(str)
            new_compare = new_row.fillna("").astype(str)

            if old_compare.equals(new_compare):
                print(f"no change: {ref}")

            else:
                conn.execute(
                    f'DELETE FROM "{table_name}" WHERE "{db_key_col}" = ?',
                    (ref,)
                )

                pd.DataFrame([new_row]).to_sql(
                    table_name,
                    conn,
                    if_exists="append",
                    index=False
                )

                print(f"it has changed: {ref}")

    conn.commit()
    conn.close()
    print("Table updated")
# PUT THE DATAFRAME TO SQL DATABASE
def automatic_assessment_c2c(db_path, df_product):
    # read the unique CAS numbers in the product
    cas_list = df_product["CAS"].unique().tolist()

    df = extract_info_from_DB(cas_list, db_path)

    # make sure cols you are working on are numeric (if they need to be numeric)
    numeric_cols = ["pH","boiling_point", "LD50_oral","oral_LOAEL","LC50_gas","LC50_vapour", "LC50_dust_mist_aerosol","inhal_LOAEL_gas","inhal_LOAEL_vapour","inhal_LOAEL_dust_mist_aerosol","LD50_dermal","dermal_LOAEL"]

    df[[col for col in numeric_cols if col in df.columns]] = (
        df[[col for col in numeric_cols if col in df.columns]]
        .apply(pd.to_numeric, errors="coerce")
    )

    # apply functions to assess each chemical
    df["hazard assessment oral toxicity"] = df.apply(
        lambda row: oral_toxicity_rating(
            acute_tox_class=row["oral_acute_tox_class"],
            asp_tox_class=row["oral_asp_tox_class"],
            stot_tox_class=row["oral_stot_tox_class"],
            ld50=row["LD50_oral"],
            loael=row["oral_LOAEL"],
        ),
        axis=1
    )

    df["hazard assessment dermal toxicity"] = df.apply(
        lambda row: dermal_toxicity_rating(
            acute_tox_class=row["dermal_acute_tox_class"],
            stot_tox_class=row["dermal_stot_tox_class"],
            ld50=row["LD50_dermal"],
            loael=row["dermal_LOAEL"],
        ),
        axis=1
    )

    df["hazard assessment inhalative toxicity"] = df.apply(
        lambda row: inhal_toxicity_rating(
            acute_tox_class=row["inhal_acute_tox_class"],
            stot_tox_class=row["inhal_stot_tox_class"],
            ld50_gas=row["LC50_gas"],
            ld50_vapour=row["LC50_vapour"],
            ld50_dust_mist=row["LC50_dust_mist_aerosol"],
            loael_gas=row["inhal_LOAEL_gas"],
            loael_vapour=row["inhal_LOAEL_vapour"],
            loael_dust_mist=row["inhal_LOAEL_dust_mist_aerosol"],
            boiling_point=row["boiling_point"],
        ),
        axis=1
    )

    df["hazard assessment skin eye respiratory corrosion irritation"] = df.apply(
        lambda row: corr_irrit_rating(
            skin_irr_class=row["skin_irr_class"],
            eye_irr_class=row["eye_irr_class"],
            resp_irr_class=row["resp_irr_class"],
            skin_testing_conclusion=row["skin_irr_conclusion"],
            eye_testing_conclusion=row["eye_irr_conclusion"],
            resp_testing_conclusion=row["resp_irr_conclusion"],
            ph=row["pH"],
        ),
        axis=1
    )

    df["hazard assessment sensitization"] = df.apply(
        lambda row: sensitization_rating(
            skin_sens_clp_class=row["skin_sens_clp_class"],
            resp_sens_clp_class=row["resp_sens_clp_class"],
            skin_sens_mak_class=row["skin_sens_mak_class"],
            resp_sens_mak_class=row["resp_sens_mak_class"],
            skin_testing_conclusion=row["skin_sens_conclusion"],
            resp_testing_conclusion=row["resp_sens_conclusion"],
        ),
        axis=1
    )

    result_df = df[['CAS', 'hazard assessment oral toxicity','hazard assessment dermal toxicity','hazard assessment inhalative toxicity',
    'hazard assessment skin eye respiratory corrosion irritation','hazard assessment sensitization']]

    return result_df

def extract_info_from_DB_all(cas_list, db_path):
    '''Extracts all C2C endpoint data from the DB for all CAS numbers in the list.
    Extends extract_info_from_DB with CARCINOGENICITY, ENDOCRINE, REPROTOX,
    DEVELOPTOX, NEUROTOX, and CHEMICALCLASS tables.
    Returns a DataFrame with one row per CAS.
    '''
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    results = []

    for cas in cas_list:
        # OTHERINFO
        cursor.execute("""
            SELECT "pH", "Boiling point"
            FROM OTHERINFO WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        ph = row[0] if row else None
        boiling_point = row[1] if row else None

        # ORALTOX
        cursor.execute("""
            SELECT
                "Oral toxicity Acute Tox classified",
                "Oral toxicity Asp Tox classified",
                "Oral toxicity STOT classified",
                "Oral Acute: LD50 =",
                "Oral Chronic: LOAEL ="
            FROM ORALTOX WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        oral_acute_tox_class = row[0] if row else None
        oral_asp_tox_class = row[1] if row else None
        oral_stot_tox_class = row[2] if row else None
        oral_ld50 = row[3] if row else None
        oral_loael = row[4] if row else None

        # INHALTOX
        cursor.execute("""
            SELECT
                "Inhalative toxicity Acute Tox classification",
                "Inhalative toxicity STOT classified",
                "Inhalative toxicity Acute: LC50 (gas) =",
                "Inhalative toxicity Acute: LC50 (vapor) =",
                "Inhalative toxicity Acute: LC50 (dust/mist/aerosol) =",
                "Inhalative toxicity Chronic: LOAEL (gas) =",
                "Inhalative toxicity Chronic: LOAEL (vapor) =",
                "Inhalative toxicity Chronic: LOAEL (dust/mist/aerosol) ="
            FROM INHALTOX WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        inhal_acute_tox_class = row[0] if row else None
        inhal_stot_tox_class = row[1] if row else None
        lc50_gas = row[2] if row else None
        lc50_vapour = row[3] if row else None
        lc50_dust = row[4] if row else None
        loael_gas = row[5] if row else None
        loael_vapour = row[6] if row else None
        loael_dust = row[7] if row else None

        # DERMALTOX
        cursor.execute("""
            SELECT
                "Dermal toxicity Acute Tox classified",
                "Dermal toxicity STOT classified",
                "Dermal Acute: LD50 =",
                "Dermal Chronic: LOAEL ="
            FROM DERMALTOX WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        dermal_acute_tox_class = row[0] if row else None
        dermal_stot_tox_class = row[1] if row else None
        dermal_ld50 = row[2] if row else None
        dermal_loael = row[3] if row else None

        # IRRITCOR
        cursor.execute("""
            SELECT
                "Skin irritation classification",
                "Skin testing: conclusion",
                "Eye irritation classification",
                "Eye testing conclusion",
                "Respiratory irritation classification",
                "Respiratory testing conclusion"
            FROM IRRITCOR WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        skin_irr_class = row[0] if row else None
        skin_irr_conclusion = row[1] if row else None
        eye_irr_class = row[2] if row else None
        eye_irr_conclusion = row[3] if row else None
        resp_irr_class = row[4] if row else None
        resp_irr_conclusion = row[5] if row else None

        # SENSITISATION
        cursor.execute("""
            SELECT
                "Skin sensitization CLP classification",
                "Skin sensitization MAK classification",
                "Skin sensitization testing conclusion",
                "Respiratory sensitization CLP classification",
                "Respiratory sensitization MAK classification",
                "Respiratory sensitization testing conclusion"
            FROM SENSITISATION WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        skin_sens_clp_class = row[0] if row else None
        skin_sens_mak_class = row[1] if row else None
        skin_sens_conclusion = row[2] if row else None
        resp_sens_clp_class = row[3] if row else None
        resp_sens_mak_class = row[4] if row else None
        resp_sens_conclusion = row[5] if row else None

        # CARCINOGENICITY
        cursor.execute("""
            SELECT
                "Carcinogenicity Classified CLP",
                "Carcinogenicity Classified MAK",
                "Carcinogenicity Classified IARC",
                "Carcinogenicity Classified TLV",
                "Carcinogenicity experimental evidence"
            FROM CARCINOGENICITY WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        carc_clp = row[0] if row else None
        carc_mak = row[1] if row else None
        carc_iarc = row[2] if row else None
        carc_tlv = row[3] if row else None
        carc_exp_evidence = row[4] if row else None

        # ENDOCRINE
        cursor.execute("""
            SELECT "Endocrine Classified CLP", "Endocrine evidence"
            FROM ENDOCRINE WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        endocrine_clp = row[0] if row else None
        endocrine_evidence = row[1] if row else None

        # REPROTOX
        cursor.execute("""
            SELECT
                "Reprotox Classified CLP",
                "Reprotox Classified MAK",
                "Reprotox Oral NOAEL =",
                "Reprotox Inhalation NOAEL ="
            FROM REPROTOX WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        repro_clp = row[0] if row else None
        repro_mak = row[1] if row else None
        repro_oral_noael = row[2] if row else None
        repro_inhal_noael = row[3] if row else None

        # DEVELOPTOX
        cursor.execute("""
            SELECT
                "Developmental Classified CLP",
                "Developmental Classified MAK",
                "Developmental Oral NOAEL =",
                "Developmental Inhalation NOAEL ="
            FROM DEVELOPTOX WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        develop_clp = row[0] if row else None
        develop_mak = row[1] if row else None
        develop_oral_noael = row[2] if row else None
        develop_inhal_noael = row[3] if row else None

        # NEUROTOX (note: "Neurtox STOT LOAEL" is the actual column name in the DB)
        cursor.execute("""
            SELECT
                "Neurotox Classified CLP",
                "Neurotox on a list",
                "Neurotox scientific evidence?",
                "Neurotox chronic LOAEL",
                "Neurtox STOT LOAEL"
            FROM NEUROTOX WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        neuro_clp = row[0] if row else None
        neuro_on_list = row[1] if row else None
        neuro_evidence = row[2] if row else None
        neuro_chronic_loael = row[3] if row else None
        neuro_stot_loael = row[4] if row else None

        # CHEMICALCLASS (organohalogens and toxic metals)
        cursor.execute("""
            SELECT "Organohalogen", "Toxic metal"
            FROM CHEMICALCLASS WHERE ref = ?
        """, (cas,))
        row = cursor.fetchone()
        organohalogen = row[0] if row else None
        toxic_metal = row[1] if row else None

        results.append({
            "CAS": cas,
            "pH": ph,
            "boiling_point": boiling_point,

            "oral_acute_tox_class": oral_acute_tox_class,
            "oral_asp_tox_class": oral_asp_tox_class,
            "oral_stot_tox_class": oral_stot_tox_class,
            "LD50_oral": oral_ld50,
            "oral_LOAEL": oral_loael,

            "inhal_acute_tox_class": inhal_acute_tox_class,
            "inhal_stot_tox_class": inhal_stot_tox_class,
            "LC50_gas": lc50_gas,
            "LC50_vapour": lc50_vapour,
            "LC50_dust_mist_aerosol": lc50_dust,
            "inhal_LOAEL_gas": loael_gas,
            "inhal_LOAEL_vapour": loael_vapour,
            "inhal_LOAEL_dust_mist_aerosol": loael_dust,

            "dermal_acute_tox_class": dermal_acute_tox_class,
            "dermal_stot_tox_class": dermal_stot_tox_class,
            "LD50_dermal": dermal_ld50,
            "dermal_LOAEL": dermal_loael,

            "skin_irr_class": skin_irr_class,
            "skin_irr_conclusion": skin_irr_conclusion,
            "eye_irr_class": eye_irr_class,
            "eye_irr_conclusion": eye_irr_conclusion,
            "resp_irr_class": resp_irr_class,
            "resp_irr_conclusion": resp_irr_conclusion,

            "skin_sens_clp_class": skin_sens_clp_class,
            "skin_sens_mak_class": skin_sens_mak_class,
            "skin_sens_conclusion": skin_sens_conclusion,
            "resp_sens_clp_class": resp_sens_clp_class,
            "resp_sens_mak_class": resp_sens_mak_class,
            "resp_sens_conclusion": resp_sens_conclusion,

            "carc_clp": carc_clp,
            "carc_mak": carc_mak,
            "carc_iarc": carc_iarc,
            "carc_tlv": carc_tlv,
            "carc_exp_evidence": carc_exp_evidence,

            "endocrine_clp": endocrine_clp,
            "endocrine_evidence": endocrine_evidence,

            "repro_clp": repro_clp,
            "repro_mak": repro_mak,
            "repro_oral_noael": repro_oral_noael,
            "repro_inhal_noael": repro_inhal_noael,

            "develop_clp": develop_clp,
            "develop_mak": develop_mak,
            "develop_oral_noael": develop_oral_noael,
            "develop_inhal_noael": develop_inhal_noael,

            "neuro_clp": neuro_clp,
            "neuro_on_list": neuro_on_list,
            "neuro_evidence": neuro_evidence,
            "neuro_chronic_loael": neuro_chronic_loael,
            "neuro_stot_loael": neuro_stot_loael,

            "organohalogen": organohalogen,
            "toxic_metal": toxic_metal,
        })

    df = pd.DataFrame(results)
    conn.close()
    return df


def automatic_assessment_c2c_all(db_path, df_product):
    cas_list = df_product["CAS"].unique().tolist()

    df = extract_info_from_DB_all(cas_list, db_path)

    numeric_cols = [
        "pH", "boiling_point",
        "LD50_oral", "oral_LOAEL",
        "LC50_gas", "LC50_vapour", "LC50_dust_mist_aerosol",
        "inhal_LOAEL_gas", "inhal_LOAEL_vapour", "inhal_LOAEL_dust_mist_aerosol",
        "LD50_dermal", "dermal_LOAEL",
        "repro_oral_noael", "repro_inhal_noael",
        "develop_oral_noael", "develop_inhal_noael",
        "neuro_chronic_loael", "neuro_stot_loael",
    ]

    df[[col for col in numeric_cols if col in df.columns]] = (
        df[[col for col in numeric_cols if col in df.columns]]
        .apply(pd.to_numeric, errors="coerce")
    )

    df["hazard assessment carcinogenicity"] = df.apply(
        lambda row: carcinogenicity_rating(
            carcinogenicity_clp=row["carc_clp"],
            carcinogenicity_mak=row["carc_mak"],
            carcinogenicity_IARC=row["carc_iarc"],
            carcinogenicity_TLV=row["carc_tlv"],
            carcinogenicity_exp_evidence=row["carc_exp_evidence"],
        ),
        axis=1
    )

    df["hazard assessment endocrine disruption"] = df.apply(
        lambda row: endocrine_rating(
            endocrine_clp=row["endocrine_clp"],
            endocrine_evidence=row["endocrine_evidence"],
        ),
        axis=1
    )

    df["hazard assessment reproductive toxicity"] = df.apply(
        lambda row: reproductive_toxicity_rating(
            reproductive_toxicity_CLP=row["repro_clp"],
            reproductive_toxicity_MAK=row["repro_mak"],
            reproductive_toxicity_oral_noael=row["repro_oral_noael"],
            reproductive_toxicity_inhalation_noael=row["repro_inhal_noael"],
        ),
        axis=1
    )

    df["hazard assessment developmental toxicity"] = df.apply(
        lambda row: developmental_toxicity(
            developmental_toxicity_CLP=row["develop_clp"],
            developmental_toxicity_MAK=row["develop_mak"],
            developmental_toxicity_oral_noael=row["develop_oral_noael"],
            developmental_toxicity_inhalation_noael=row["develop_inhal_noael"],
        ),
        axis=1
    )

    df["hazard assessment oral toxicity"] = df.apply(
        lambda row: oral_toxicity_rating(
            acute_tox_class=row["oral_acute_tox_class"],
            asp_tox_class=row["oral_asp_tox_class"],
            stot_tox_class=row["oral_stot_tox_class"],
            ld50=row["LD50_oral"],
            loael=row["oral_LOAEL"],
        ),
        axis=1
    )

    df["hazard assessment dermal toxicity"] = df.apply(
        lambda row: dermal_toxicity_rating(
            acute_tox_class=row["dermal_acute_tox_class"],
            stot_tox_class=row["dermal_stot_tox_class"],
            ld50=row["LD50_dermal"],
            loael=row["dermal_LOAEL"],
        ),
        axis=1
    )

    df["hazard assessment inhalative toxicity"] = df.apply(
        lambda row: inhal_toxicity_rating(
            acute_tox_class=row["inhal_acute_tox_class"],
            stot_tox_class=row["inhal_stot_tox_class"],
            ld50_gas=row["LC50_gas"],
            ld50_vapour=row["LC50_vapour"],
            ld50_dust_mist=row["LC50_dust_mist_aerosol"],
            loael_gas=row["inhal_LOAEL_gas"],
            loael_vapour=row["inhal_LOAEL_vapour"],
            loael_dust_mist=row["inhal_LOAEL_dust_mist_aerosol"],
            boiling_point=row["boiling_point"],
        ),
        axis=1
    )

    df["hazard assessment neurotoxicity"] = df.apply(
        lambda row: neurotoxicity(
            neurotoxicity_CLP=row["neuro_clp"],
            neurotoxicity_on_list=row["neuro_on_list"] if pd.notna(row["neuro_on_list"]) else "",
            neurotoxicity_evidence=row["neuro_evidence"],
            neurotoxicity_chronic_loael=row["neuro_chronic_loael"],
            neurotoxicity_stot_loael=row["neuro_stot_loael"],
        ),
        axis=1
    )

    df["hazard assessment skin eye respiratory corrosion irritation"] = df.apply(
        lambda row: corr_irrit_rating(
            skin_irr_class=row["skin_irr_class"],
            eye_irr_class=row["eye_irr_class"],
            resp_irr_class=row["resp_irr_class"],
            skin_testing_conclusion=row["skin_irr_conclusion"],
            eye_testing_conclusion=row["eye_irr_conclusion"],
            resp_testing_conclusion=row["resp_irr_conclusion"],
            ph=row["pH"],
        ),
        axis=1
    )

    df["hazard assessment sensitization"] = df.apply(
        lambda row: sensitization_rating(
            skin_sens_clp_class=row["skin_sens_clp_class"],
            resp_sens_clp_class=row["resp_sens_clp_class"],
            skin_sens_mak_class=row["skin_sens_mak_class"],
            resp_sens_mak_class=row["resp_sens_mak_class"],
            skin_testing_conclusion=row["skin_sens_conclusion"],
            resp_testing_conclusion=row["resp_sens_conclusion"],
        ),
        axis=1
    )

    df["hazard assessment organohalogens"] = df.apply(
        lambda row: organohalogens(
            organohalogens_class=row["organohalogen"] if pd.notna(row["organohalogen"]) else "",
        ),
        axis=1
    )

    df["hazard assessment toxic metals"] = df.apply(
        lambda row: toxic_metals(
            toxic_metals_class=row["toxic_metal"] if pd.notna(row["toxic_metal"]) else "",
        ),
        axis=1
    )

    all_assessment_cols = [
        "CAS",
        "hazard assessment carcinogenicity",
        "hazard assessment endocrine disruption",
        "hazard assessment reproductive toxicity",
        "hazard assessment developmental toxicity",
        "hazard assessment oral toxicity",
        "hazard assessment dermal toxicity",
        "hazard assessment inhalative toxicity",
        "hazard assessment neurotoxicity",
        "hazard assessment skin eye respiratory corrosion irritation",
        "hazard assessment sensitization",
        "hazard assessment organohalogens",
        "hazard assessment toxic metals",
    ]

    return df[all_assessment_cols]


### PROGRAM FUNCTION
### This is for running the C2C Automatic assessment

# read the Excel with the product to assess: it needs to hava CAS and homogenous materials specified
db_path = '/Users/juliakulpa/Desktop/DB_tests_mixture_rules/C2Cdatabase.db'
df_product = pd.read_excel('/Users/juliakulpa/Desktop/DB_tests_mixture_rules/Test_for_mixture_rules_v2.xlsx')
# automatic assessment for C2C (original 5 endpoints)
df = automatic_assessment_c2c(db_path, df_product)
# safe the information to sql
save_assessment_df_to_sql(
    df=df,
    db_path=db_path,
    table_name="AUTOMATIC_ASSESSMENT",
)

# automatic assessment for C2C (all endpoints)
df_all = automatic_assessment_c2c_all(db_path, df_product)
# safe the information to sql
save_assessment_df_to_sql(
    df=df_all,
    db_path=db_path,
    table_name="AUTOMATIC_ASSESSMENT_ALL",
    keep_cols=[
        "CAS",
        "hazard assessment carcinogenicity",
        "hazard assessment endocrine disruption",
        "hazard assessment reproductive toxicity",
        "hazard assessment developmental toxicity",
        "hazard assessment oral toxicity",
        "hazard assessment dermal toxicity",
        "hazard assessment inhalative toxicity",
        "hazard assessment neurotoxicity",
        "hazard assessment skin eye respiratory corrosion irritation",
        "hazard assessment sensitization",
        "hazard assessment organohalogens",
        "hazard assessment toxic metals",
    ],
)

