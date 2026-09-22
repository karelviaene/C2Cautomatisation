# C2C Assessment Tool — What It Does and How

Notes on the two files in this folder, for a colleague who knows the regulatory/chemistry side but not the code.

- `C2C_assessment_full.py` — the calculation engine (~5,000 lines). Reads the MAS file, builds scenarios, queries the hazard database, applies the CLP/GHS mixture rule, writes the Excel outputs.
- `C2C_assessment_full_app.py` — a small desktop window with three buttons, each running one of the pipelines below and saving the results with no further prompts.

## The three modes

**Percent Assessed** — no database involved. It only answers how much of the product's composition is actually known, material by material, across every sourcing combination. Good as a first sanity check on a MAS file, or when composition-completeness is the only question.

**Quick Assessment** — queries the database but does not apply the CLP/GHS mixture (additive) rule. For every CAS number found, it reports that chemical's own raw hazard colour from `COLOUR_ASSESSMENT_C2C` across all 21 C2C endpoints. Nothing is combined mathematically — you're seeing each ingredient's own classification, not the mixture's.

**Mixture Rules Assessment** — the full assessment. For the 8 endpoints where CLP/GHS defines an additive mixture rule (acute oral/dermal/inhalative toxicity; skin/eye/respiratory corrosion-irritation; sensitization; fish/invertebrate/algae aquatic toxicity), it combines each ingredient's hazard and concentration using CLP's summation/cut-off maths, producing one rating per homogeneous material. For the other 13 endpoints (carcinogenicity, mutagenicity, reproductive/developmental toxicity, STOT, persistence, bioaccumulation, etc.), CLP defines no additive rule, so the tool instead checks relevance — any ingredient present above 0.01% or its own specific concentration limit — and reports the worst individual rating among relevant ingredients (`NO_MIXTURE_RULES_ENDPOINTS` in the code). Both groups sit side by side in the summary sheet, but only the first 8 come from an actual mixture calculation.

Three non-colour labels can appear in a Mixture Rules workbook: "Not full comp - no mixture rules applied" means an active ingredient has an unknown percentage or identity, so no mixture maths can run; "INCOMPLETE COMP - NO MIXTURE RULES - CURRENT WORST CASE RATING: <colour>" is specific to the 8 additive endpoints and means the additive calculation couldn't produce a trustworthy number, so the tool falls back to the worst individual raw colour among that material's ingredients (a plain "GREY" elsewhere is a genuine mixture-rule outcome, not this fallback); "NOT ENOUGH INFO TO CALCULATE - NO MIXTURE RULES APPLIED" is the equivalent for the 13 non-additive endpoints, though a missing rating there is usually just absorbed as "GREY" rather than blocking the result.

## The flow of a run

Every pipeline (all three app buttons, plus the older command-line menu) follows the same steps:

1. **Read and clean the MAS file.** The Material Analysis Sheet is read into a table; the tool auto-detects how many ingredient tiers it goes down to from the column headers, tidies percentages/weights/flags/whitespace, and gives each row a stable ID and a readable location in the Product → Homogeneous Material → Tier hierarchy.
2. **Generate scenarios.** A scenario is one way of resolving every alternative-material choice into a concrete bill of materials — e.g. if Tier 2 material X can be Supplier A's or Supplier B's resin, `generate_scenarios` enumerates every combination across the product (three independent two-option choices → 8 scenarios), respecting coupling rules like "only use this hardener if that resin was chosen." The tool reports either the worst case across scenarios or every scenario's own result.
3. **Calculate % composition.** For each active row, the tool works out its percentage of the product and of its own homogeneous material, as a min/max range since MAS files often give ranges, cascading through every tier with a full trace kept for audit purposes.
4. **Pull hazard data.** For Quick and Mixture Rules Assessment, every valid CAS number is looked up in a SQLite database covering pre-computed hazard colours (`COLOUR_ASSESSMENT_C2C`), acute toxicity data (`ORALTOX`/`DERMALTOX`/`INHALTOX`), irritation and sensitization classification, aquatic toxicity (`AQUATOX`), specific concentration limits (`SCONCLIM`), and chemical-class flags (`CHEMICALCLASS`). A CAS not found in a table is logged as a "missing CAS" flag rather than treated as safe.
5. **Compute final ratings.** Percent Assessed stops after step 3, Quick Assessment after step 4. Mixture Rules Assessment runs the CLP additive rule on the 8 capable endpoints and the relevance/worst-case test on the other 13, per homogeneous material, worst-cased across scenarios.
6. **Save the output.**

### Output sheets

- **overview** — one page per product, the one a consultant or client checks first: worst % assessed across scenarios plus a missing-data flag on the left; one row per homogeneous material with its worst-case colour per endpoint, chemical-class flags, and an "Overall C2C Material Health Rating" with a comment naming the driving endpoint(s).
- **percentage_assessed** — the % assessed broken out per product and per scenario, and per homogeneous material and per scenario, so you can see which scenario is worst and why.
- **risk_assessed** — one row per (Product, Homogeneous Material, Scenario), with every endpoint's colour for that scenario — the full detail behind the overview's worst-case picks.
- **detailed_overview** — one row per (CAS, scenario): each ingredient's own raw colour from the database (never the mixture-rule result), its percentages, and the full tier-by-tier trace, for auditing a single ingredient's numbers. Saved separately because it can get very large (see Limitations).

overview, percentage_assessed and risk_assessed live in one summary workbook; detailed_overview is always a separate file.

## Building blocks

Reading the MAS file: `clean_data`, `get_highest_tier`, `add_helper_columns`, `add_final_map`/`build_location`. Building scenarios: `identify_alternative_groups`, `generate_scenarios`, `row_is_active`, and the percentage-cascade functions (`calculate_material_percentages_product`, `calculate_material_percentages_hom_mat`, `calc_row_contribution`), stitched together by `build_selected_scenarios_df`. Database access: `is_valid_cas_number` filters placeholder entries before querying; `extract_info_from_DB`, `extract_colour_assessment_C2C` and `extract_chemical_class` pull the toxicity data, the 21 pre-computed colours, and the class flags, assembled by `build_mixture_rules_toxicity_info_from_db`.

Endpoint calculations are organised into four groups matching CLP's own families, each merging ingredients with hazard data, working out worst-case concentration per homogeneous material, then applying CLP's summation/cut-off thresholds:

- *Acute toxicity* — `C2C_acute_toxicity` computes the mixture's ATE from each ingredient's LD50/LC50 and concentration, for oral, dermal and inhalation routes.
- *Corrosion & irritation* — `skin_corr_mixture_rule_c2c`, `eye_corr_mixture_rule_c2c`, `resp_corr_rule_c2c` apply CLP's additivity formula against fixed % cut-offs.
- *Sensitization* — `skin_and_resp_sens_c2c` uses each ingredient's specific concentration limit where known, or CLP's default 0.1%/1.0% thresholds otherwise, and can raise the tool's strongest flag, `"!!! SENS 1 OR 1A PRESENT !!!"`, for a Category 1/1A sensitizer above threshold.
- *Aquatic toxicity* — `acute_aquatic_c2c`/`chronic_aquatic_c2c`, combined by `final_aquatic_c2c`, apply the GHS summation method (with M-factor scaling) separately for fish, daphnia and algae, kept independent rather than forced to agree.

`mixture_rules_C2C_assessment_from_db` runs all four groups for one product and merges in `assessment_with_no_mixture_rules` for the 13 non-additive endpoints; `analyse_the_dataset_with_mixture_rules` orchestrates this across every scenario. The output-sheet builders (`build_overview_df`, `build_percentage_assessed_df`, `build_risk_assessed_df`, `build_c2c_assessment_df`, and their Percent Assessed counterparts) turn the results into the tables above. `_overall_c2c_rating` is worth knowing specifically: it isn't just the worst colour across all 21 endpoints — fish/invertebrate/algae toxicity, persistence, bioaccumulation and the PB risk flag are excluded from it entirely, some endpoints ignore a GREY result, and reproductive + developmental toxicity are resolved as a coupled pair first. The `save_*` functions write everything into the Excel template, generating formula rows to match project size and splitting oversized projects into multiple files.

**The desktop app** is thin: `run_percent_assessed`, `run_quick_assessment` and `run_mixture_rules` each call the engine functions above and hand results to `MixtureRulesApp`, a `tkinter` window with three mode buttons, file pickers, a log box, and a Run button that remembers your last-used paths. The older command-line menu still exists and asks interactive yes/no questions about saving extra scenario detail; the app always saves the same fixed outputs with no prompts.

## Known limitations

**Row caps on large projects.** The summary workbook's template only scans a bounded number of rows (`C2C_ASSESSMENT_TEMPLATE_MAX_ROWS`, 5,000) — a 30,000-row project with an unbounded scan range previously crashed Excel on opening, hence the conservative cap. detailed_overview has a higher cap (`DETAILED_OVERVIEW_ROW_CAP`, 50,000); past that the tool splits output into one file per product, and further into scenario-range batches if even one product is too big, saved into a `detailed_assessment_<name>_<date>/` subfolder. The older command-line Quick Assessment option is stricter than the app's button: it hard-stops on an oversized project instead of splitting automatically.

**A missing chemical is never treated as safe.** A CAS not found in a database table is logged and flagged rather than defaulted to green. Older logic sometimes defaulted such gaps to the worst rating instead, which was just as wrong the other way — the current logic treats a genuinely missing rating as GREY, a distinct "unknown" state.

**The "current worst case rating" fallback isn't a real mixture-rule result** — it's a conservative placeholder for when the additive calculation can't be trusted, and a sign the composition needs completing before the mixture rule can run. Not all 21 endpoints count equally toward the Overall C2C Material Health Rating either: aquatic toxicity, persistence, bioaccumulation and the PB risk flag never influence it, some endpoints ignore GREY, and reproductive/developmental toxicity are merged into one colour first — check the comment column for what drove the result.

**Percent Assessed never touches the database**, so it can't flag missing CAS numbers or show hazard information at all. One unknown ingredient — a "not assessed" CAS or unknown percentage — marks the whole material incomplete for every mixture-rule-capable endpoint, even if every other ingredient in it is fully characterised.
