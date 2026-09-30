---
name: c2c_full_assessment_skill
description: Reference knowledge for C2C_assessment_full/C2C_assessment_full.py and C2C_assessment_full_app.py - the "cradle to cradle" chemical/material hazard assessment tool (Percent Assessed, Quick Assessment, and Mixture Rules pipelines). Load this BEFORE reading, explaining, modifying, debugging, or reviewing anything in either file - it covers the three pipelines' entry points, the additive-vs-non-additive endpoint split, a whole class of easy-to-reintroduce "exact-match colour comparison" bugs, CAS/EC identifier handling, the detailed_overview column layout, and output naming conventions. Use it any time someone asks about C2C hazard colours, mixture rules, quick assessment, percent assessed, corrosion/irritation ratings, or a discrepancy between two of this tool's outputs - even if they don't name the file directly.
---

# C2C Assessment Full

## What this program is

A "cradle to cradle" (C2C) chemical/material hazard assessment tool for MAS
(Bill-of-Materials-style) Excel files, cross-referenced against a SQLite hazard
database. It has a CLI (`C2C_assessment_full.py`, run via its `__main__` menu) and a
GUI (`C2C_assessment_full_app.py`, a tkinter app with 3 buttons - one per pipeline).
**The GUI reuses the CLI module's functions by import** (`import C2C_assessment_full as
core`) rather than duplicating logic - a change to a shared helper function
automatically applies to both. But call-site-level choices (which flags/`name_base` to
pass) are duplicated across CLI functions (`run_c2c_assessment_only`,
`run_wint_C2C_mixture_rules`) and GUI functions (`run_quick_assessment`,
`run_mixture_rules`, `run_percent_assessed`), so those must be updated in both places.

## The three pipelines

1. **Percent Assessed** (option A) - composition-completeness check only, NO hazard
   database queried at all. Calculates each material's %-share of the product and of
   its homogeneous material (min/max range) across all tiers/scenarios. Output: a
   "% assessed" score per product (worst case across scenarios) plus missing-composition
   flags. Entry points: `run_with_percentage_assessed` (CLI - a legacy dict-based save
   mechanism, NOT the templated one) and `run_percent_assessed` (GUI - uses
   `save_percent_assessed_workbook`/`build_percent_assessed_detailed_df`, the modern
   templated path).

2. **Quick Assessment** (option C) - fast per-substance hazard screen. Pulls each
   ingredient's own pre-computed hazard colour directly from `COLOUR_ASSESSMENT_C2C` by
   CAS/EC number - no concentration-based combination. The 8 endpoints Mixture Rules
   would combine are labelled `"WITHOUT MIXTURE RULES: {colour}"`, so it's clear the
   number shown is a raw per-substance worst case. Entry points:
   `run_c2c_assessment_only` (CLI), `run_quick_assessment` (GUI). Both call
   `build_c2c_assessment_df(..., include_mixture_rule_db_details=False)` then
   `save_c2c_assessment_output(..., mixture_rules_ran=False,
   name_base="C2C_quick_assessment")`.

3. **Mixture Rules** (option B) - full regulatory-style (CLP/GHS-like) mixture
   assessment, combining each ingredient's hazard with its actual concentration. Entry
   points: `run_wint_C2C_mixture_rules` (CLI), `run_mixture_rules` (GUI). Orchestrated by
   `analyse_the_dataset_with_mixture_rules` -> `mixture_rules_C2C_assessment_from_db` ->
   `mixture_rules_C2C_assessment` (the 4 additive endpoint-group functions, each wrapped
   in `safe_run` so one group's failure doesn't crash the others) plus
   `assessment_with_no_mixture_rules` (the 13 non-additive endpoints).

## The 21 hazard endpoints: two fundamentally different calculations

### The 8 additive / mixture-rule-capable endpoints (`MIXTURE_RULE_CAPABLE_ENDPOINTS`)

Oral/dermal/inhalative acute toxicity, skin/eye/respiratory corrosion & irritation,
sensitization, fish/invertebrate/algae aquatic toxicity. Computed via
concentration-weighted CLP-style additive rules:

- `C2C_acute_toxicity` - ATE (LD50/LC50-based) calculation per route.
- `skin_corr_mixture_rule_c2c` / `eye_corr_mixture_rule_c2c` - concentration-weighted
  RED/GREY/YELLOW/GREEN thresholds. RED if the sum of RED-rated ingredients each ≥1%
  reaches ≥5% (skin) / ≥3% (eye); a sub-1% corrosive still counts **10×** its
  concentration toward the irritant/YELLOW threshold. This is a real CLP mixture-rule
  mechanism, not a bug - "one RED ingredient at 0.8% concentration → YELLOW, not RED"
  is surprising at first glance but correct.
- `resp_corr_rule_c2c` - simple worst-case among ALL ingredients regardless of
  concentration (no weighting, unlike skin/eye).
- `corr_n_irr_mixture_rule_c2c` - combines skin/eye/resp worst-case, then overwrites
  with `NOT_ENOUGH_DB_DATA_PLACEHOLDER` (a relevant known-CAS ingredient is missing its
  DB rating) or `NOT_FULL_COMPOSITION_LABEL` (composition/CAS unknown) as needed.
- `skin_and_resp_sens_c2c` - SCL-based, falling back to the generic CLP thresholds
  (Cat 1/1A ≥0.1%, Cat 1B ≥1.0%) when no substance-specific SCL is defined.
- `acute_aquatic_c2c` / `chronic_aquatic_c2c` / `final_aquatic_c2c` and friends -
  per-species (fish/daphnia/algae) LC50/EC50/NOEC-driven aquatic toxicity with M-factor
  weighting.
- When the additive calc can't produce a trustworthy value (composition unknown, or DB
  data missing for a relevant ingredient), `_apply_incomplete_comp_fallback` replaces
  the placeholder with the worst *individual* raw colour among relevant chemicals,
  wrapped in `INCOMPLETE_COMP_LABEL` (`"INCOMPLETE COMP - NO MIXTURE RULES - CURRENT
  WORST CASE RATING: {colour}"`) or `NOT_ENOUGH_DB_DATA_LABEL` (`"NOT ENOUGH DATA IN DB
  TO CALCULATE MIXTURE RULES - WORST CASE: {colour}"`), depending on which reason
  applies. This wrapping always carries the real colour word - see why that matters
  below.

### The 13 non-additive endpoints (`NO_MIXTURE_RULES_ENDPOINTS`)

Carcinogenicity, endocrine disruption, mutagenicity/genotoxicity, reproductive
toxicity, developmental toxicity, neurotoxicity, terrestrial toxicity, other species
toxicity, persistence, bioaccumulation, combined PB risk flag, combined aquatic risk
flag, climatic relevance/ozone depletion. Computed by `assessment_with_no_mixture_rules`:
per (Product, Hom Mat), a chemical is "relevant" if its concentration is ≥0.01%
(`0.0001` as a fraction - concentration columns are always 0-1 fractions, **never**
percentages, throughout this codebase) - OR, if this endpoint has an SCL specifically
defined for that chemical (SCONCLIM's dynamically-detected `"<label> - Lower/Upper
Limit: (%)"` columns, surfaced in the detailed_overview as `"Does SCL {endpoint}
exist"` / `"SCL {endpoint} value"`), its concentration is above *that chemical's own*
SCL instead of the flat cutoff. An SCL **replaces** the flat cutoff for that specific
chemical - it doesn't add another way in via OR. The hom mat's rating is the worst
rating among relevant chemicals (missing/unrecognised rating = GREY, the same
"missing = GREY" convention used everywhere).

When composition is incomplete (unknown CAS), the *already-computed* worst-case
colour among known chemicals gets wrapped in `INCOMPLETE_COMP_LABEL` too, matching the
8 additive endpoints' pattern - it must **never** be overwritten with a bare,
colour-less `NOT_FULL_COMPOSITION_LABEL` string, because downstream `classify_colour()`
(used by `_worst_colour_by_group` for worst-across-scenarios aggregation, and
elsewhere) reads text back via substring search for "RED"/"YELLOW"/"GREEN" and silently
defaults anything else to GREY. A bare label with no colour word silently discards the
true worst colour and corrupts the result. This was a real, previously-shipped bug: a
RED-rated substance well above the concentration cutoff had its true colour lost this
way, showing GREY in Mixture Rules while Quick Assessment (no such completeness gate)
correctly showed RED for the same substance.

## The critical, easy-to-reintroduce bug class: exact-match colour comparisons

`COLOUR_ASSESSMENT_C2C` (and other DB tables) commonly store a human-annotated value
like `"Manually set to: Green"` instead of the bare word `"GREEN"`. Any code that does
`value == "RED"`, `value.strip().upper() == "GREY"`, or looks `value` up as an exact
dict key against `{"RED","YELLOW","GREEN","GREY"}` will silently fail to recognize an
annotated value and mistreat it as missing/absent - not necessarily as a downgrade to
GREY; depending on the code, it can silently **exclude** the substance from a weighted
sum entirely (undercounting hazard, worse than downgrading), or default it to GREY if
the "missing = GREY" convention applies.

The fix pattern used throughout this codebase: always route a raw DB colour value
through `classify_colour(value)` (substring search - `"RED"` in text → RED, elif
`"YELLOW"` → YELLOW, elif `"GREEN"` → GREEN, else GREY, so an annotation or
wrong-casing is read correctly) before comparing it, or use
`_worst_case_raw_colour(sub_df, colour_col)` for a worst-case-across-rows reduction
(already built on `classify_colour`). `_normalize_corr_rating(value)` is the
corrosion-functions' equivalent - same `classify_colour`-based normalization, but
passes a genuine NaN/missing value through **unchanged** rather than promoting it to
GREY, since `corr_n_irr_mixture_rule_c2c`'s own completeness check
(`missing_rating_mask`) relies on telling "no rating at all" apart from "a real GREY
rating."

**When writing new endpoint-calculation code: never compare a raw DB colour string
with `==` directly - always classify it first.** Three real, previously-shipped bugs
in this codebase were exactly this pattern (in `_worst_case_raw_colour`, in the 4
corrosion/irritation functions, and the discard-the-colour-in-a-placeholder variant
described above). Assume it can happen anywhere a raw DB value reaches a comparison.

## CAS/EC substance identifiers

`clean_cas_values` / `is_valid_cas_number` (`CAS_NUMBER_PATTERN = r"^\d{2,7}-\d{2}-\d$"`)
gate every DB query's identifier list. The DB also supports EC (European Community)
numbers, stored with a literal `"EC "` prefix exactly as typed (e.g. `"EC
430-150-6"`), validated by `EC_NUMBER_PATTERN` / `is_valid_ec_number`. Both formats
pass through `clean_cas_values` unmodified - no normalization, since the DB stores it
exactly as given, so whatever passes validation is used as-is for both the SQL query
parameter and the merge key.

A value that fails *both* patterns (free text, blank, `"not assessed"`, material names
like `"wood"`) is silently dropped from DB queries. If it's a real substance identifier
in some other format (typo, wrong format, an EC number typed without a space), the row
still carries a real (non-`"not assessed"`) CAS value through the whole pipeline, is
**not** recognized as "unknown identity" anywhere (that check is a literal `==
"not assessed"` string comparison, not a format validation), the merge against DB
results silently produces NaN for every hazard column, and it defaults to GREY with
**zero diagnostic trail** anywhere - not even in the "CAS missing from
COLOUR_ASSESSMENT_C2C" log, since that only reports entries that *were* in the
cleaned/valid query list. This is a known, not-yet-fixed gap - worth flagging if you
see something that looks like a malformed identifier silently going GREY.

## Colour rank convention

`COLOUR_RANK` / `_NO_MIXTURE_RULES_RANK` = `{"GREEN": 1, "YELLOW": 2, "GREY": 3, "RED":
4}` - note GREY ranks *above* YELLOW: "unknown/uncertain" is treated as worse than
"known mild hazard." `RANK_TO_COLOUR` is the inverse map. Worst-case aggregation is
always "take the max rank."

## Detailed_overview column layout (per-CAS raw data sheet)

Built by `build_c2c_assessment_df` (shared by Quick Assessment and Mixture Rules' own
detailed_overview; Percent Assessed uses the separate, DB-free
`build_percent_assessed_detailed_df`). Column order:

1. Base composition/contribution columns
2. Harmonized/Organohalogen/Toxic metal/SVHC (`CHEMICALCLASS`, via `extract_chemical_class`)
3. The 21 `"C2C assessment <endpoint>"` hazard colour columns - **must stay contiguous**, see below
4. Mixture Rules only (`include_mixture_rule_db_details=True`): every other raw DB
   value the additive calculation used - LD50/LC50s, CLP classes, sensitisation/aquatic
   classification text, M-factor, `"SCL {endpoint} value"` / `"Does SCL {endpoint}
   exist"` for the 13 non-additive endpoints - via
   `build_mixture_rules_toxicity_info_from_db`
5. Per-tier running-% trace columns - must stay **last**

The RED/YELLOW/GREEN/GREY Excel conditional formatting (`_hazard_col_range` /
`_apply_colour_conditional_formatting` in `save_detailed_overview_only`) is fully
dynamic - it detects the hazard block's column range at write time from wherever the
21 `"C2C assessment "` columns actually land, so reordering surrounding columns never
requires manually adjusting the formatting. But the 21 hazard columns must stay
contiguous (nothing inserted between two of them), since the range detection only
looks at the first and last hazard column's position.

## Output file naming

Three parallel naming schemes, one per pipeline, each producing (general overview
file, detailed_overview folder, detailed_overview file(s)):

| Pipeline | General overview | Detailed_overview folder + file |
|---|---|---|
| Percent Assessed | `C2C_percent_assessed_{file}_{date}` | `C2C_percent_assessed_detailed_overview_{file}_{date}` |
| Quick Assessment | `C2C_quick_assessment_{file}_{date}` | `C2C_quick_assessment_detailed_overview_{file}_{date}` |
| Mixture Rules | `C2C_assessment_{file}_{date}` | `C2C_assessment_detailed_overview_{file}_{date}` |

Controlled via `name_base` / `detail_name_base` params on `save_c2c_assessment_output`
/ `save_c2c_detailed_overview_output`, defaulting to `name_base="C2C_assessment"` (the
Mixture Rules convention) - so only Quick Assessment's call sites need to override it.

## Working on this file

- Verify against a real (or realistic, hand-built) end-to-end test case before
  assuming something is a bug - several apparent bugs turned out to be correct
  CLP-methodology behavior (e.g. the skin-corrosion 10× weighting effect above), while
  some real bugs (the annotated-colour and placeholder-overwrite bugs above) weren't
  visible from code reading alone and needed a concrete row-level trace to actually
  see.
- When fixing a calculation bug, add a fresh, minimal repro (a fake in-memory SQLite DB
  or fake dataframes) proving the fix, *and* a regression check proving unaffected
  cases stay unaffected. This file has no formal test suite - that repro is the only
  real safety net.
- `MIXTURE_RULES_TEMPLATE_PATH` points to a shared Excel template
  (`../C2C_Quick_assessment_program/templates/C2C_assessment_template.xlsx`) used by
  all three pipelines' output-writing functions - it's copied, then overwritten with
  computed values (not template formulas) for overview/percentage_assessed/risk_assessed,
  and written positionally for detailed_overview.
