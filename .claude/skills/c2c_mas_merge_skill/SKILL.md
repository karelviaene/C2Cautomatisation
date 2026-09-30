---
name: c2c_mas_merge_skill
description: Explains how MAS_generation.py and MAS_generator_app.py in C2C_MAS_merge_program work together to merge a MAS excel's tier sheets and export a unique-CAS list, plus the conventions (return shapes, sheet naming, blank-key matching rules, GUI threading rules) any change to them must respect. Use this whenever working on this repo's MAS merge tool - editing either file, debugging a wrong/missing/duplicated merge result, changing the unique-CAS export, or adding a GUI feature. Trigger even without the word "skill" - e.g. "the tier merge is dropping rows", "add an option to the MAS merge app", "why did two supplier rows get merged that shouldn't have", "the CAS export button is disabled".
---

# C2C MAS Merge

This repo has two files that work together, and they're easy to get wrong in ways that don't crash - they just silently produce a wrong Excel file. Read this before touching either one.

## The two files, and why they're split this way

**`MAS_generation.py`** is the merge logic. It has zero side effects at import time - the entire legacy console/dialog flow lives under `if __name__ == "__main__":` at the bottom. That guard is load-bearing: it's what lets `MAS_generator_app.py` (and any test script) `import MAS_generation as core` and call its functions directly, without a wall of tkinter pop-ups and `input()` prompts firing first. If you ever add a new top-level statement to this file, ask whether it belongs inside that guard.

Inside the guard only (CLI path, don't call these from the GUI):
- `open_excel_file()` / `select_output_file()` - tkinter file dialogs with `messagebox` pop-ups.
- `get_max_tier()` / `get_choice()` - blocking `input()` prompts.

Reusable core (this is what the GUI, and any future caller, should use):
- `normalize_key_column(series, blank_context=None)` - see the dedicated section below, this is the function most bugs have come from.
- `join_tier_sheets(input_file, max_tier)` and `join_tier_sheets_with_suppliers(input_file, max_tier)` - the two merge variants.
- `join_to_excel(final_df, output_file)` - writes the merged result with an xlsxwriter table.
- `export_unique_cas(final_df, max_tier, output_file)` - pulls every value out of the "CAS Tier N" columns (1..max_tier) of an already-merged dataframe and writes the deduplicated list to its own Excel file. This is a separate, optional step, not part of the merge itself - see below.

**`MAS_generator_app.py`** is a tkinter GUI wrapper. It should never reimplement merge logic - it calls into `core.*`. If a change means the merge behaves differently, make it in `MAS_generation.py` and let the app pick it up; don't patch behavior in the app layer.

## Sheet and column naming the merge relies on

- Start sheet: `"PR-HM-T1"` or `"P-HM-T1"`.
- Tier transition sheets: `"T{i}-T{i+1}"` or `"T{i}_T{i+1}"` (both dash and underscore forms are checked).
- Join/key columns: `"Tier N Material"` and, in the suppliers variant, `"Tier N Supplier"`.
- CAS columns (used only by `export_unique_cas`, not by the merge itself): `"CAS Tier N"`, optionally with a `_T{i+1}` suffix if a merge collision suffixed it (the same kind of collision `join_tier_sheets` handles for `"Tier N Material"`/`"Tier N Supplier"`) - both forms are matched by `export_unique_cas`'s regex so a duplicate column name doesn't silently drop that tier's CAS values.

A missing tier-transition sheet is treated as a normal stopping condition (the merge just stops there), not an error - see "reached_tier" below. A sheet that *is* found but is missing its expected columns is treated as a real data problem and raises `KeyError` in both merge variants.

## `normalize_key_column` - read this before touching join keys

This is the function that turns a raw Excel column into the string used to join two sheets. It exists because of two non-obvious pandas behaviors that caused real bugs here, plus a deliberate design choice about blanks that's easy to get backwards:

1. **`pandas.merge` treats `NaN == NaN` as a match.** This is the opposite of SQL NULL semantics, and it's easy to assume otherwise. Left completely unhandled, every blank cell in one sheet would join to every blank cell in the other sheet.
2. **But a blank is sometimes a legitimate, meaningful value** - e.g. "no supplier assigned yet for Tier 2" is itself information, and two rows that are both blank at "Tier 2 Supplier" *should* still join to each other the same way two rows with the same real value would. So blanks aren't just suppressed - they're given a sentinel scoped to `blank_context` (e.g. `"material_2"`, `"supplier_3"`). Blanks that share the same `blank_context` match each other; blanks from a *different* context (a blank "Tier 3 Material" vs a blank "Tier 2 Supplier") never cross-match, because their sentinels differ. Every call site in `join_tier_sheets`/`join_tier_sheets_with_suppliers` passes a `blank_context` for exactly this reason - **don't drop it** if you're refactoring these functions, or you'll either reintroduce global blank-to-blank fan-out (by omitting it) or accidentally merge unrelated blank columns together (by reusing the same context string for two different columns).
   - `blank_context=None` (the default, used when you call this function for something that *isn't* one of the tier merge columns) falls back to a per-row unique sentinel, so those blanks never match anything. Use this default when blanks genuinely shouldn't join to anything; don't reuse it for a new tier-like join without thinking about whether blank-to-blank matching is actually wanted there.
3. **A numeric column with any blank cells gets read by pandas as `float64`.** So a material code like `100234` becomes `100234.0`, and a naive `str()` cast produces `"100234.0"` - which will never match the same code stored as an `int` or a plain string elsewhere. `normalize_key_column` special-cases whole-number floats back to their int form before stringifying.

Apply this function uniformly to **every** "Normalized Material/Supplier N" column, including Tier 1. Tier 1 used to be special-cased with a plain `.str.strip().str.lower()` and no dtype safety, which is exactly what caused the historical `AttributeError` (on numeric columns) and a blank-fan-out bug. If you're adding a third merge variant or a new tier-like join anywhere in this file, route it through `normalize_key_column` (with a sensible `blank_context`) rather than writing a new ad hoc normalization.

If you need to change this function's behavior, don't reason about pandas' NaN/merge semantics from memory - write a quick synthetic test instead (see "How to verify a change" below). That's how the `NaN == NaN` behavior was actually confirmed here, and it's the fastest way to check whether a `blank_context` choice produces the matching you expect.

## The `(final_df, reached_tier)` return contract

Both `join_tier_sheets` and `join_tier_sheets_with_suppliers` return a **tuple**, not just a dataframe: `(final_df, reached_tier)`.

- `reached_tier` is the highest tier actually merged. It equals the requested `max_tier` on a full success, and is lower than it if an expected tier-transition sheet was missing (the loop just `break`s - this is expected, not exceptional).
- A missing *column* on a sheet that *was* found is different: that's a real malformed-data problem, so both variants raise `KeyError` for it. (Don't reintroduce the historical asymmetry where `join_tier_sheets_with_suppliers` silently returned a partial `final_df` here instead of raising, while the materials-only variant raised - both must behave the same way.)
- Practical consequence: "no exception raised" means "either fully succeeded, or gracefully truncated - check `reached_tier` before assuming the requested tier depth was actually reached."

`MAS_generator_app.py`'s `run_pipeline()` wraps this further - it returns `(final_df, output_path, reached_tier)`, a **3-tuple**, because the GUI keeps the merged dataframe around afterward for the "Export unique CAS list" button (see below) rather than discarding it once the Excel file is written.

If you change what any of these functions return, update every caller in the same change: the `__main__` CLI block at the bottom of `MAS_generation.py`, and `run_pipeline()` / `_worker()` / `_on_success()` in `MAS_generator_app.py`. Grep for `join_tier_sheets(`, `join_tier_sheets_with_suppliers(`, and `run_pipeline(` to find all call sites before assuming you got them all.

## The unique-CAS export

`export_unique_cas(final_df, max_tier, output_file)` is a separate, optional post-processing step on an already-merged dataframe - it does not affect the merge itself and isn't required for it to succeed. Key behaviors worth knowing before changing it:
- It matches "CAS Tier N" columns for N from 1 to `max_tier`, including the `_T{i+1}`-suffixed duplicates a merge collision can create (see the sheet-naming section above) - if you add a new way for CAS columns to get suffixed, update the regex (`cas_column_pattern`) to match.
- Values are exported **exactly as they appear** in the source - no stripping or case-folding - except that blank/whitespace-only cells and case-insensitive `"not assessed"` entries are excluded, and deduplication is on `(type(value), value)` rather than plain equality, specifically so a CAS stored as a float in one tier and an int in another (which Python's `==` would otherwise silently collapse) doesn't get merged into a single entry while other representations survive. If you touch the dedupe logic, keep this type-aware behavior or you'll get inconsistent exports depending on which sheet happened to store the numeric CAS as which dtype.
- It raises `KeyError` if no matching CAS columns exist at all - both the CLI (`__main__` block) and the GUI (`_on_export_cas`) catch this specifically and treat it as "nothing to export" rather than a crash. Don't let a change here turn this into an unguarded exception that would surface as a raw traceback to a user who simply doesn't have CAS columns in their file.
- In the GUI, this is a **manual, separate action** - the "Export unique CAS list..." button, enabled only after a successful merge and disabled again on any failed run (see `_last_final_df`/`_last_max_tier`/`_last_output_path`, reset to `None` in `_on_failure` specifically so a stale dataframe from a previous run can't be exported under a new, failed run's context). It writes to its own auto-numbered filename (`CAS_list_<merged-file-name>_<date>.xlsx`, `(2)`, `(3)`, ... if one already exists) rather than overwriting - if you change this naming scheme, keep the collision-avoidance loop, since there's no save dialog to let the user pick a different name.

## GUI conventions in `MAS_generator_app.py`

This app mirrors a sibling app in this codebase, `C2C_Quick_assessment_program/MAS_quick_C2C_assessment_app.py` - if you change a pattern in one, check whether the other should match for consistency.

- **`PathRow`**: inline Browse-button pickers, no pop-up dialogs. Last-used paths persist in `~/.mas_merge_app_config.json` via `load_config`/`save_config`/`_remembered`/`_remember`.
- **Threading**: the merge runs in a background `threading.Thread` (`_worker`) so the Tk mainloop stays responsive. Every widget mutation triggered from that thread goes through `self.after(0, ...)` - see `_log_threadsafe`, `_on_success`, `_on_failure`. Never touch a tk widget directly from the worker thread; if you add a new background operation, follow this same pattern rather than calling a widget method inline. (The CAS export currently runs synchronously on the main thread in `_on_export_cas` since it's fast and operates on data already in memory - if it's ever changed to re-read the source Excel or do something slow, move it to a background thread with the same `self.after(0, ...)` discipline.)
- **Log capture**: `core`'s `print()` progress lines are captured into the log widget via a `LogStream` + `contextlib.redirect_stdout`, guarded by a module-level `_stdout_lock`. `redirect_stdout` swaps `sys.stdout` for the whole process, not just the calling thread - the lock is defense-in-depth even though the UI already serializes runs by disabling the Run button during execution. Keep both protections if you touch this code.
- **Status/animation language**: a bouncing-magnifying-glass spinner while running; confetti *only* on a full, complete success. When `reached_tier < max_tier` (a partial merge from a missing tier sheet), show the amber/"WARN" status with no confetti - this distinction is intentional (checked in `_on_success` via `if reached_tier < max_tier`), don't collapse it back into a single generic "Done" state that would hide a truncated merge from the user. A partial merge still enables the CAS-export button, since a partial merge still has valid CAS data for the tiers it did reach.
- **Filename field**: must reject anything where `os.path.basename(filename) != filename` (catches absolute paths and `../` traversal) *before* building `output_path` with `os.path.join(saving_dir, filename)` - `os.path.join` silently discards `saving_dir` if `filename` is absolute, which would otherwise let a mistyped filename write outside the chosen folder.

## How to verify a change

Don't hand-reason about pandas merge/dtype behavior, or about which `blank_context` values will or won't collide - it's counterintuitive in exactly the ways described above. Instead, build a tiny in-memory Excel file and run the real function:

```python
import pandas as pd, tempfile, os
import MAS_generation as core

with tempfile.TemporaryDirectory() as d:
    path = os.path.join(d, "mas.xlsx")
    t1 = pd.DataFrame({"Tier 1 Material": [100234.0, float("nan"), 200000.0]})
    t1_t2 = pd.DataFrame({"Tier 1 Material": [100234, float("nan")], "Tier 2 Material": ["x", "y"]})
    with pd.ExcelWriter(path) as w:
        t1.to_excel(w, sheet_name="PR-HM-T1", index=False)
        t1_t2.to_excel(w, sheet_name="T1-T2", index=False)
    df, reached_tier = core.join_tier_sheets(path, 2)
    print(df[["Tier 1 Material", "Tier 2 Material"]])
    print("reached_tier:", reached_tier)
```

Check: the numeric-coded row should match across sheets, a blank "Tier 1 Material" row should pick up whatever blank-row value the other sheet has at that same tier (this is now intentional - see the `blank_context` section), and `reached_tier` should reflect how far the merge actually got. For `export_unique_cas`, build a small dataframe with a couple of "CAS Tier N" columns (including a duplicate/suffixed one and a blank/"not assessed" entry) directly rather than round-tripping through Excel, and check the output's row count and dtypes match what you expect. This pattern (synthetic in-memory data, run the real core function, inspect the result) is the fastest way to catch a regression in this file - much faster than tracing pandas semantics by eye.
