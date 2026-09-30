---
name: c2c_database_program_skill
description: Reference knowledge for C2C_database_program/DB_communication_core.py and DB_communication_app.py - the tool that screens a list of CAS/EC numbers against ECHA CnL, syncs a SQLite C2C hazard database with CPS Excel files on disk, and exports reports. Load this BEFORE reading, explaining, modifying, debugging, or reviewing either file - it covers the CAS/EC filename regex conventions (and a class of bugs already found there), the run_cas_screening pipeline order, the "Check CnL data via API" toggle, why folder-scanning warnings must only be logged once per run, and the GUI's threading/logging conventions. Use it any time someone asks about CAS vs EC number handling, CPS filenames not being recognized, CnL/ECHA API screening, "CAS missing from DB" messages, or duplicate/misleading log output from this tool - even if they don't name the file directly.
---

# C2C Database Communication

## What this program is

A tool that keeps a SQLite hazard database (`C2C_DATABASE` + related tables) in sync
with a folder of per-substance "CPS" Excel workbooks, and cross-checks everything
against the ECHA CnL (Classification & Labelling) API. Two files:

- **`DB_communication_core.py`** - all the logic (regex parsing, SQLite access, the
  CnL API calls, Excel read/write). No GUI code, no side effects at import time.
- **`DB_communication_app.py`** - a tkinter GUI wrapper. It **only** calls into
  `core.*` (`import DB_communication_core as core`) - it never reimplements parsing
  or DB logic itself. If a change should affect behavior, make it in `core.py`; the
  app just needs the matching parameter threaded through.

There is also `streamlit_version/streamlit_DB_communication_CURRENT.py` - an older,
**self-contained** Streamlit version that does NOT import `core.py` and is not kept in
sync automatically (this is stated explicitly in `DB_communication_app.py`'s header
comment). Don't assume a fix in `core.py` also applies there - check with whoever
still uses it before assuming it's dead.

## Folder layout, derived from `db_path` alone

`derive_paths(db_path)` computes every other path purely from the database file's
location - there's no separate config file for this. The convention is:

```
<project_root>/
  Database/<name>.db          <- db_path itself lives one level down from project_root
  CPS/                         <- folder_excels: all the "CPS_CAS ..."/"CPS_EC ..." workbooks
  JSON/                        <- CnL API responses get dumped here, one per run
  Streamlit info/NextSDS API key.txt   <- the CnL API key, first line of the file
  Template/CPS_CAS TEMPLATE V2.xlsm
  Downloads from Streamlit/
    Report exports/            <- make_cas_report_excel() output
    CPS downloads/             <- CPS files generated *from* the DB for CAS missing a file
  Database/Backups/            <- make_a_backup() writes here before every screening run
```

If someone reports "wrong folder" errors, check `db_path` first - moving the `.db`
file to a different depth relative to `CPS`/`JSON`/etc. breaks everything downstream
since none of those paths are configurable independently.

## CAS/EC filename parsing - read this before touching any regex here

This is where every real bug in this file has come from so far. The three regexes
(`file_pattern`, `cas_pattern`, `ec_pattern`) are **duplicated** across three
functions: `check_if_excel_is_in_folder`, `is_DB_data_up_to_date_with_excel`, and
`extract_info_form_excel_to_DB`. If you fix or extend one, grep for the other two -
there is no shared single source of truth for these patterns (a good thing to fix if
you're ever in there for another reason).

- **`file_pattern`**: `CAS (.*?)\.(xlsx|xlsm)$` - the gate. A filename only gets looked
  at further if it contains `"CAS "` somewhere before the extension. A file with no
  `"CAS "` in its name (e.g. missing that literal token entirely) is invisible to the
  whole pipeline, silently.
- **`cas_pattern`**: requires digits immediately after `"CAS "`, e.g.
  `CAS 71-43-2.xlsx`. This must match first; only if it fails does the code fall back
  to EC.
- **`ec_pattern`**: **captures the `"EC "` prefix together with the digits**, e.g.
  `EC 947-819-8`, matching the exact string format expected in the CAS-list Excel's
  `"CAS"` column (yes, EC entries go in the column literally named "CAS" - see next
  section). This looks redundant with `cas_pattern`'s separate `"CAS "` requirement,
  but it's intentional: a file can be named `CPS_CAS EC 947-819-8.xlsx` (the literal
  word `"CAS"` is still in the filename to pass `file_pattern`, and `"EC ..."` is the
  actual identifier `ec_pattern` extracts).

**Two real bugs already fixed here, worth knowing about in case they resurface in a
refactor:**
1. `ec_pattern` used to capture only the digits (`947-819-8`), not the `"EC "` prefix.
   Since the Excel `"CAS"` column stores the value *with* the prefix (`"EC 947-819-8"`),
   the extracted identifier could never equal the list entry, so every EC-only
   chemical silently failed every `in CAS_list` check downstream.
2. In `check_if_excel_is_in_folder`, the EC branch extracted `inv_number` but never
   added it to the `inv_in_folder` set (unlike the CAS branch, which does). A file
   could match perfectly and still be reported as "not in folder." If you ever add a
   third identifier type, make sure whatever branch handles it also updates
   `inv_in_folder` - it's easy to copy the `if match: ... else: ...` shape and forget
   the `.add()` call in the new branch.

**How to verify a filename/regex change**: don't reason about the unicode dash
character classes (`[-‐‑–—]` etc. - these patterns intentionally accept several
dash-like unicode characters, not just ASCII hyphen) by eye. Test directly:

```python
import re
ec_pattern = re.compile(r'(EC \d{2,7}[-‐‑–—]\d{3}[-‐‑–—]\d{1})')
m = ec_pattern.search("CPS_CAS EC 947-819-8.xlsx")
print(m.group(1) if m else None)   # should print "EC 947-819-8"
```

## Untracked/unparseable CPS files - log once per run, not once per scan

`check_if_excel_is_in_folder`, `is_DB_data_up_to_date_with_excel`, and
`extract_info_form_excel_to_DB` each independently list the whole CPS folder, and
`run_cas_screening` calls some of them more than once per run (once for `found`
CAS/EC, once for `not_found`). A file with no CAS/EC number in its name (e.g. a
trade-name-only file like `CPS_CAS Lanasol Blue 3G.xlsx`) would previously get logged
as a warning by *every* one of those scans, producing repeated, confusing output for
the exact same file.

The fix: `find_files_without_cas_or_ec(folder_excels)` does this scan **once**, and is
called a single time near the top of `run_cas_screening` (right after the CAS list is
loaded), logging one plain (uncolored - deliberately not `:blue[...]`/`:red[...]`, so
it reads as neutral information rather than an error) summary line. The three
per-function scanners still silently skip these files (`continue`) but no longer log
anything themselves. **If you add a fourth place that scans the CPS folder, don't
give it its own warning log** - either call `find_files_without_cas_or_ec` yourself if
you need the list, or just let the run-level summary already logged upstream cover it.

## The `run_cas_screening` pipeline, in order

1. Build the identifier list (`CASall`) - either `load_cas_list_from_excel` (reads the
   `"CAS"` column of an uploaded Excel; this column holds *both* CAS and EC entries,
   the latter written as `"EC ..."`) or `load_cas_list_from_folder` (derives the list
   from CPS filenames already on disk - **CAS-only today**, it doesn't parse EC-named
   files; extending it to do so is a real gap, not yet fixed as of this writing).
2. `find_files_without_cas_or_ec` - one-time untracked-file summary (see above).
3. `make_a_backup(db_path, ...)` - always runs, before anything touches the DB.
4. **`check_json` (the ECHA CnL API call)** - gated by the `check_cnl` parameter /
   the GUI's "Check CnL data via API" checkbox. When off, `CnL_json` is set to `[]`
   and the API is never called at all (no network request, not even a dry one).
5. `checking_if_CAS_exists` - splits `CASall` into `found`/`not_found`. Note: it
   requires a match in **both** `C2C_DATABASE.ID` and `GENERALINFO.ref` for an entry
   to count as "found" - a row in only one of those tables (e.g. a partially-written
   previous run) counts as not found and gets reprocessed.
6. For `not_found`: `check_if_excel_is_in_folder` splits them into "has a CPS file
   already" vs. "missing entirely" (`CAS_not_in_DB_and_not_in_excel` - this is the
   list behind the `"CAS missing from DB and not in the Excel files"` message). Files
   that do exist get ingested via `extract_info_form_excel_to_DB`.
7. For `found`: `check_if_excel_is_in_folder` again (same folder, same regexes, called
   a second time with a different input list - this is *why* any per-file logging in
   that function would double up, see previous section), then
   `is_DB_data_up_to_date_with_excel` (3-year staleness check + "is the Excel file
   newer than the DB row" check) to decide which found entries need re-ingesting.
8. **CnL-info-into-DB step** - also gated by `check_cnl`. When on:
   `insert_json_info_to_DB(CnL_json, db_path, all_CAS_to_check_CnL)` matches each
   entry by `identifier == target_cas` in the downloaded JSON (not `casNumber` - that
   field lives one level deeper, inside `results`) and updates `ECHACHEM_CL`. When
   off, this whole step is skipped and the report's CnL-related columns are just
   empty for this run - **everything else in the pipeline still ran normally**, this
   is the one deliberately-skippable step.
9. `make_cas_report_excel` - writes the run's full summary to
   `Downloads from Streamlit/Report exports/`, regardless of whether CnL was checked.

**Open question, not yet resolved**: `check_json` sends the whole `CASall` list as one
flat payload to the CnL/ECHA API. Whether that API can resolve an `"EC ..."`-prefixed
identifier the same way it resolves a bare CAS number is unconfirmed from the code -
there's an unused, half-built `formatted_ec` dict shape in `load_cas_list_from_excel`
(`{"casNumber": "", "ecNumber": ec}`) suggesting someone intended a different payload
shape for EC lookups that was never wired in. Don't assume EC entries get valid CnL
data back without checking a real run's `JSON/` output.

## GUI conventions in `DB_communication_app.py`

- **`PathRow`**: inline Browse-button pickers, no pop-up dialogs. Last-used paths
  persist in `~/.db_communication_app_config.json` via `load_config`/`save_config`/
  `_remembered`/`_remember`.
- **Checkboxes gate pipeline behavior, not GUI-only behavior**: e.g.
  `use_cps_folder_var` and `check_cnl_var` are read once in `_on_run_screening` and
  passed straight through as `core.run_cas_screening(..., use_cps_folder=..., check_cnl=...)`.
  Don't implement a checkbox's effect in the app layer - thread it into `core.py` as a
  parameter, the same way both of these are done, so the CLI/other callers of `core`
  get the same behavior.
- **Threading**: screening/export run in a background `threading.Thread` (`daemon=True`)
  so the Tk mainloop stays responsive. `core.log_callback` is repointed to
  `self._log_threadsafe`, which hops back to the main thread via `self.after(0, ...)`
  before touching any widget. Never call a tk widget method directly from the worker
  thread.
- **Color markup in log messages**: `core.py`'s `_log()` calls sometimes wrap text in
  `":red[...]"` / `":blue[...]"` / `":green[...]"` - a leftover convention from when
  this logic lived in a Streamlit app (`st.markdown`-style color spans). The tkinter
  app's `COLOR_MARKER_RE` parses this back out and applies a text tag; anything without
  a recognized marker prints in plain/default color. When adding a new log line, only
  add a color marker if the message is genuinely a warning (`:red[...]`) or a phase
  marker (`:blue[...]`)/success (`:green[...]`) - purely informational lines (like the
  untracked-files summary) should stay unmarked/plain, matching the existing
  convention for "FYI, not an error" output.
- **`_on_close`**: warns before quitting mid-run, because the pipeline commits to
  SQLite multiple times per CAS/EC rather than once atomically at the end - closing
  mid-run can leave the DB partially updated for whichever entry was in flight.

## How to verify a change to this pipeline

Point the GUI (or a short script calling `core.run_cas_screening` directly) at a
throwaway copy of a real database + CPS folder - never the live database, since
`make_a_backup` runs before mutation but a bad run can still leave partially-updated
rows if you're mid-edit on the ingestion functions themselves. For a narrow regex or
folder-scanning change, a synthetic test is faster and doesn't need a real database at
all - create an empty temp dir, drop a few strategically-named empty files in it
(`CPS_CAS 71-43-2.xlsx`, `CPS_CAS EC 947-819-8.xlsx`, `CPS_CAS Trade Name.xlsx`), and
call `find_files_without_cas_or_ec`/`check_if_excel_is_in_folder` directly to check
what gets matched, skipped, or (still) misreported.
