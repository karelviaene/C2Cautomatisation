### Mixture Rules Assessment - desktop app
### A small tkinter window with 3 buttons - one per pipeline in
### C2C_assessment_full.py - that runs the picked pipeline to
### completion with no interactive y/n prompts (unlike that module's own CLI menu, which
### asks whether to additionally save "all scenarios"/"selected scenarios" - this app
### always saves the core outputs and skips those optional extras, matching "don't prompt
### the user for options, just make buttons").
### Reuses C2C_assessment_full.py's logic by import (that module now
### has a __main__ guard, so importing it does not auto-run its own CLI menu) rather than
### duplicating it - keep the two in sync if the pipeline itself changes.

import os
import sys
import json
import threading
import traceback
from datetime import datetime

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import C2C_assessment_full as core

APP_BG = "#ffffff"
TEXT = "#000000"
ACCENT = "#16a34a"
GOOD = "#16a34a"
# --- Unused: no remaining references as of 2026-09 cleanup, kept for reference ---
# BAD = "#000000"

# Remembers the last-used save folder (and MAS/DB file paths) across runs of the app - same
# mechanism as MAS_quick_C2C_assessment_app.py's own load_config/save_config, but its own
# config file so the two apps don't clobber each other's remembered paths.
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".c2c_assessment_full_app_config.json")


def load_config():
    """Load the app's remembered-paths config (last MAS/DB file and save folder) from CONFIG_PATH, returning {} if it doesn't exist or can't be read."""
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_config(cfg):
    """Persist the app's remembered-paths config dict to CONFIG_PATH, silently ignoring write failures."""
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(cfg, f)
    except Exception:
        pass  # not critical - just means the next run won't remember the folder


def _timestamp():
    """Return today's date as a "YYYYMMDD" string, used to timestamp output file names."""
    return datetime.now().strftime("%Y%m%d")


def run_percent_assessed(mas_path, saving_dir, log):
    """Percent assessed only - no DB, no hazard endpoints (option "Percent Assessed").

    Output is exactly two things, same as Quick Assessment/Mixture Rules - no other files:
    - a summary file with "overview"/"percentage_assessed" only (no "risk_assessed" sheet -
      that sheet is entirely built from hazard colours, which this pipeline never has since
      it never queries the database - and "overview" has no hazard columns either);
    - a separate detailed_overview file (or files, split/subfoldered for large projects)
      with each row's own composition data (no hazard colours).
    """
    file_name = os.path.basename(mas_path)
    df = core.pd.read_excel(mas_path)

    log("Reading MAS file and detecting tier depth...")
    max_tier = core.get_highest_tier(df, core.col_CAS)
    log(f"Max tier found: {max_tier}")

    df = core.clean_data(df, max_tier)
    df = core.add_helper_columns(df, max_tier)
    df = core.add_final_map(df, max_tier)

    cas_count, _ = core.count_CAS_unique(df, "CAS")
    log(f"Unique CAS found: {cas_count}")

    log("Generating scenarios...")
    df = core.identify_alternative_groups(df, max_tier)
    scenarios = core.generate_scenarios(df, max_tier)
    scenario_ids = [s["scenario_id"] for s in scenarios]
    log(f"Scenarios generated: {len(scenarios)}")

    log("Building the detailed per-CAS dataset (this can take a while for large projects)...")
    all_scenarios_df = core.build_selected_scenarios_df(df, scenarios, scenario_ids)
    detailed_df = core.build_percent_assessed_detailed_df(all_scenarios_df)

    time_str = _timestamp()
    file_stem = os.path.splitext(file_name)[0]

    log("Saving percent-assessed summary (overview/percentage_assessed)...")
    saving_summary = os.path.join(saving_dir, f"C2C_percent_assessed_{file_stem}_{time_str}.xlsx")
    core.save_percent_assessed_workbook(detailed_df, saving_summary, write_detailed=False)

    log("Saving detailed_overview...")
    detailed_paths = core.save_c2c_detailed_overview_output(
        detailed_df, saving_dir, file_name, time_str, name_base="C2C_percent_assessed_detailed_overview"
    )

    log("Saved percent-assessed summary and detailed_overview files.")

    return [saving_summary] + detailed_paths


def run_quick_assessment(mas_path, saving_dir, db_path, log):
    """C2C assessment only, no mixture rules (option C) - all scenarios, no selection.

    Output matches MAS_quick_C2C_assessment_static.py's run_quick_c2c_assessment_static()
    1:1: same save_c2c_assessment_output() call, same "no hard row cap" behaviour (a large
    project splits the detailed_overview into multiple files/batches instead of raising).
    """
    file_name = os.path.basename(mas_path)
    df = core.pd.read_excel(mas_path)

    log("Reading MAS file and detecting tier depth...")
    max_tier = core.get_highest_tier(df, core.col_CAS)
    log(f"Max tier found: {max_tier}")

    df = core.clean_data(df, max_tier)
    df = core.add_helper_columns(df, max_tier)
    df = core.add_final_map(df, max_tier)

    cas_count, _ = core.count_CAS_unique(df, "CAS")
    log(f"Unique CAS found: {cas_count}")

    log("Generating scenarios...")
    df = core.identify_alternative_groups(df, max_tier)
    scenarios = core.generate_scenarios(df, max_tier)
    scenario_ids = [s["scenario_id"] for s in scenarios]
    log(f"Scenarios generated: {len(scenarios)}")

    log("Building the detailed dataset (this can take a while for large projects)...")
    all_scenarios_df = core.build_selected_scenarios_df(df, scenarios, scenario_ids)
    log(f"Detailed rows generated: {len(all_scenarios_df)}")

    log("Pulling C2C colour assessment hazards from the database...")
    c2c_df = core.build_c2c_assessment_df(all_scenarios_df, db_path)
    cas_list_all = core.clean_cas_values(c2c_df["CAS"].tolist()) if "CAS" in c2c_df.columns else []
    _, missing_cas_df = core.extract_colour_assessment_C2C(cas_list_all, db_path)

    time_str = _timestamp()
    log("Saving C2C assessment (summary + detailed_overview)...")
    saved_paths = core.save_c2c_assessment_output(
        c2c_df, missing_cas_df, saving_dir, file_name, time_str,
        mixture_rules_ran=False, name_base="C2C_quick_assessment"
    )
    log("Saved C2C assessment summary and detailed_overview files.")

    return saved_paths


def run_mixture_rules(mas_path, saving_dir, db_path, log):
    """Full % assessed + mixture rules (including the no-mixture-rules endpoints) assessment (option B).

    Output is exactly two things (same "overview"/"percentage_assessed"/"risk_assessed"/
    "detailed_overview" sheet shapes used by the Quick Assessment program) - no other files:
    - a summary file with the mixture-rule-computed overview/percentage_assessed/risk_assessed
      sheets (per (Product, Homogenous Material[, Scenario]), 8 endpoints computed by the
      additive mixture rule, 13 non-additive endpoints unchanged);
    - a separate detailed_overview file (or files, split/subfoldered for large projects)
      with each CAS's own RAW colour - same shape as option A/C's own detailed_overview,
      NOT the hom-mat mixture-rule result broadcast down.
    """
    file_name = os.path.basename(mas_path)
    df = core.pd.read_excel(mas_path)

    log("Reading MAS file and detecting tier depth...")
    max_tier = core.get_highest_tier(df, core.col_CAS)
    log(f"Max tier found: {max_tier}")

    df = core.clean_data(df, max_tier)
    df = core.add_helper_columns(df, max_tier)
    df = core.add_final_map(df, max_tier)

    cas_count, _ = core.count_CAS_unique(df, "CAS")
    log(f"Unique CAS found: {cas_count}")

    log("Generating scenarios...")
    df = core.identify_alternative_groups(df, max_tier)
    scenarios = core.generate_scenarios(df, max_tier)
    scenario_ids = [s["scenario_id"] for s in scenarios]
    log(f"Scenarios generated: {len(scenarios)}")

    log("Calculating mixture rules (acute toxicity, irritation, sensitization, aquatic "
        "toxicity, no-mixture-rules endpoints) - toxicity data comes from the database only, this can "
        "take a while for large projects...")
    _, _, c2c_extremes_df, all_c2c_scenario_results_df, active_scaffold_df = core.analyse_the_dataset_with_mixture_rules(
        df, scenarios, db_path
    )

    time_str = _timestamp()
    file_stem = os.path.splitext(file_name)[0]

    log("Building the detailed per-CAS dataset (this can take a while for large projects)...")
    all_scenarios_df = core.build_selected_scenarios_df(df, scenarios, scenario_ids)
    detailed_overview_df = core.build_c2c_assessment_df(all_scenarios_df, db_path, include_mixture_rule_db_details=True)
    cas_list_all = (
        core.clean_cas_values(detailed_overview_df["CAS"].tolist())
        if "CAS" in detailed_overview_df.columns else []
    )
    _, missing_cas_df = core.extract_colour_assessment_C2C(cas_list_all, db_path)

    log("Saving detailed_overview (per-CAS, raw colours)...")
    detailed_paths = core.save_c2c_detailed_overview_output(
        detailed_overview_df, saving_dir, file_name, time_str
    )

    log("Saving mixture-rule summary (overview/percentage_assessed/risk_assessed)...")
    readable_scaffold_df = core.rename_mixture_rules_endpoints_to_readable(active_scaffold_df)
    saving_mixture_rules_summary = os.path.join(saving_dir, f"C2C_assessment_{file_stem}_{time_str}.xlsx")
    core.save_c2c_assessment_workbook_static(
        readable_scaffold_df, missing_cas_df, saving_mixture_rules_summary, write_detailed=False
    )

    log("Saved mixture-rules summary and detailed_overview files.")

    return [saving_mixture_rules_summary] + detailed_paths


class MixtureRulesApp:
    MODES = {
        "percent": {
            "label": "Percent Assessed",
            "needs_db": False,
            "description": "Just the % of each material assessed per product, across all scenarios. No database, no hazard ratings.",
        },
        "quick": {
            "label": "Quick Assessment",
            "needs_db": True,
            "description": "Initial assessment: C2C hazard colours per chemical (from the database).",
        },
        "mixture": {
            "label": "Mixture Rules Assessment",
            "needs_db": True,
            "description": "Full mixture-rule hazard assessment.",
        },
    }

    def __init__(self, root):
        """Build the app window: mode-selection buttons, styling, remembered-path restoration, the (initially empty) inputs frame, the Run button, and the log text box."""
        self.root = root
        root.title("C2C Screener")
        root.configure(bg=APP_BG)
        root.geometry("640x520")

        # macOS's native ("aqua") ttk/tk theme ignores custom bg/fg on buttons, which is
        # what made every button render white-on-white. "clam" honors our colors on all
        # platforms.
        style = ttk.Style(root)
        style.theme_use("clam")
        style.configure(
            "Mode.TButton", font=("Helvetica", 12, "bold"),
            background=ACCENT, foreground="white", borderwidth=0, padding=10,
        )
        style.map("Mode.TButton", background=[("active", "#127a37")])
        style.configure(
            "Run.TButton", font=("Helvetica", 12, "bold"),
            background=GOOD, foreground="white", borderwidth=0, padding=8,
        )
        style.map("Run.TButton", background=[("active", "#127a37"), ("disabled", "#9ad6b0")])

        self._config = load_config()

        self.mas_path = tk.StringVar()
        self.saving_dir = tk.StringVar()
        self.db_path = tk.StringVar()
        self.selected_mode = None

        # Remember the last-used MAS file, save folder, and database file across runs of the
        # app, but only restore each if it still actually exists (e.g. an external drive
        # that's no longer plugged in, or a file since moved/deleted).
        self.mas_path.set(self._remembered("last_mas_file", is_dir=False))
        self.saving_dir.set(self._remembered("last_folder", is_dir=True))
        self.db_path.set(self._remembered("last_db_file", is_dir=False))

        title = tk.Label(root, text="C2C Screener", font=("Helvetica", 18, "bold"), bg=APP_BG, fg=TEXT)
        title.pack(pady=(18, 4))
        subtitle = tk.Label(root, text="Choose which assessment to run.", font=("Helvetica", 11), bg=APP_BG, fg=TEXT)
        subtitle.pack(pady=(0, 16))

        button_frame = tk.Frame(root, bg=APP_BG)
        button_frame.pack(pady=4)
        for key, cfg in self.MODES.items():
            btn = ttk.Button(
                button_frame, text=cfg["label"], style="Mode.TButton",
                width=32, command=lambda k=key: self.select_mode(k),
            )
            btn.pack(pady=6)

        self.desc_label = tk.Label(root, text="", font=("Helvetica", 10), bg=APP_BG, fg=TEXT, wraplength=560, justify="left")
        self.desc_label.pack(pady=(10, 10))

        self.inputs_frame = tk.Frame(root, bg=APP_BG)
        self.inputs_frame.pack(fill="x", padx=20)

        self.run_button = ttk.Button(
            root, text="Run", style="Run.TButton",
            width=16, command=self.run_selected, state="disabled",
        )
        self.run_button.pack(pady=10)

        self.log_box = tk.Text(root, height=12, bg="white", fg=TEXT, font=("Courier", 9), state="disabled")
        self.log_box.pack(fill="both", expand=True, padx=20, pady=(0, 16))

    def log(self, message):
        """Append `message` to the log text box on the Tk main thread (safe to call from the background worker thread)."""
        def _write():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", message + "\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.root.after(0, _write)

    def _remembered(self, key, is_dir):
        """Read back a remembered path from config, but only if it still actually exists."""
        path = self._config.get(key, "")
        check = os.path.isdir if is_dir else os.path.isfile
        return path if path and check(path) else ""

    def _remember(self, key, path):
        """Store `path` under `key` in the in-memory config and persist it to disk."""
        self._config[key] = path
        save_config(self._config)

    def select_mode(self, key):
        """Switch the UI to the chosen assessment mode: show its description, rebuild the input rows (adding the database-file row only if that mode needs one), and enable the Run button."""
        self.selected_mode = key
        cfg = self.MODES[key]
        self.desc_label.configure(text=cfg["description"])

        for widget in self.inputs_frame.winfo_children():
            widget.destroy()

        self._add_path_row(self.inputs_frame, "MAS Excel file:", self.mas_path, self.browse_mas_file)
        self._add_path_row(self.inputs_frame, "Save folder:", self.saving_dir, self.browse_folder)
        if cfg["needs_db"]:
            self._add_path_row(self.inputs_frame, "Database file:", self.db_path, self.browse_db_file)

        self.run_button.configure(state="normal")

    def _add_path_row(self, parent, label_text, var, browse_command):
        """Add a labeled path-entry row (label, text entry bound to `var`, and a "Browse..." button) to `parent`."""
        row = tk.Frame(parent, bg=APP_BG)
        row.pack(fill="x", pady=3)
        tk.Label(row, text=label_text, width=14, anchor="w", bg=APP_BG, fg=TEXT).pack(side="left")
        tk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True, padx=6)
        tk.Button(row, text="Browse...", command=browse_command).pack(side="left")

    def browse_mas_file(self):
        """Prompt for the MAS Excel file, store the chosen path, remember it, and default the save folder to its containing directory if none is set yet."""
        path = filedialog.askopenfilename(title="Select the MAS Excel file", filetypes=[("Excel files", "*.xlsx *.xls")])
        if path:
            self.mas_path.set(path)
            self._remember("last_mas_file", path)
            if not self.saving_dir.get():
                self.saving_dir.set(os.path.dirname(path))
                self._remember("last_folder", os.path.dirname(path))

    def browse_folder(self):
        """Prompt for the output save folder and remember the chosen path."""
        path = filedialog.askdirectory(title="Select the folder to save output files in")
        if path:
            self.saving_dir.set(path)
            self._remember("last_folder", path)

    def browse_db_file(self):
        """Prompt for the SQLite database file and remember the chosen path."""
        path = filedialog.askopenfilename(title="Select the database file", filetypes=[("Database files", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")])
        if path:
            self.db_path.set(path)
            self._remember("last_db_file", path)

    def run_selected(self):
        """Validate the required inputs for the selected mode, clear the log, and kick off that mode's pipeline in a background thread."""
        mode = self.selected_mode
        cfg = self.MODES[mode]

        if not self.mas_path.get() or not os.path.exists(self.mas_path.get()):
            messagebox.showerror("Missing input", "Please select a valid MAS Excel file.")
            return
        if not self.saving_dir.get() or not os.path.isdir(self.saving_dir.get()):
            messagebox.showerror("Missing input", "Please select a valid save folder.")
            return
        if cfg["needs_db"] and (not self.db_path.get() or not os.path.exists(self.db_path.get())):
            messagebox.showerror("Missing input", "Please select a valid database file.")
            return

        self.run_button.configure(state="disabled")
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        self.log(f"Running: {cfg['label']}...")

        thread = threading.Thread(target=self._run_in_background, args=(mode,), daemon=True)
        thread.start()

    def _run_in_background(self, mode):
        """Worker-thread entry point: run the selected mode's pipeline function, logging errors and re-enabling the Run button on failure, or logging the output paths and showing a completion dialog on success."""
        try:
            if mode == "percent":
                outputs = run_percent_assessed(self.mas_path.get(), self.saving_dir.get(), self.log)
            elif mode == "quick":
                outputs = run_quick_assessment(self.mas_path.get(), self.saving_dir.get(), self.db_path.get(), self.log)
            else:
                outputs = run_mixture_rules(self.mas_path.get(), self.saving_dir.get(), self.db_path.get(), self.log)
        except Exception as e:
            self.log(f"ERROR: {e}")
            self.log(traceback.format_exc())
            self.root.after(0, lambda: messagebox.showerror("Failed", f"{e}"))
            self.root.after(0, lambda: self.run_button.configure(state="normal"))
            return

        self.log("Done!")
        for path in outputs:
            self.log(f"  - {path}")
        self.root.after(0, lambda: messagebox.showinfo("Done", "Calculations finished. Files saved to your chosen folder."))
        self.root.after(0, lambda: self.run_button.configure(state="normal"))


def main():
    """Launch the tkinter app: create the root window, build the MixtureRulesApp UI, and start the main loop."""
    root = tk.Tk()
    MixtureRulesApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
