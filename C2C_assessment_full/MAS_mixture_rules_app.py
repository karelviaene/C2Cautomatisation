### Mixture Rules Assessment - desktop app
### A small tkinter window with 3 buttons - one per pipeline in
### MAS_automation_with_mixture_rules_current.py - that runs the picked pipeline to
### completion with no interactive y/n prompts (unlike that module's own CLI menu, which
### asks whether to additionally save "all scenarios"/"selected scenarios" - this app
### always saves the core outputs and skips those optional extras, matching "don't prompt
### the user for options, just make buttons").
### Reuses MAS_automation_with_mixture_rules_current.py's logic by import (that module now
### has a __main__ guard, so importing it does not auto-run its own CLI menu) rather than
### duplicating it - keep the two in sync if the pipeline itself changes.

import os
import sys
import threading
import traceback
from datetime import datetime

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import MAS_automation_with_mixture_rules_current as core

APP_BG = "#ffffff"
TEXT = "#000000"
ACCENT = "#16a34a"
GOOD = "#16a34a"
BAD = "#000000"


def _timestamp():
    return datetime.now().strftime("%Y%m%d")


def run_percent_assessed(mas_path, saving_dir, log):
    """Percent assessed only - no DB, no hazard endpoints."""
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
    log(f"Scenarios generated: {len(scenarios)}")

    log("Calculating percentage assessed (this can take a while for large projects)...")
    summary_df, pct_dict = core.analyse_the_dataset(df, scenarios)

    time_str = _timestamp()
    saving_summary = os.path.join(saving_dir, f"summary_{time_str}_{file_name}.xlsx")
    saving_percent_assessed = os.path.join(saving_dir, f"percent_assessed_{time_str}_{file_name}.xlsx")
    saving_CAS = os.path.join(saving_dir, f"CAS_{time_str}_{file_name}.xlsx")

    summary_df.to_excel(saving_summary, index=False)
    core.save_percent_assessed(pct_dict, saving_percent_assessed)
    core.save_unique_values(df, "CAS", saving_CAS)
    log("Saved summary, percentage assessed and unique-CAS files.")

    return [saving_summary, saving_percent_assessed, saving_CAS]


def run_quick_assessment(mas_path, saving_dir, db_path, log):
    """C2C assessment only, no mixture rules (option C) - all scenarios, no selection."""
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

    if len(all_scenarios_df) > core.C2C_ASSESSMENT_TEMPLATE_MAX_ROWS:
        raise RuntimeError(
            f"This project generates {len(all_scenarios_df)} rows, more than the "
            f"{core.C2C_ASSESSMENT_TEMPLATE_MAX_ROWS}-row cap for Quick Assessment. "
            "Use Percent Assessed or Mixture Rules Assessment instead for a project this size."
        )

    log("Pulling C2C colour assessment hazards from the database...")
    c2c_df = core.build_c2c_assessment_df(all_scenarios_df, db_path)

    time_str = _timestamp()
    saving_c2c = os.path.join(saving_dir, f"C2C_assessment_all_scenarios_{time_str}_{file_name}.xlsx")
    core.save_c2c_assessment_workbook(c2c_df, saving_c2c)
    log("Saved C2C assessment (all scenarios) file.")

    return [saving_c2c]


def run_mixture_rules(mas_path, saving_dir, db_path, log):
    """Full % assessed + mixture rules (including Assessment C) assessment (option B)."""
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
    log(f"Scenarios generated: {len(scenarios)}")

    log("Calculating mixture rules (acute toxicity, irritation, sensitization, aquatic "
        "toxicity, Assessment C) - toxicity data comes from the database only, this can "
        "take a while for large projects...")
    summary_df, pct_dict, c2c_extremes_df, all_c2c_scenario_results_df = core.analyse_the_dataset_with_mixture_rules(
        df, scenarios, db_path
    )

    time_str = _timestamp()
    saving_summary = os.path.join(saving_dir, f"summary_{time_str}_{file_name}.xlsx")
    saving_percent_assessed = os.path.join(saving_dir, f"percent_assessed_{time_str}_{file_name}.xlsx")
    saving_CAS = os.path.join(saving_dir, f"CAS_{time_str}_{file_name}.xlsx")
    saving_mixture_rules = os.path.join(saving_dir, f"mixture_rules_{time_str}_{file_name}.xlsx")

    summary_df.to_excel(saving_summary, index=False)
    core.save_percent_assessed(pct_dict, saving_percent_assessed)
    core.save_unique_values(df, "CAS", saving_CAS)
    core.save_mixture_rules_assessment_output(c2c_extremes_df, all_c2c_scenario_results_df, saving_mixture_rules)
    log("Saved summary, percentage assessed, unique-CAS and mixture rules assessment files.")

    return [saving_summary, saving_percent_assessed, saving_CAS, saving_mixture_rules]


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

        self.mas_path = tk.StringVar()
        self.saving_dir = tk.StringVar()
        self.db_path = tk.StringVar()
        self.selected_mode = None

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
        def _write():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", message + "\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.root.after(0, _write)

    def select_mode(self, key):
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
        row = tk.Frame(parent, bg=APP_BG)
        row.pack(fill="x", pady=3)
        tk.Label(row, text=label_text, width=14, anchor="w", bg=APP_BG, fg=TEXT).pack(side="left")
        tk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True, padx=6)
        tk.Button(row, text="Browse...", command=browse_command).pack(side="left")

    def browse_mas_file(self):
        path = filedialog.askopenfilename(title="Select the MAS Excel file", filetypes=[("Excel files", "*.xlsx *.xls")])
        if path:
            self.mas_path.set(path)
            if not self.saving_dir.get():
                self.saving_dir.set(os.path.dirname(path))

    def browse_folder(self):
        path = filedialog.askdirectory(title="Select the folder to save output files in")
        if path:
            self.saving_dir.set(path)

    def browse_db_file(self):
        path = filedialog.askopenfilename(title="Select the database file", filetypes=[("Database files", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")])
        if path:
            self.db_path.set(path)

    def run_selected(self):
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
    root = tk.Tk()
    MixtureRulesApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
