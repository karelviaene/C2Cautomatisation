### C2C Database Communication - desktop app
### A small tkinter window wrapping DB_communication_core.py's pipeline (itself a
### faithful, non-Streamlit port of streamlit_DB_communication_CURRENT.py): pick the
### CAS excel and the SQLite database inline (no instructional pop-ups, just Browse
### buttons and a path field), then either "Run CAS Screening" (backup, ECHA CnL
### lookup, DB sync, CPS excel generation/ingestion, report export) or "Export DB to
### Excel" - watch a bouncing magnifying glass while it works, get a "Done!" screen
### with confetti once finished.
### Reuses DB_communication_core.py's logic by import (that file is import-only, no
### __main__ guard that runs anything) rather than duplicating it - keep the two in
### sync if the pipeline itself changes. DB_communication_core.py is a mechanical
### port of streamlit_DB_communication_CURRENT.py and intentionally keeps that
### file's known quirks/bugs unfixed (see KNOWN ISSUES below) - the source file is
### never modified by this app.
###
### KNOWN ISSUES ported over from the original Streamlit script (flagged, not fixed):
### - `if CnL_json is None: _log(...success...)` in run_cas_screening looks inverted
###   (a "success" message fires when the CnL lookup returned nothing).
### - Some DB helper functions can raise UnboundLocalError out of their own `finally`
###   blocks if the initial sqlite3.connect() itself fails, masking the real error.
### - Generated CPS excels are saved with a hardcoded "Test " filename prefix.
### - Several nested Excel-extraction helpers swallow SQLite errors via print() only.
### - No pre-flight check that the CAS excel actually contains rows before running.

import os
import sys
import io
import re
import json
import math
import random
import threading
import traceback
from datetime import datetime

import tkinter as tk
from tkinter import filedialog, ttk

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import DB_communication_core as core

APP_BG = "#f4f6f8"
ACCENT = "#2563eb"
GOOD = "#16a34a"
BAD = "#dc2626"
CANVAS_W, CANVAS_H = 520, 140

# remembers the last-used paths across runs of the app
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".db_communication_app_config.json")

# the ported core module still emits Streamlit's ":color[text]" markdown-ish markers
# (e.g. ":red[CAS older than 3 years: ...]") since that text lived inside the log
# messages themselves, not in st.* call structure - strip it back to plain text (and
# use it to color the line) instead of showing the raw markup to the user.
COLOR_MARKER_RE = re.compile(r"^:(\w+)\[(.*)\]$", re.DOTALL)


def load_config():
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_config(cfg):
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(cfg, f)
    except Exception:
        pass  # not critical - just means the next run won't remember the paths


class PathRow(ttk.Frame):
    """One labeled path field + Browse button - no pop-ups, just an inline picker."""

    def __init__(self, parent, label, mode, filetypes=None, initial="", on_change=None):
        super().__init__(parent)
        self.mode = mode
        self.filetypes = filetypes or [("All files", "*.*")]
        self.on_change = on_change
        self.path_var = tk.StringVar(value=initial)

        ttk.Label(self, text=label, width=16, anchor="w").pack(side="left")
        entry = ttk.Entry(self, textvariable=self.path_var, state="readonly")
        entry.pack(side="left", fill="x", expand=True, padx=(4, 8))
        ttk.Button(self, text="Browse...", command=self._browse).pack(side="left")

    def _browse(self):
        # start the picker in whatever is already selected, so re-browsing is quick
        start_dir = self.path_var.get() or None
        if self.mode == "open_file":
            path = filedialog.askopenfilename(
                title="Select a file", filetypes=self.filetypes,
                initialdir=os.path.dirname(start_dir) if start_dir else None,
            )
        elif self.mode == "folder":
            path = filedialog.askdirectory(title="Select a folder", initialdir=start_dir)
        else:
            raise ValueError(self.mode)
        if path:
            self.path_var.set(path)
            if self.on_change:
                self.on_change(path)

    def get(self):
        return self.path_var.get()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("C2C Database Communication")
        self.configure(bg=APP_BG)
        self.resizable(False, False)

        self._anim_job = None
        self._anim_t = 0.0
        self._confetti_particles = []
        self._confetti_job = None
        self._running = False
        self._config = load_config()

        outer = ttk.Frame(self, padding=16)
        outer.pack(fill="both", expand=True)

        ttk.Label(outer, text="C2C Database Communication", font=("Helvetica", 16, "bold")).pack(anchor="w")
        ttk.Label(
            outer,
            text="Screens CAS against ECHA CnL and the C2C database, or exports the DB to Excel.",
            foreground="#555",
        ).pack(anchor="w", pady=(0, 12))

        self.cas_row = PathRow(
            outer, "CAS excel:", "open_file", [("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")],
            initial=self._remembered("last_cas_file", is_dir=False),
            on_change=lambda p: self._remember("last_cas_file", p),
        )
        self.cas_row.pack(fill="x", pady=4)
        self.db_row = PathRow(
            outer, "SQL database:", "open_file", [("Database files", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")],
            initial=self._remembered("last_db_file", is_dir=False),
            on_change=lambda p: self._remember("last_db_file", p),
        )
        self.db_row.pack(fill="x", pady=4)

        btn_row = ttk.Frame(outer)
        btn_row.pack(fill="x", pady=(12, 8))
        self.run_button = ttk.Button(btn_row, text="Run CAS Screening", command=self._on_run_screening)
        self.run_button.pack(side="left")
        self.export_button = ttk.Button(btn_row, text="Export DB to Excel", command=self._on_export_db)
        self.export_button.pack(side="left", padx=(8, 0))
        self.status_label = ttk.Label(btn_row, text="Idle", foreground="#555")
        self.status_label.pack(side="left", padx=12)

        self.canvas = tk.Canvas(outer, width=CANVAS_W, height=CANVAS_H, bg="white", highlightthickness=1, highlightbackground="#ddd")
        self.canvas.pack(pady=(4, 8))
        self._draw_idle()

        log_frame = ttk.Frame(outer)
        log_frame.pack(fill="both", expand=True)
        self.log_text = tk.Text(log_frame, width=100, height=22, state="disabled", wrap="word")
        self.log_text.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self.log_text.tag_configure("blue", foreground="#1d4ed8")
        self.log_text.tag_configure("green", foreground=GOOD)
        self.log_text.tag_configure("red", foreground=BAD)

        # route the core module's log messages to this window (module-level, so
        # only one pipeline should run at a time - enforced below via self._running)
        core.log_callback = self._log_threadsafe

    # ---------------------------------------------------------------- log
    def _log(self, message):
        tag = None
        match = COLOR_MARKER_RE.match(message)
        if match:
            tag, message = match.group(1), match.group(2)
            if tag not in ("blue", "green", "red"):
                tag = None
        self.log_text.configure(state="normal")
        if tag:
            self.log_text.insert("end", message + "\n", tag)
        else:
            self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _log_threadsafe(self, message):
        self.after(0, self._log, message)

    # ------------------------------------------------------------ canvas
    def _draw_idle(self):
        self.canvas.delete("all")
        self.canvas.create_text(CANVAS_W // 2, CANVAS_H // 2, text="Ready when you are.", fill="#999", font=("Helvetica", 12))

    def _start_spinner(self):
        self._anim_t = 0.0
        self._animate_magnifier()

    def _animate_magnifier(self):
        self.canvas.delete("all")
        base_y = CANVAS_H * 0.62
        amplitude = 22
        bounce = abs(math.sin(self._anim_t)) * amplitude
        x = CANVAS_W * 0.5 + math.sin(self._anim_t * 0.6) * 60
        y = base_y - bounce
        # shadow that shrinks/grows opposite the bounce, for a little depth
        shadow_scale = 1.0 - (bounce / amplitude) * 0.5
        self.canvas.create_oval(
            x - 18 * shadow_scale, base_y + 20, x + 18 * shadow_scale, base_y + 26,
            fill="#e5e7eb", outline="",
        )
        self.canvas.create_text(x, y, text="\U0001F50D", font=("Helvetica", 40))
        self.canvas.create_text(
            CANVAS_W // 2, CANVAS_H - 14, text="Working on it...", fill="#666", font=("Helvetica", 11)
        )
        self._anim_t += 0.28
        self._anim_job = self.after(40, self._animate_magnifier)

    def _stop_spinner(self):
        if self._anim_job is not None:
            self.after_cancel(self._anim_job)
            self._anim_job = None

    def _start_confetti(self):
        colors = ["#f87171", "#fbbf24", "#34d399", "#60a5fa", "#a78bfa", "#f472b6"]
        self._confetti_particles = [
            {
                "x": random.uniform(0, CANVAS_W),
                "y": random.uniform(-CANVAS_H, 0),
                "vy": random.uniform(2.5, 5.5),
                "vx": random.uniform(-1.5, 1.5),
                "size": random.uniform(4, 8),
                "color": random.choice(colors),
                "life": random.uniform(50, 90),
            }
            for _ in range(70)
        ]
        self._animate_confetti()

    def _animate_confetti(self):
        self.canvas.delete("all")
        self.canvas.create_text(
            CANVAS_W // 2, 24, text="Done!", fill=GOOD, font=("Helvetica", 22, "bold")
        )
        alive = []
        for p in self._confetti_particles:
            p["x"] += p["vx"]
            p["y"] += p["vy"]
            p["life"] -= 1
            if p["life"] > 0 and p["y"] < CANVAS_H:
                self.canvas.create_rectangle(
                    p["x"], p["y"], p["x"] + p["size"], p["y"] + p["size"] * 0.6,
                    fill=p["color"], outline="",
                )
                alive.append(p)
        self._confetti_particles = alive
        if alive:
            self._confetti_job = self.after(30, self._animate_confetti)
        else:
            self._confetti_job = None

    def _draw_error(self, message):
        self.canvas.delete("all")
        self.canvas.create_text(
            CANVAS_W // 2, CANVAS_H // 2 - 10, text="Something went wrong", fill=BAD, font=("Helvetica", 14, "bold")
        )
        self.canvas.create_text(
            CANVAS_W // 2, CANVAS_H // 2 + 16, text=message[:80], fill="#555", font=("Helvetica", 10)
        )

    def _remembered(self, key, is_dir):
        """Read back a remembered path from config, but only if it still actually exists."""
        path = self._config.get(key, "")
        check = os.path.isdir if is_dir else os.path.isfile
        return path if path and check(path) else ""

    def _remember(self, key, path):
        self._config[key] = path
        save_config(self._config)

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def _begin_run(self):
        self._running = True
        self.run_button.configure(state="disabled")
        self.export_button.configure(state="disabled")
        self.status_label.configure(text="Running...", foreground=ACCENT)
        self._clear_log()
        self._start_spinner()

    def _end_run_ok(self, status_text):
        self._running = False
        self._stop_spinner()
        self.run_button.configure(state="normal")
        self.export_button.configure(state="normal")
        self.status_label.configure(text=status_text, foreground=GOOD)
        self._start_confetti()

    def _end_run_failed(self, error_message, tb):
        self._running = False
        self._stop_spinner()
        self.run_button.configure(state="normal")
        self.export_button.configure(state="normal")
        self.status_label.configure(text="Failed - see log below.", foreground=BAD)
        self._log(f"\n[ERROR] {error_message}\n{tb}")
        self._draw_error(error_message)

    # ------------------------------------------------------ run screening
    def _on_run_screening(self):
        if self._running:
            return
        cas_path = self.cas_row.get()
        db_path = self.db_row.get()
        if not cas_path or not db_path:
            self.status_label.configure(text="Please select the CAS excel and the database first.", foreground=BAD)
            return

        self._begin_run()
        thread = threading.Thread(target=self._run_screening_worker, args=(cas_path, db_path), daemon=True)
        thread.start()

    def _run_screening_worker(self, cas_path, db_path):
        try:
            db_path = core.validate_db_path(db_path)
            result = core.run_cas_screening(cas_path, db_path)
            self.after(0, self._on_screening_success, result)
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, self._end_run_failed, str(e), tb)

    def _on_screening_success(self, result):
        if result is None:
            self._end_run_failed("The database path is not a valid file.", "")
            return
        n_found = len(result.get("found", []))
        n_not_found = len(result.get("not_found", []))
        self._log(f"\nFinished. {n_found} CAS found in DB, {n_not_found} not found. Report: {result.get('out_file')}")
        self._end_run_ok(f"Done - {n_found} found, {n_not_found} not found.")

    # ---------------------------------------------------------- export DB
    def _on_export_db(self):
        if self._running:
            return
        db_path = self.db_row.get()
        if not db_path:
            self.status_label.configure(text="Please select the database first.", foreground=BAD)
            return

        self._begin_run()
        thread = threading.Thread(target=self._export_db_worker, args=(db_path,), daemon=True)
        thread.start()

    def _export_db_worker(self, db_path):
        try:
            db_path = core.validate_db_path(db_path)
            core.export_db_to_excel(db_path)
            self.after(0, self._on_export_success)
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, self._end_run_failed, str(e), tb)

    def _on_export_success(self):
        self._log("\nFinished exporting the database to Excel.")
        self._end_run_ok("Done - database exported.")


if __name__ == "__main__":
    App().mainloop()
