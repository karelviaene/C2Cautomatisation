### MAS Merge - desktop app

import os
import sys
import io
import json
import math
import random
import threading
import traceback
import contextlib
from datetime import datetime

import tkinter as tk
from tkinter import filedialog, ttk

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import MAS_generation as core

APP_BG = "#f4f6f8"
ACCENT = "#2563eb"
GOOD = "#16a34a"
BAD = "#dc2626"
CANVAS_W, CANVAS_H = 520, 140

# remembers the last-used paths and settings across runs of the app
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".mas_merge_app_config.json")


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
        pass  # not critical - just means the next run won't remember the settings


class LogStream(io.TextIOBase):
    """Forwards stdout (the plain print() progress lines inside MAS_generation.py's
    merge functions) to the app's log widget, line by line."""

    def __init__(self, log):
        self.log = log
        self._buf = ""

    def write(self, s):
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line:
                self.log(line)
        return len(s)

    def flush(self):
        pass


def run_pipeline(mas_path, output_path, max_tier, choice, log):
    """Runs the actual merge, writing progress lines to `log` (a callable)."""
    log(f"Merging on {'materials' if choice == 'a' else 'materials + suppliers'} up to Tier {max_tier}...")
    with contextlib.redirect_stdout(LogStream(log)):
        if choice == "a":
            final_df = core.join_tier_sheets(mas_path, max_tier)
        else:
            final_df = core.join_tier_sheets_with_suppliers(mas_path, max_tier)
    log(f"Merged dataset: {len(final_df)} rows.")

    log("Saving the merged excel file...")
    core.join_to_excel(final_df, output_path)
    log(f"Saved: {output_path}")
    return output_path


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
        self.title("MAS Merge")
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

        ttk.Label(outer, text="MAS Merge", font=("Helvetica", 16, "bold")).pack(anchor="w")
        ttk.Label(
            outer, text="Merge instead of Powerquery - joins the tier sheets of an MAS excel.",
            foreground="#666",
        ).pack(anchor="w", pady=(0, 12))

        self.mas_row = PathRow(
            outer, "MAS excel:", "open_file", [("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")],
            initial=self._remembered("last_mas_file", is_dir=False),
            on_change=lambda p: self._remember("last_mas_file", p),
        )
        self.mas_row.pack(fill="x", pady=4)
        self.folder_row = PathRow(
            outer, "Save folder:", "folder",
            initial=self._remembered("last_folder", is_dir=True),
            on_change=lambda p: self._remember("last_folder", p),
        )
        self.folder_row.pack(fill="x", pady=4)

        filename_row = ttk.Frame(outer)
        filename_row.pack(fill="x", pady=4)
        ttk.Label(filename_row, text="File name:", width=16, anchor="w").pack(side="left")
        date_str = datetime.now().strftime("%Y-%m-%d")
        self.filename_var = tk.StringVar(value=f"MAS_merged_{date_str}.xlsx")
        ttk.Entry(filename_row, textvariable=self.filename_var).pack(side="left", fill="x", expand=True, padx=(4, 0))

        tier_row = ttk.Frame(outer)
        tier_row.pack(fill="x", pady=4)
        ttk.Label(tier_row, text="Final tier:", width=16, anchor="w").pack(side="left")
        self.tier_var = tk.StringVar(value=self._config.get("last_max_tier", "5"))
        ttk.Spinbox(tier_row, from_=1, to=50, width=6, textvariable=self.tier_var).pack(side="left", padx=(4, 0))

        merge_row = ttk.Frame(outer)
        merge_row.pack(fill="x", pady=(8, 4))
        ttk.Label(merge_row, text="Merge on:", width=16, anchor="w").pack(side="left", anchor="n")
        options = ttk.Frame(merge_row)
        options.pack(side="left", fill="x", expand=True)
        self.choice_var = tk.StringVar(value=self._config.get("last_choice", "a"))
        ttk.Radiobutton(
            options, text="Materials only (e.g. Tier 1 Material)",
            variable=self.choice_var, value="a",
        ).pack(anchor="w")
        ttk.Radiobutton(
            options, text="Materials + suppliers (exact match required in both sheets)",
            variable=self.choice_var, value="b",
        ).pack(anchor="w")

        btn_row = ttk.Frame(outer)
        btn_row.pack(fill="x", pady=(12, 8))
        self.run_button = ttk.Button(btn_row, text="Run Merge", command=self._on_run)
        self.run_button.pack(side="left")
        self.status_label = ttk.Label(btn_row, text="Idle", foreground="#555")
        self.status_label.pack(side="left", padx=12)

        self.canvas = tk.Canvas(outer, width=CANVAS_W, height=CANVAS_H, bg="white", highlightthickness=1, highlightbackground="#ddd")
        self.canvas.pack(pady=(4, 8))
        self._draw_idle()

        log_frame = ttk.Frame(outer)
        log_frame.pack(fill="both", expand=True)
        self.log_text = tk.Text(log_frame, width=78, height=10, state="disabled", wrap="word")
        self.log_text.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scrollbar.set)

    # ---------------------------------------------------------------- log
    def _log(self, message):
        self.log_text.configure(state="normal")
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

    # -------------------------------------------------------------- run
    def _on_run(self):
        if self._running:
            return
        mas_path = self.mas_row.get()
        saving_dir = self.folder_row.get()
        filename = self.filename_var.get().strip()
        tier_value = self.tier_var.get().strip()
        choice = self.choice_var.get()

        if not mas_path or not saving_dir or not filename:
            self.status_label.configure(text="Please select the MAS excel, save folder, and file name first.", foreground=BAD)
            return
        try:
            max_tier = int(tier_value)
            if max_tier <= 0:
                raise ValueError
        except ValueError:
            self.status_label.configure(text="Final tier must be a positive whole number.", foreground=BAD)
            return
        if not filename.lower().endswith(".xlsx"):
            filename += ".xlsx"
        output_path = os.path.join(saving_dir, filename)

        self._remember("last_max_tier", str(max_tier))
        self._remember("last_choice", choice)

        self._running = True
        self.run_button.configure(state="disabled")
        self.status_label.configure(text="Running...", foreground=ACCENT)
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")
        self._start_spinner()

        thread = threading.Thread(target=self._worker, args=(mas_path, output_path, max_tier, choice), daemon=True)
        thread.start()

    def _worker(self, mas_path, output_path, max_tier, choice):
        try:
            saved_path = run_pipeline(mas_path, output_path, max_tier, choice, self._log_threadsafe)
            self.after(0, self._on_success, saved_path)
        except Exception as e:
            tb = traceback.format_exc()
            self.after(0, self._on_failure, str(e), tb)

    def _on_success(self, saved_path):
        self._running = False
        self._stop_spinner()
        self.run_button.configure(state="normal")
        self.status_label.configure(text="Done - file saved.", foreground=GOOD)
        self._log(f"\nFinished. Saved to {saved_path}")
        self._start_confetti()

    def _on_failure(self, error_message, tb):
        self._running = False
        self._stop_spinner()
        self.run_button.configure(state="normal")
        self.status_label.configure(text="Failed - see log below.", foreground=BAD)
        self._log(f"\n[ERROR] {error_message}\n{tb}")
        self._draw_error(error_message)


if __name__ == "__main__":
    App().mainloop()
