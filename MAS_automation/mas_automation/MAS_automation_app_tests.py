import os
import sys
import queue
import signal
import threading
import subprocess
import customtkinter as ctk

ctk.set_appearance_mode("light")

# ── Pastel green palette ───────────────────────────────────────────
BG        = "#e8f5e9"   # pastel green background
PANEL     = "#c8e6c9"   # slightly deeper panel
CARD      = "#f1f8f1"   # near-white card
ACCENT    = "#2e7d32"   # deep green accent
ACCENT_H  = "#388e3c"   # hover
TEXT      = "#1b5e20"   # dark green text
TEXT_DIM  = "#4caf50"   # muted green
TERM_BG   = "#f0faf0"   # terminal area
TERM_FG   = "#1b5e20"   # terminal text
BTN_STOP  = "#c62828"   # red stop
BTN_STOP_H= "#b71c1c"


class App:
    def __init__(self, root: ctk.CTk) -> None:
        self.root = root
        self.root.title("Program Runner")
        self.root.geometry("900x620")
        self.root.configure(fg_color=BG)

        self.command = [sys.executable, "MAS_automation.py"]

        self.process: subprocess.Popen | None = None
        self.output_queue: queue.Queue[str] = queue.Queue()
        self.status_var = ctk.StringVar(value="Ready")

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self.poll_output)

    def _build_ui(self) -> None:
        # ── TOP: title + subtitle ─────────────────────────────────
        top = ctk.CTkFrame(self.root, fg_color="transparent")
        top.pack(fill="x", padx=20, pady=(18, 0))

        ctk.CTkLabel(
            top, text="Program Runner",
            font=("Helvetica", 22, "bold"), text_color=ACCENT
        ).pack(anchor="w")

        ctk.CTkLabel(
            top, text="Click Run to start the program. Terminal output will appear below.",
            font=("Helvetica", 12), text_color=TEXT_DIM
        ).pack(anchor="w", pady=(2, 12))

        # ── CONTROLS ──────────────────────────────────────────────
        controls = ctk.CTkFrame(top, fg_color="transparent")
        controls.pack(fill="x")

        self.run_button = ctk.CTkButton(
            controls, text="▶  Run", width=100, height=36,
            font=("Helvetica", 13, "bold"),
            fg_color=ACCENT, hover_color=ACCENT_H,
            text_color="white", corner_radius=8,
            command=self.start_program
        )
        self.run_button.pack(side="left")

        self.stop_button = ctk.CTkButton(
            controls, text="⏹  Stop", width=100, height=36,
            font=("Helvetica", 13, "bold"),
            fg_color=BTN_STOP, hover_color=BTN_STOP_H,
            text_color="white", corner_radius=8,
            state="disabled", command=self.stop_program
        )
        self.stop_button.pack(side="left", padx=(10, 0))

        self.clear_button = ctk.CTkButton(
            controls, text="🗑  Clear", width=100, height=36,
            font=("Helvetica", 13),
            fg_color="transparent", hover_color=PANEL,
            text_color=ACCENT, border_width=1,
            border_color=ACCENT, corner_radius=8,
            command=self.clear_output
        )
        self.clear_button.pack(side="left", padx=(10, 0))

        # Status pill
        self.status_label = ctk.CTkLabel(
            controls, textvariable=self.status_var,
            font=("Courier", 12), text_color=TEXT_DIM
        )
        self.status_label.pack(side="right")

        # ── SEPARATOR ─────────────────────────────────────────────
        ctk.CTkFrame(self.root, fg_color=PANEL, height=2).pack(fill="x", padx=20, pady=12)

        # ── OUTPUT TERMINAL ───────────────────────────────────────
        middle = ctk.CTkFrame(self.root, fg_color=CARD, corner_radius=12)
        middle.pack(fill="both", expand=True, padx=20)

        ctk.CTkLabel(
            middle, text="OUTPUT",
            font=("Courier", 10, "bold"), text_color=TEXT_DIM
        ).pack(anchor="w", padx=14, pady=(10, 4))

        self.output = ctk.CTkTextbox(
            middle, font=("Menlo", 12),
            fg_color=TERM_BG, text_color=TERM_FG,
            corner_radius=8,
            scrollbar_button_color=PANEL,
            scrollbar_button_hover_color=ACCENT,
            wrap="word"
        )
        self.output.pack(fill="both", expand=True, padx=14, pady=(0, 14))
        self.output.insert("end", "Program output will appear here...\n")
        self.output.configure(state="disabled")

        # ── INPUT BAR ─────────────────────────────────────────────
        bottom = ctk.CTkFrame(self.root, fg_color=PANEL, corner_radius=0, height=70)
        bottom.pack(fill="x", side="bottom")
        bottom.pack_propagate(False)

        ctk.CTkLabel(
            bottom, text="Send input to program:",
            font=("Helvetica", 11), text_color=TEXT
        ).pack(anchor="w", padx=20, pady=(8, 0))

        input_row = ctk.CTkFrame(bottom, fg_color="transparent")
        input_row.pack(fill="x", padx=20, pady=(4, 0))

        self.input_entry = ctk.CTkEntry(
            input_row, font=("Menlo", 12),
            fg_color=CARD, text_color=TEXT,
            border_color=ACCENT, border_width=1,
            corner_radius=8, placeholder_text="Type input and press Enter..."
        )
        self.input_entry.pack(side="left", fill="x", expand=True)
        self.input_entry.bind("<Return>", self.send_input)

        self.send_button = ctk.CTkButton(
            input_row, text="Send", width=80, height=32,
            font=("Helvetica", 12, "bold"),
            fg_color=ACCENT, hover_color=ACCENT_H,
            text_color="white", corner_radius=8,
            state="disabled", command=self.send_input
        )
        self.send_button.pack(side="left", padx=(10, 0))

    # ── Output helpers ────────────────────────────────────────────
    def append_output(self, text: str) -> None:
        self.output.configure(state="normal")
        self.output.insert("end", text)
        self.output.see("end")
        self.output.configure(state="disabled")

    def clear_output(self) -> None:
        self.output.configure(state="normal")
        self.output.delete("1.0", "end")
        self.output.configure(state="disabled")

    # ── Process control ───────────────────────────────────────────
    def start_program(self) -> None:
        if self.process is not None and self.process.poll() is None:
            return

        self.append_output(f"\n$ {' '.join(self.command)}\n\n")
        self.status_var.set("● Running")
        self.status_label.configure(text_color="#2e7d32")
        self.run_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.send_button.configure(state="normal")

        try:
            self.process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                text=True, bufsize=1,
                universal_newlines=True,
            )
        except Exception as e:
            self.append_output(f"Failed to start program: {e}\n")
            self.status_var.set("Error")
            self.status_label.configure(text_color="#c62828")
            self.run_button.configure(state="normal")
            self.stop_button.configure(state="disabled")
            self.send_button.configure(state="disabled")
            self.process = None
            return

        threading.Thread(target=self.read_output, daemon=True).start()
        threading.Thread(target=self.wait_for_exit, daemon=True).start()

    def read_output(self) -> None:
        if self.process is None or self.process.stdout is None:
            return
        for line in self.process.stdout:
            self.output_queue.put(line)

    def wait_for_exit(self) -> None:
        if self.process is None:
            return
        code = self.process.wait()
        self.output_queue.put(f"\n[Program finished with exit code {code}]\n")
        self.root.after(0, self.on_program_finished)

    def on_program_finished(self) -> None:
        self.status_var.set("✔ Finished")
        self.status_label.configure(text_color=ACCENT)
        self.run_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.send_button.configure(state="disabled")
        self.process = None

    def poll_output(self) -> None:
        try:
            while True:
                chunk = self.output_queue.get_nowait()
                self.append_output(chunk)
        except queue.Empty:
            pass
        self.root.after(100, self.poll_output)

    def send_input(self, event=None) -> None:
        text = self.input_entry.get()
        if not text.strip():
            return
        if self.process is None or self.process.stdin is None:
            self.append_output("[No running program to send input to]\n")
            return
        try:
            self.process.stdin.write(text + "\n")
            self.process.stdin.flush()
            self.append_output(f"> {text}\n")
            self.input_entry.delete(0, "end")
        except Exception as e:
            self.append_output(f"[Failed to send input: {e}]\n")

    def stop_program(self) -> None:
        if self.process is None:
            return
        try:
            if os.name == "nt":
                self.process.terminate()
            else:
                self.process.send_signal(signal.SIGTERM)
            self.append_output("\n[Stop requested]\n")
            self.status_var.set("■ Stopping")
            self.status_label.configure(text_color="#c62828")
        except Exception as e:
            self.append_output(f"\n[Failed to stop program: {e}]\n")

    def on_close(self) -> None:
        if self.process is not None and self.process.poll() is None:
            try:
                self.process.terminate()
            except Exception:
                pass
        self.root.destroy()


def main() -> None:
    root = ctk.CTk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()