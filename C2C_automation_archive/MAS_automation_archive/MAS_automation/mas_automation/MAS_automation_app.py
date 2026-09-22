import os
import sys
import queue
import signal
import threading
import subprocess
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

colour = "#d8f3dc"

class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Program Runner")
        self.root.geometry("900x600")
        self.root.configure(bg=colour)
        # Styling
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background=colour)
        style.configure("TLabel", background=colour)

        # Put the program you want to run here.
        self.command = [sys.executable, "MAS_automation_v2.py"]

        self.process: subprocess.Popen | None = None
        self.output_queue: queue.Queue[str] = queue.Queue()

        self.status_var = tk.StringVar(value="Ready")

        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self.poll_output)

    def _build_ui(self) -> None:
        top = ttk.Frame(self.root, padding=12)
        top.pack(fill="x")

        title = ttk.Label(
            top,
            text="Run the program for calculations of MAS",
            font=("Helvetica", 24, "bold"),
        )
        title.pack(anchor="w")

        subtitle = ttk.Label(
            top,
            text="Click Run to start the program. Terminal output will appear below. Follow the prompts of the program.",
        )
        subtitle.pack(anchor="w", pady=(4, 10))

        controls = ttk.Frame(top)
        controls.pack(fill="x")

        self.run_button = ttk.Button(controls, text="Run", command=self.start_program)
        self.run_button.pack(side="left")

        self.stop_button = ttk.Button(controls, text="Stop", command=self.stop_program)
        self.stop_button.pack(side="left", padx=(8, 0))
        self.stop_button.state(["disabled"])

        self.clear_button = ttk.Button(controls, text="Clear the console", command=self.clear_output)
        self.clear_button.pack(side="left", padx=(8, 0))

        status = ttk.Label(controls, textvariable=self.status_var)
        status.pack(side="right")

        middle = ttk.Frame(self.root, padding=(12, 0, 12, 0))
        middle.pack(fill="both", expand=True)

        self.output = ScrolledText(middle, wrap="word", font=("Menlo", 12))
        self.output.pack(fill="both", expand=True)
        self.output.insert("end", "Program output will appear here...\n")
        self.output.configure(state="disabled")

        bottom = ttk.Frame(self.root, padding=12)
        bottom.pack(fill="x")

        input_label = ttk.Label(bottom, text="Send input to program:")
        input_label.pack(anchor="w")

        input_row = ttk.Frame(bottom)
        input_row.pack(fill="x", pady=(6, 0))

        self.input_entry = ttk.Entry(input_row)
        self.input_entry.pack(side="left", fill="x", expand=True)
        self.input_entry.bind("<Return>", self.send_input)

        self.send_button = ttk.Button(input_row, text="Send", command=self.send_input)
        self.send_button.pack(side="left", padx=(8, 0))
        self.send_button.state(["disabled"])

    def append_output(self, text: str) -> None:
        self.output.configure(state="normal")
        self.output.insert("end", text)
        self.output.see("end")
        self.output.configure(state="disabled")

    def clear_output(self) -> None:
        self.output.configure(state="normal")
        self.output.delete("1.0", "end")
        self.output.configure(state="disabled")

    def start_program(self) -> None:
        if self.process is not None and self.process.poll() is None:
            return

        self.append_output(f"\n$ {' '.join(self.command)}\n\n")
        self.status_var.set("Running")
        self.run_button.state(["disabled"])
        self.stop_button.state(["!disabled"])
        self.send_button.state(["!disabled"])

        try:
            self.process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
        except Exception as e:
            self.append_output(f"Failed to start program: {e}\n")
            self.status_var.set("Error")
            self.run_button.state(["!disabled"])
            self.stop_button.state(["disabled"])
            self.send_button.state(["disabled"])
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
        self.status_var.set("Finished")
        self.run_button.state(["!disabled"])
        self.stop_button.state(["disabled"])
        self.send_button.state(["disabled"])
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
            self.status_var.set("Stopping")
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
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
