import tkinter as tk
from tkinter import scrolledtext, messagebox
import subprocess
import threading

class NetworkToolGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Network Tool")
        self.geometry("800x600")
        
        self.create_widgets()
        
    def create_widgets(self):
        # Input Section
        self.input_label = tk.Label(self, text="Paste the list of SiteName, TicketNumber, and IPAddress (or just IPAddress):")
        self.input_label.pack(pady=5)
        
        self.input_text = scrolledtext.ScrolledText(self, height=10, width=95)
        self.input_text.pack(pady=5)
        
        self.run_button = tk.Button(self, text="Run Commands", command=self.run_commands)
        self.run_button.pack(pady=5)
        
        # Output Section with scrollbar
        self.output_canvas = tk.Canvas(self)
        self.output_frame = tk.Frame(self.output_canvas)
        self.vsb = tk.Scrollbar(self, orient="vertical", command=self.output_canvas.yview)
        self.output_canvas.configure(yscrollcommand=self.vsb.set)

        self.vsb.pack(side="right", fill="y")
        self.output_canvas.pack(side="left", fill="both", expand=True)
        self.output_canvas.create_window((4,4), window=self.output_frame, anchor="nw")

        self.output_frame.bind("<Configure>", self.on_frame_configure)
        
    def on_frame_configure(self, event):
        self.output_canvas.configure(scrollregion=self.output_canvas.bbox("all"))
        
    def run_commands(self):
        raw_input = self.input_text.get("1.0", tk.END).strip()
        if not raw_input:
            messagebox.showwarning("Input Error", "Please enter some input.")
            return
        
        targets = self.parse_input(raw_input)
        
        for widget in self.output_frame.winfo_children():
            widget.destroy()
        
        for site, ticket, ip in targets:
            output_box = self.create_output_box(site, ticket, ip)
            threading.Thread(target=self.execute_command, args=(site, ticket, ip, output_box)).start()
            
    def parse_input(self, raw_input):
        targets = []
        lines = raw_input.splitlines()
        for line in lines:
            parts = line.split()
            if len(parts) == 1:
                site = ""
                ticket = ""
                ip = parts[0]
            elif len(parts) == 2:
                site = ""
                ticket = parts[0]
                ip = parts[1]
            else:
                site = parts[0]
                ticket = parts[1]
                ip = parts[2]
            targets.append((site, ticket, ip))
        return targets
    
    def create_output_box(self, site, ticket, ip):
        frame = tk.Frame(self.output_frame, relief=tk.SUNKEN, borderwidth=1)
        frame.pack(pady=5, padx=5, fill=tk.BOTH, expand=True)
        
        title = f"Site: {site}, Ticket: {ticket}, IP: {ip}"
        label = tk.Label(frame, text=title, font=('Arial', 12, 'bold'))
        label.pack(pady=5)
        
        output_text = scrolledtext.ScrolledText(frame, height=10, width=95)
        output_text.pack(pady=5)
        
        copy_button = tk.Button(frame, text="Copy Output", command=lambda: self.copy_to_clipboard(output_text.get("1.0", tk.END)))
        copy_button.pack(pady=5)
        
        return output_text
    
    def execute_command(self, site, ticket, ip, output_text):
        title = f"{site} - {ip}"
        output = []
        output.append(f"Site: {site} Ticket: {ticket} IP: {ip}")
        output.append(f"Tracert Output for {site} ({ticket}, {ip}):")
        tracert_result = subprocess.getoutput(f"tracert -w 300 {ip}")
        output.append(tracert_result)
        output.append(f"Ping Output for {site} ({ticket}, {ip}):")
        ping_result = subprocess.getoutput(f"ping {ip}")
        output.append(ping_result)
        output.append(f"Site: {site} Ticket: {ticket} IP: {ip}")
        output.append("Press any key to close...\n")
        
        self.update_output(output_text, "\n".join(output))
        
    def update_output(self, output_text, output):
        output_text.insert(tk.END, output + "\n")
        output_text.see(tk.END)
    
    def copy_to_clipboard(self, text):
        self.clipboard_clear()
        self.clipboard_append(text)
        messagebox.showinfo("Copied", "Output copied to clipboard")

if __name__ == "__main__":
    app = NetworkToolGUI()
    app.mainloop()
