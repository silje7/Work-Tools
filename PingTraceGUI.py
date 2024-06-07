import tkinter as tk
from tkinter import scrolledtext, messagebox
import subprocess
import os
import threading

class NetworkToolGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Network Tool")
        self.geometry("600x400")
        
        self.create_widgets()
        
    def create_widgets(self):
        # Input Section
        self.input_label = tk.Label(self, text="Paste the list of SiteName, TicketNumber, and IPAddress (or just IPAddress):")
        self.input_label.pack(pady=5)
        
        self.input_text = scrolledtext.ScrolledText(self, height=10, width=70)
        self.input_text.pack(pady=5)
        
        self.run_button = tk.Button(self, text="Run Commands", command=self.run_commands)
        self.run_button.pack(pady=5)
        
        # Output Section
        self.output_label = tk.Label(self, text="Output:")
        self.output_label.pack(pady=5)
        
        self.output_text = scrolledtext.ScrolledText(self, height=10, width=70)
        self.output_text.pack(pady=5)
        
    def run_commands(self):
        raw_input = self.input_text.get("1.0", tk.END).strip()
        if not raw_input:
            messagebox.showwarning("Input Error", "Please enter some input.")
            return
        
        targets = self.parse_input(raw_input)
        
        self.output_text.delete("1.0", tk.END)
        self.output_text.insert(tk.END, "Running tracert and ping in parallel for multiple IP addresses...\n")
        
        for site, ticket, ip in targets:
            threading.Thread(target=self.execute_command, args=(site, ticket, ip)).start()
            
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
    
    def execute_command(self, site, ticket, ip):
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
        
        self.update_output("\n".join(output))
        
    def update_output(self, output):
        self.output_text.insert(tk.END, output + "\n")
        self.output_text.see(tk.END)

if __name__ == "__main__":
    app = NetworkToolGUI()
    app.mainloop()
