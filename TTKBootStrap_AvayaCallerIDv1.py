# Import necessary libraries
import tkinter as tk
from tkinter import ttk
import ttkbootstrap as tb  
import os, datetime, re, pytz 
import pyperclip as clipboard 
from urllib.parse import urlparse
from io import StringIO

# Define global variables for file and directory paths
appdata = os.path.expandvars(r"%APPDATA%")
last_phonebook_file = os.path.join(appdata, r"NMC_AIO\phonebook.csv")
last_site_search_file = os.path.join(appdata, r"NMC_AIO\sitesearch.csv")
log_file = os.path.join(appdata, r"Avaya\one-X Agent\2.5\Log Files\OneXAgent.log")
default_phonebook_file = r"G:\Documents\Alarm Check Contacts List (21).txt"
phonebook_file = default_phonebook_file  # Default phonebook file path, subject to change

# Function to fetch the most recent caller information from a log file
def update_current_caller(log_file, phonebook_file):
    try:
        with open(log_file, 'r', encoding='cp1252') as log_file:
            log_lines = log_file.readlines()
    except FileNotFoundError:
        return "Log file not found", "Please check the log file path."
    
    phone_number = None
    # Iterate over log lines in reverse to find the latest call
    for line in reversed(log_lines):
        match = re.search(r'RemoteParty=\[(\d{3}-\d{3}-\d{4}),\d{10}\]', line)
        if match:
            phone_number = match.group(1)
            break
    
    # Match the found phone number against entries in the phonebook file
    if phone_number:
        caller_match = match_caller_to_phonebook(phone_number, phonebook_file)
    else:
        caller_match = "No phone number found"
    return phone_number, caller_match

# Matches a given phone number with entries in a specified phonebook file
def match_caller_to_phonebook(phone_number, phonebook_file):
    try:
        with open(phonebook_file, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
    except FileNotFoundError:
        return "Phonebook file not found."

    matches = [line.strip() for line in lines if phone_number in line]
    return "\n".join(matches) if matches else "No match found for {phone_number}"

# Reads and returns phonebook entries from a specified file
def read_phonebook(phonebook_file):
    try:
        with open(phonebook_file, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
    except FileNotFoundError:
        return ["Phonebook file not found."]
    return [line.strip().replace('\t', ' ') for line in lines]

# Searches for a given query within a list of phonebook lines
def search_phonebook(lines, search_query):
    return [line for line in lines if search_query.lower() in line.lower()]


def setup_timezone_tab(notebook):
    timezone_frame = ttk.Frame(notebook)
    notebook.add(timezone_frame, text='Timezone')

    tz_names = ['UTC', 'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific', 'Europe/Berlin']
    tz_labels = {}

    # Current time in UTC
    utc_now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

    for i, tz_name in enumerate(tz_names, start=1):
        tz = pytz.timezone(tz_name)
        tz_time = utc_now.astimezone(tz)
        formatted_time = tz_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')

        label_text = f"{tz_name}: {formatted_time}"
        tz_labels[tz_name] = ttk.Label(timezone_frame, text=label_text)
        tz_labels[tz_name].grid(row=i, column=0, sticky='w', padx=5, pady=2)

# Main function to setup and run the GUI application
def main():
    # Create the main application window with a specific theme
    app = tb.Window(themename="litera")
    app.title("Avaya Caller ID")
    app.geometry("600x180")  # Define the initial size of the window
    app.minsize(600, 130)  # Set the minimum size of the window

    # Setup a notebook (tabbed interface) within the application window
    notebook = ttk.Notebook(app)
    notebook.pack(expand=True, fill='both')

    # Define frames (tabs) for different sections of the application
    current_call_frame = ttk.Frame(notebook)
    phonebook_frame = ttk.Frame(notebook)
    timezone_frame = ttk.Frame(notebook)
    settings_frame = ttk.Frame(notebook)

    # Add the frames as tabs to the notebook
    notebook.add(current_call_frame, text='Current Call')
    notebook.add(phonebook_frame, text='Phonebook')
    notebook.add(settings_frame, text='Settings')  # Placeholder for future functionality
    setup_timezone_tab(notebook)  # Properly set up the Timezone tab

    # Define StringVar objects for dynamic data display
    caller_phone_var = tk.StringVar(value="Loading...")
    caller_match_var = tk.StringVar(value="Searching...")

    # Setup the caller information display frame
    caller_info_frame = ttk.Frame(current_call_frame)
    caller_info_frame.pack(padx=10, pady=10, fill='both', expand=True)
    caller_info_frame.grid_columnconfigure(1, weight=1)  # Allow the phone number label to expand

    # Label and display for the caller's number
    ttk.Label(caller_info_frame, text="Caller's Number:", font=('TkDefaultFont', 12)).grid(row=0, column=0, sticky='w')
    phone_label = ttk.Label(caller_info_frame, textvariable=caller_phone_var, font=('TkDefaultFont', 12, 'bold'))
    phone_label.grid(row=0, column=1, sticky='w')

    # Button for copying caller information; functionality to be defined later
    copy_button = ttk.Button(caller_info_frame, text="Copy")
    copy_button.grid(row=2, column=0, columnspan=2, sticky='w', pady=(5, 0))

    # Frame for displaying match information; can hold a Listbox or a Label depending on the number of matches
    match_display_frame = ttk.Frame(caller_info_frame)
    match_display_frame.grid(row=1, column=0, columnspan=2, sticky='ew', pady=(5, 0))
    match_display_frame.grid_columnconfigure(0, weight=1)  # Make this frame expandable

    # Function to update the match display based on the current matches
    def update_match_display(matches):
        # Clear any existing widgets from the match display frame
        for widget in match_display_frame.winfo_children():
            widget.destroy()

        # If multiple matches are found, use a Listbox to display them
        if len(matches) > 1:
            listbox = tk.Listbox(match_display_frame, listvariable=tk.StringVar(value=matches), height=4, exportselection=False)
            listbox.grid(row=0, column=0, sticky='ew')
            scrollbar = ttk.Scrollbar(match_display_frame, orient='vertical', command=listbox.yview)
            scrollbar.grid(row=0, column=1, sticky='ns')
            listbox.config(yscrollcommand=scrollbar.set)
            # Update the copy button to copy the selected match from the Listbox
            copy_button.config(command=lambda: clipboard.copy(listbox.get(listbox.curselection()[0])) if listbox.curselection() else None)
        else:
            # If a single match is found or no matches, use a Label to display the match information
            label = ttk.Label(match_display_frame, text=matches[0] if matches else "No match found", font=('TkDefaultFont', 11))
            label.grid(row=0, column=0, sticky='ew')  # Ensure the label expands with the frame
            # Update the copy button to copy the text from the Label
            copy_button.config(command=lambda: clipboard.copy(label.cget("text")))

    # Periodically update caller information and refresh the match display
    def update_caller_info():
        phone, match = update_current_caller(log_file, phonebook_file)
        caller_phone_var.set(phone)
        matches = match.split('\n')
        update_match_display(matches)
        app.after(1000, update_caller_info)  # Schedule this function to run again after 1 second

    # Call update_caller_info initially to populate the display with current data
    update_caller_info()

    # Start the GUI event loop
    app.mainloop()

# Entry point of the script
if __name__ == '__main__':
    main()
