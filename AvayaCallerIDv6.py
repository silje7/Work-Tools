import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import ttkbootstrap as tb
import os, datetime, re, pytz 
import pyperclip as clipboard 
from urllib.parse import urlparse
from io import StringIO
from xml.dom import minidom
import xml.etree.ElementTree as ET


# Define global variables for file and directory paths
appdata = os.path.expandvars(r"%APPDATA%")
last_phonebook_file = os.path.join(appdata, r"NMC_AIO\phonebook.csv")
last_selected_index = None
last_site_search_file = os.path.join(appdata, r"NMC_AIO\sitesearch.csv")
log_file = os.path.join(appdata, r"Avaya\one-X Agent\2.5\Log Files\OneXAgent.log")
default_phonebook_file = r"G:\Documents\Alarm Check Contacts List (21).txt"
phonebook_file = default_phonebook_file  # Default phonebook file path, subject to change

def setup_settings_tab(notebook, update_phonebook_callback, app):
    settings_frame = ttk.Frame(notebook)
    notebook.add(settings_frame, text='Settings')

    # Checkbox for bringing the application to the front
    bring_to_front_var = tk.BooleanVar(value=True)
    bring_to_front_checkbox = ttk.Checkbutton(settings_frame, text="Bring to front on log change", variable=bring_to_front_var)
    bring_to_front_checkbox.grid(row=2, column=0, sticky='w', padx=5, pady=5)

    # Theme selection ComboBox
    theme_var = tk.StringVar()
    theme_label = ttk.Label(settings_frame, text="Select Theme:")
    theme_label.grid(row=0, column=0, sticky='w', padx=5, pady=5)
    theme_selector = ttk.Combobox(settings_frame, textvariable=theme_var, state="readonly", values=tb.Style().theme_names())
    theme_selector.grid(row=0, column=1, sticky='ew', padx=5)
    theme_selector.bind('<<ComboboxSelected>>', lambda event: on_theme_change(event, app, theme_var))

    # Button for adding contacts to the Avaya XML Contacts list
    add_contacts_button = ttk.Button(settings_frame, text="Add NMC Contacts (Run ONCE)", command=lambda: add_contacts_to_avaya_xml(app))
    add_contacts_button.grid(row=3, column=0, sticky='w', padx=5, pady=(5, 0))

    # Note advising to use the button only once
    note_label = ttk.Label(settings_frame, text="Note: Please run this only once to avoid duplicates.", font=('TkDefaultFont', 8))
    note_label.grid(row=4, column=0, sticky='w', padx=5, pady=(0, 5))

# Callback function to update the phonebook file
def update_phonebook_callback(filepath):
    global phonebook_file
    phonebook_file = filepath

def setup_current_call_tab(notebook):
    current_call_frame = ttk.Frame(notebook)
    notebook.add(current_call_frame, text="Current Call")

    caller_phone_var = tk.StringVar(value="Loading...")

    caller_info_frame = ttk.Frame(current_call_frame)
    caller_info_frame.pack(padx=10, pady=10, fill="both", expand=True)
    caller_info_frame.grid_columnconfigure(1, weight=1)

    ttk.Label(caller_info_frame, text="Caller's Number:", font=('TkDefaultFont', 12)).grid(row=0, column=0, sticky='w')
    phone_label = ttk.Label(caller_info_frame, textvariable=caller_phone_var, font=('TkDefaultFont', 12, 'bold'))
    phone_label.grid(row=0, column=1, sticky='w')

    match_display_frame = ttk.Frame(caller_info_frame)
    match_display_frame.grid(row=1, column=0, columnspan=2, sticky='ew')
    match_display_frame.grid_columnconfigure(0, weight=1)

    listbox = tk.Listbox(match_display_frame, height=4, exportselection=False)
    scrollbar = ttk.Scrollbar(match_display_frame, orient="vertical", command=listbox.yview)
    listbox.config(yscrollcommand=scrollbar.set)
    listbox.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
    scrollbar.grid(row=0, column=1, sticky='ns')

    def handle_copy_action():
        if listbox.curselection():
            clipboard.copy(listbox.get(listbox.curselection()[0]))

    copy_button = ttk.Button(match_display_frame, text="Copy", command=handle_copy_action)
    copy_button.grid(row=1, column=0, columnspan=2, sticky="ew")

    def update_caller_info():
        phone, match = update_current_caller(log_file, phonebook_file)
        caller_phone_var.set(phone)
        if not match:
            matches = ["No match found"]
        else:
            matches = match.split('\n')
        
        listbox.delete(0, tk.END)
        for match in matches:
            listbox.insert(tk.END, match)
        
        # If there are multiple matches, delay the next update to give users more time to interact.
        if len(matches) > 1:
            refresh_delay = 6000  # Delay for 6 seconds if more than one match is found
        else:
            refresh_delay = 1000  # Default refresh rate of 1 second
        
        # Use the refresh_delay for scheduling the next update
        current_call_frame.after(refresh_delay, update_caller_info)




    update_caller_info()

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
        caller_match = phone_number
    return phone_number, caller_match

# Matches a given phone number with entries in a specified phonebook file
def match_caller_to_phonebook(phone_number, phonebook_file):
    try:
        with open(phonebook_file, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()
    except FileNotFoundError:
        return "Phonebook file not found."

    matches = [line.strip() for line in lines if phone_number in line]
    return "\n".join(matches) if matches else f"No match found for {phone_number}"

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

    # Introduce padding at the top (optional: adjust the padding as needed)
    #ttk.Label(timezone_frame, text="").grid(row=0, column=0, pady=(0,0))  # Adjust (10,0) for more or less padding

    # Setup GMT to EST conversion (now starts from row 1 instead of 0)
    gmt_input_label = ttk.Label(timezone_frame, text="Enter GMT Time (HH:MM):", font=('TkDefaultFont', 10, 'bold'))
    gmt_input_label.grid(row=1, column=0, sticky='w', padx=5)
    gmt_input_var = tk.StringVar()
    gmt_input = ttk.Entry(timezone_frame, textvariable=gmt_input_var, font=('TkDefaultFont', 11, 'bold'), width=10)
    gmt_input.grid(row=1, column=1, sticky='w', padx=5)

    est_time_label = ttk.Label(timezone_frame, text="EST", font=('TkDefaultFont', 10, 'bold'))
    est_time_label.grid(row=1, column=2, sticky='w', padx=(70, 0))  # Reduced horizontal padding before EST label
    est_converted_label = ttk.Label(timezone_frame, text="", font=('TkDefaultFont', 10))
    est_converted_label.grid(row=1, column=2, sticky='w', padx=(5, 0))  # Reduced horizontal padding after EST label

    # Function to handle conversion and update the EST label
    def convert_to_est(*args):
        gmt_time_str = gmt_input_var.get()
        try:
            gmt_time = datetime.datetime.strptime(gmt_time_str, "%H:%M")
            gmt_time = gmt_time.replace(tzinfo=pytz.timezone("UTC"))
            est_time = gmt_time.astimezone(pytz.timezone("US/Eastern"))
            est_converted_label.config(text=est_time.strftime("%H:%M %p"))
        except ValueError:
            est_converted_label.config(text="Invalid")

    # Trace the variable linked to the input box to update in real time
    gmt_input_var.trace_add("write", convert_to_est)

    # Define time zones
    tz_names = {
        'UTC': 'UTC',
        'New York (EST)': 'America/New_York',
        'Chicago (CST)': 'America/Chicago',
        'Denver (MST)': 'America/Denver',
        'Los Angeles (PST)': 'America/Los_Angeles',
        'Berlin (CET)': 'Europe/Berlin',
    }

    start_row_for_timezones = 2  # Start printing time zones from this row
    column_offset = 0  # Adjust based on where you want the columns to start

    # Print time zones in columns
    for i, (name, tz) in enumerate(tz_names.items()):
        row = start_row_for_timezones + (i % (len(tz_names) // 2))
        column = column_offset + 2 * (i // (len(tz_names) // 2))
        label_text = f"{name}: Loading..."
        tz_label = ttk.Label(timezone_frame, text=label_text, font=('TkDefaultFont', 10))
        tz_label.grid(row=row, column=column, sticky='w', padx=5, pady=2)

    def update_times():
        now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        for i, (name, tz) in enumerate(tz_names.items()):
            row = start_row_for_timezones + (i % (len(tz_names) // 2))
            column = column_offset + 2 * (i // (len(tz_names) // 2))
            local_time = now.astimezone(pytz.timezone(tz))
            tz_label = timezone_frame.grid_slaves(row=row, column=column)[0]
            tz_label.config(text=f"{name}: {local_time.strftime('%I:%M %p')}")

        timezone_frame.after(1000, update_times)

    update_times()

def setup_phonebook_tab(notebook):
    phonebook_frame = ttk.Frame(notebook)
    notebook.add(phonebook_frame, text='Phonebook')

    placeholder_text = "Start typing..."
    search_var = tk.StringVar(value=placeholder_text)
    
    search_box = ttk.Entry(phonebook_frame, textvariable=search_var, font=('TkDefaultFont', 11))
    search_box.pack(padx=10, pady=(10, 0), fill='x')

    def on_focus_in(event):
        if search_var.get() == placeholder_text:
            search_box.delete(0, tk.END)
            search_box.config(foreground='grey')

    def on_focus_out(event):
        if not search_var.get():
            search_box.insert(0, placeholder_text)
            search_box.config(foreground='grey')

    search_box.bind("<FocusIn>", on_focus_in)
    search_box.bind("<FocusOut>", on_focus_out)
    search_box.config(foreground='grey')

    def clear_placeholder(event=None):
        """Clear placeholder text when the user starts typing and reset text color."""
        if search_var.get() == placeholder_text:
            search_box.delete(0, tk.END)
            search_box.config(foreground='black')
            search_box.unbind('<KeyRelease>', clear_placeholder_id)

    clear_placeholder_id = search_box.bind("<KeyRelease>", clear_placeholder)

    def execute_search(*args):
        """Execute search based on the entry's content, excluding placeholder text."""
        query = search_var.get().strip().lower()
        if query and query != placeholder_text:
            results = search_phonebook(read_phonebook(phonebook_file), query)
        else:
            results = []
        update_search_results(results)

    search_box.bind("<KeyRelease>", execute_search, add="+")  # Ensure this doesn't replace the clear_placeholder binding

    results_var = tk.StringVar(value=[])
    search_results_listbox = tk.Listbox(phonebook_frame, listvariable=results_var, height=3, exportselection=False)
    search_results_listbox.pack(padx=10, pady=5, fill='both', expand=True)

    def copy_selected_result():
        if search_results_listbox.curselection():
            selected_text = search_results_listbox.get(search_results_listbox.curselection()[0])
            clipboard.copy(selected_text)

    copy_result_button = ttk.Button(phonebook_frame, text="Copy", command=copy_selected_result)
    copy_result_button.pack(pady=(0, 10), padx=10)

    def update_search_results(results):
        search_results_listbox.delete(0, tk.END)  # Clear existing results
        for result in results:
            search_results_listbox.insert(tk.END, result)

def on_theme_change(event, app, theme_var):
    selected_theme = theme_var.get()
    app.style = tb.Style(theme=selected_theme)


def add_contacts_to_avaya_xml(app):
    xml_file_path = os.path.join(appdata, "Avaya", "one-X Agent", "2.5", "Profiles", "OMZ - Open", "contacts.xml")
    ns = "http://avaya.com/OneXAgent/ObjectModel/Contacts"
    ET.register_namespace('', ns)

    # Load or initialize XML
    if os.path.exists(xml_file_path):
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    else:
        root = ET.Element(f"{{{ns}}}ContactGroup", {
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            "ReadOnly": "false",
            "Type": "User",
            "Version": "2.5.60624.801"
        })
        tree = ET.ElementTree(root)

    contacts = [
        ("CaryNMC-Power", "800-251-6517", "NMC-Support"),
        ("CaryNMC-Transport", "800-873-7866", "NMC-Support"),
        ("CaryNMC-Voice", "800-229-7427", "NMC-Support"),
        ("CaryNMC-IP", "800-281-8396", "NMC-Support"),
        ("Call Out of Country", "9011+CC+#", "NMC-Support"),
        ("NTNOC", "888-684-6656", "NMC-Support"),
        ("CSC (opt.3-1)", "888-696-3973", "NMC-Dispatch"),
        ("OCC (opt.1)", "877-628-6672", "NMC-Dispatch"),
        ("New England NDC (opt.6-2)", "855-468-6280", "NMC-Dispatch"),
        ("Potomac NDC (opt.1-3)", "866-618-9822", "NMC-Dispatch"),
        ("PA & DE NDC (opt.1-3)", "855-632-7551", "NMC-Dispatch"),
        ("New Jersey NDC (opt.1-3)", "855-632-7552", "NMC-Dispatch"),
        ("New York NDC (opt.1-3)", "855-632-7553", "NMC-Dispatch"),
        ("New York NDC (NYC) (opt.1-3-1)", "855-632-7554", "NMC-Dispatch"),
        ("Directory Assistance", "XXX-555-1212", "Utility"),
        ("GENMC", "800-444-0902", "NMC-Support"),
        ("NSNOC - PA", "888-479-8340", "NMC-Support"),
        ("Adesta (G4S)", "888-637-2344", "NMC-External"),
        ("Telemetry (TAM)", "800-487-0350", "NMC-Support"),
        ("Level 3 (CenturyLink)", "303-260-4942", "NMC-External"),
        ("Equinix", "866-378-4649", "NMC-External"),
        ("Rapid Response", "800-932-3822", "NMC-External"),
        ("NMS (GSPS)", "866-535-1481", "NMC-Support"),
        ("Sprint (opt.3-1-1)", "866-400-6040", "NMC-External"),
        ("AT&T", "866-400-6649", "NMC-External"),
        ("Amtrak", "800-832-3116", "NMC-External"),
        ("Zayo (opt.1-1)", "866-236-2824", "NMC-External"),
        ("vRepair Helpdesk", "877-389-0900", "NMC-Support"),
        ("FCC (NOTAMs)", "877-487-6867", "NMC-External"),
        ("Verizon Wireless (VzW) NOC", "800-852-2671", "NMC-External"),
    ]

    contacts_group = root.find(f".//{{{ns}}}Group[@Name='My Contacts']")
    if not contacts_group:
        contacts_group = ET.SubElement(root, f"{{{ns}}}Group", Name='My Contacts', ReadOnly="false", Type="User")
    contacts_container = contacts_group.find(f"{{{ns}}}Contacts")
    if not contacts_container:
        contacts_container = ET.SubElement(contacts_group, f"{{{ns}}}Contacts", ReadOnly="false")

    # Add new contacts
    for company, phone, business in contacts:
        contact_id = str(hash(f"{company}{phone}"))
        contact_element = ET.SubElement(contacts_container, f"{{{ns}}}Contact", {
            "Id": contact_id,
            "FirstName": business,
            "LastName": company,
            "Work": phone,
            "Email": "",
            "Favorite": "false",
            "SpeedDial": "false",
            "ReadOnly": "false"
        })

    # Generate pretty-printed XML string
    xmlstr = minidom.parseString(ET.tostring(root, 'utf-8')).toprettyxml(indent="   ")

    with open(xml_file_path, "w", encoding="utf-8") as f:
        f.write(xmlstr)

    messagebox.showinfo("Success", "Contacts have been successfully added.")



# Main function to setup and run the GUI application
def main():
    # Create the main application window with a specific theme
    app = tb.Window(themename="solar")
    app.title("Avaya Caller ID v6")
    app.geometry("600x180")  # Define the initial size of the window
    app.minsize(600, 130)  # Set the minimum size of the window
    # app.iconbitmap('path_to_your_icon.ico')

    # Setup a notebook (tabbed interface) within the application window
    notebook = ttk.Notebook(app)
    notebook.pack(expand=True, fill='both')

    # Define frames (tabs) for different sections of the application
    current_call_frame = ttk.Frame(notebook)
    phonebook_frame = ttk.Frame(notebook)
    timezone_frame = ttk.Frame(notebook)
    settings_frame = ttk.Frame(notebook)

    # Tabs
    notebook.add(current_call_frame, text='Current Call')
    setup_timezone_tab(notebook)  # Timezone tab
    setup_phonebook_tab(notebook)
    setup_settings_tab(notebook, lambda filepath: None, app)
    #setup_settings_tab(notebook, update_phonebook_callback,app)

    
    ##PhoneBook Functions
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

    # Frame for displaying match information;
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
 
    def update_selection_index(listbox):
        global last_selected_index
        selection = listbox.curselection()
        last_selected_index = selection[0] if selection else None

    # Periodically update caller information and refresh the match display
    def update_caller_info():
        phone, match = update_current_caller(log_file, phonebook_file)
        caller_phone_var.set(phone)
        matches = match.split('\n')
        update_match_display(matches)

    # Call update_caller_info initially to populate the display with current data
    update_caller_info()

    # Start the GUI event loop
    app.mainloop()

# Entry point of the script
if __name__ == '__main__':
    main()