import re
import os

# Extracts call details from a Cisco Finesse/Jabber log line
def extract_last_caller(log_file):
    if not os.path.isfile(log_file):
        print(f"Log file not found: {log_file}")
        return None

    last_call = None
    with open(log_file, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*callingPartyNumber='(\\+?\d+)'", line)
            if match:
                last_timestamp = match.group(1)
                calling_party_number = match.group(4)
                last_caller = calling_party_number
    
    if 'last' in locals():
        print(f"Last Caller ID: {last}")
    else:
        print("No caller information found in log.")

# Specify your Cisco Jabber log file path
log_file_path = os.path.expandvars(r"%userprofile%\\appdata\\local\\cisco\\unified communications\\jabber\\csf\\logs\\jabber.log")

extract_last_caller(log_file_path)