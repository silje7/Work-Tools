import subprocess
import csv
import os

# Paths to the files
ip_file = 'targets.txt'
community_file = 'restrings.txt'
working_output_file = 'working_snmp_results.csv'
timeout_output_file = 'timedout_snmp_results.csv'

def run_snmpwalk(ip, community_string):
    """Run snmpwalk and return the output."""
    try:
        # Execute the snmpwalk command with subprocess
        result = subprocess.run(
            ["snmpwalk.exe", "-v", "2c", "-c", community_string, ip],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Return stdout and stderr output
        return result.stdout, result.stderr
    except Exception as e:
        return None, str(e)

def main():
    # Load the list of IP addresses and community strings
    if not os.path.exists(ip_file) or not os.path.exists(community_file):
        print("Error: The required input files (targets.txt, restrings.txt) were not found.")
        return

    with open(ip_file, 'r') as ip_f:
        ip_list = [line.strip() for line in ip_f.readlines()]

    with open(community_file, 'r') as community_f:
        community_list = [line.strip() for line in community_f.readlines()]

    # To store working combinations and timed-out combinations
    working_results = []
    timedout_results = []

    # Open CSV files to save the results
    with open(working_output_file, 'w', newline='') as working_csv, open(timeout_output_file, 'w', newline='') as timeout_csv:
        working_writer = csv.writer(working_csv)
        timeout_writer = csv.writer(timeout_csv)
        
        # Write headers for both files
        working_writer.writerow(['IP Address', 'Community String'])
        timeout_writer.writerow(['IP Address', 'Community Strings Tried'])

        # Iterate through each IP and each community string
        for ip in ip_list:
            ip_worked = False  # Flag to track if any community string works for the IP
            timeout_strings = []  # To collect all failed community strings for the IP

            for community in community_list:
                print(f"Testing IP: {ip} with community string: {community}")
                output, error = run_snmpwalk(ip, community)

                # Check if there's valid output (anything that is not empty or an error)
                if output and "Timeout" not in error:
                    print(f"Working SNMP for {ip} with community string {community}\n")
                    working_results.append((ip, community))
                    working_writer.writerow([ip, community])
                    ip_worked = True
                    break  # Stop testing other community strings if one works
                else:
                    timeout_strings.append(community)
            
            # If no community string worked, save to timeout list
            if not ip_worked:
                print(f"Timed out for {ip} with all community strings\n")
                timedout_results.append((ip, timeout_strings))
                timeout_writer.writerow([ip, ", ".join(timeout_strings)])

if __name__ == "__main__":
    main()