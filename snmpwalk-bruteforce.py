import subprocess
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# paths to the files
ip_file = r'targetips.txt'
community_file = r'readstrings.txt'
working_output_file = r'working_snmp_results.csv'
timeout_output_file = r'timedout_snmp_results.csv'

def run_snmpwalk(ip, community_string):
    """run snmpwalk and return the output."""
    try:
        # execute the snmpwalk command with subprocess
        result = subprocess.run(
            ["snmpwalk.exe", "-v", "2c", "-c", community_string, ip],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout= 5  # set timeout to 10 seconds
        )
        # return stdout and stderr output
        return result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except Exception as e:
        return None, str(e)

def test_ip_community(ip, community):
    """tests a single ip and community string combination."""
    print(f"testing ip: {ip} : {community}")
    output, error = run_snmpwalk(ip, community)

    if output and "timeout" not in error:
        print(f"working: {ip} with community string {community}\n")
        return ip, community, "working"
    else:
        return ip, community, "timeout"

def main():
    # load the list of ip addresses and community strings
    if not os.path.exists(ip_file) or not os.path.exists(community_file):
        print("error: the required input files (targets.txt, restrings.txt) were not found.")
        return

    with open(ip_file, 'r') as ip_f:
        ip_list = [line.strip() for line in ip_f.readlines()]

    with open(community_file, 'r') as community_f:
        community_list = [line.strip() for line in community_f.readlines()]

    # to store working combinations 
    working_results = []
    timed_out_ips = []  # Store IPs that timed out with all communities

    # iterate over ips
    for ip in ip_list:
        # flag to track if a working community string is found for the current ip
        working_community_found = False
        
        # use a threadpoolexecutor to run checks concurrently for each community string for the current ip
        with ThreadPoolExecutor(max_workers=10) as executor:  # adjust max_workers as needed
            futures = {executor.submit(test_ip_community, ip, community): community
                       for community in community_list}

            for future in as_completed(futures):
                community = futures[future]
                ip, _, result_type = future.result()
                
                if result_type == "working":
                    working_results.append((ip, community))
                    working_community_found = True
                    break  # stop testing other community strings for this ip
        
        # If no working community found, log the IP and all tried communities
        if not working_community_found:
            timed_out_ips.append((ip, community_list))

    # save the results to csv file
    with open(working_output_file, 'w', newline='') as working_csv:
        working_writer = csv.writer(working_csv)
        working_writer.writerow(['ip address', 'community string'])
        for ip, community in working_results:
            working_writer.writerow([ip, community])

    # Save the timed-out IPs and communities to a separate file
    with open(timeout_output_file, 'w', newline='') as timeout_csv:
        timeout_writer = csv.writer(timeout_csv)
        timeout_writer.writerow(['ip address', 'community strings'])
        for ip, communities in timed_out_ips:
            timeout_writer.writerow([ip, ', '.join(communities)])  # Join communities into a single string

if __name__ == "__main__":
    main()
