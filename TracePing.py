import subprocess
import os

def main():
    targets = []

    print("Paste the list of SiteName, TicketNumber, and IPAddress (or just IPAddress), followed by an empty line to finish:")

    while True:
        line = input()
        if line.strip() == "":
            break

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

    print("Running tracert and ping in parallel for multiple IP addresses...")

    for site, ticket, ip in targets:
        title = f"{site} - {ip}"
        command = f'start cmd /k "title {title} && ' \
                  f'echo Site: {site} Ticket: {ticket} IP: {ip} && ' \
                  f'echo Tracert Output for {site} ({ticket}, {ip}): && ' \
                  f'tracert -w 300 {ip} && ' \
                  f'echo Ping Output for {site} ({ticket}, {ip}): && ' \
                  f'ping {ip} && ' \
                  f'echo Site: {site} Ticket: {ticket} IP: {ip} && ' \
                  f'echo Press any key to close... && pause >nul"'
        subprocess.Popen(command, shell=True)

    print("Quitting...")

if __name__ == "__main__":
    main()
