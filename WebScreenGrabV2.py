#!/usr/bin/env python
r"""
WebScreenGrab.py

Usage:
    python WebScreenGrab.py ips.txt --local-chromedriver "C:\Users\V613867\Desktop\Projects\tools\chromedriver-win64\chromedriver.exe"
    [--output-excel results.xlsx] [--timeout 10] [--no-headless]

Description:
    Reads a list of IPs/hosts from a file, removing duplicates. For each host:
      - Pings the host to see if it's reachable. If not, skip.
      - Tries HTTP first, then HTTPS for verification.
      - Takes a screenshot (preferring the protocol that worked first) and collects metadata.
      - Writes an Excel row (with embedded screenshot) for each host in real-time.
      - The script is headless by default.

Dependencies:
    pip install selenium requests openpyxl
"""

import argparse
import base64
import logging
import os
import socket
import subprocess
import sys
import time
from time import sleep
import urllib3
from io import BytesIO

import requests
from openpyxl import Workbook, load_workbook
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# Disable InsecureRequestWarnings from requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global columns for Excel
EXCEL_COLUMNS = [
    "Ip Address",
    "Dev Type",
    "Note",
    "Password",
    "HTTPS Works",
    "Title (Chosen Protocol)",
    "Screenshot",
    "HTTPS Title",
    "HTTPS Status Code",
    "HTTPS Content-Length",
    "HTTPS Content-Type",
    "HTTPS cache-control",
    "HTTPS Remote Body",
    "HTTPS Remote Headers",
    "HTTP Title",
    "HTTP Status Code",
    "HTTP Content-Length",
    "HTTP Content-Type",
    "HTTP cache-control",
    "HTTP Remote Body",
    "HTTP Remote Headers",
    "Ping Success",
    "Protocol Used"
]


def ping_host(ip):
    """Ping a host (Windows style, one ping) to check if it is reachable."""
    try:
        output = subprocess.run(["ping", "-n", "1", ip],
                                capture_output=True, text=True)
        return "TTL" in output.stdout
    except Exception as e:
        logging.error(f"Error pinging {ip}: {e}")
        return False


def check_ip_protocol(ip_address, timeout=2):
    """Quick TCP-based check: returns 'HTTP', 'HTTPS', or 'Neither'."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip_address, 80))
        sock.close()
        return "HTTP"
    except:
        pass

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip_address, 443))
        sock.close()
        return "HTTPS"
    except:
        pass

    return "Neither"


def setup_driver(chrome_driver_path, timeout, headless=True):
    """Initialize the Chrome driver with the given timeout and headless settings."""
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-insecure-localhost")
    try:
        service = Service(executable_path=chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(timeout)
        driver.set_script_timeout(timeout)
        driver.implicitly_wait(2)
    except Exception as e:
        logging.error(f"Error initializing Chrome driver: {e}")
        sys.exit(1)
    return driver


def test_protocol(driver, base_url, protocol, timeout):
    """
    Load the host with Selenium, take a screenshot,
    and fetch response metadata with requests.
    """
    result = {
        "works": False,
        "title": "",
        "screenshot_path": "",
        "status_code": "",
        "content_length": "",
        "content_type": "",
        "cache_control": "",
        "remote_body": "",
        "remote_headers": ""
    }

    full_url = protocol + base_url
    logging.info(f"Testing {full_url}...")

    try:
        driver.get(full_url)
        sleep(2)
        result["title"] = driver.title
        result["works"] = True
    except TimeoutException as te:
        logging.error(f"Timeout loading {full_url}: {te}")
    except Exception as e:
        logging.error(f"Error loading {full_url} with Selenium: {e}")

    if result["works"]:
        try:
            screenshot_b64 = driver.get_screenshot_as_base64()
            ts = int(time.time() * 1000)
            screenshot_filename = os.path.join(
                "screenshots",
                f"{protocol.replace('://','')}_{base_url}_{ts}.png"
            )
            os.makedirs(os.path.dirname(screenshot_filename), exist_ok=True)
            with open(screenshot_filename, "wb") as f:
                f.write(base64.b64decode(screenshot_b64))
            result["screenshot_path"] = screenshot_filename
            logging.info(f"Screenshot saved to {screenshot_filename}")
        except Exception as e:
            logging.error(f"Error taking screenshot for {full_url}: {e}")

    try:
        r = requests.get(full_url, verify=False, timeout=timeout)
        result["status_code"] = r.status_code
        result["content_length"] = r.headers.get("Content-Length", "")
        result["content_type"] = r.headers.get("Content-Type", "")
        result["cache_control"] = r.headers.get("cache-control", "")
        result["remote_body"] = r.text
        result["remote_headers"] = str(r.headers)
    except Exception as e:
        logging.error(f"Error fetching headers/body for {full_url}: {e}")

    return result


def init_excel(excel_filename):
    """
    Create a new Excel workbook with headers if it doesn't exist,
    or load an existing one.
    """
    if os.path.exists(excel_filename):
        try:
            wb = load_workbook(excel_filename)
            ws = wb.active
            logging.info(f"Loaded existing Excel workbook: {excel_filename}")
        except Exception as e:
            logging.error(f"Error loading workbook {excel_filename}: {e}. Creating new workbook.")
            wb = Workbook()
            ws = wb.active
            ws.title = "Results"
            ws.append(EXCEL_COLUMNS)
            wb.save(excel_filename)
    else:
        wb = Workbook()
        ws = wb.active
        ws.title = "Results"
        ws.append(EXCEL_COLUMNS)
        wb.save(excel_filename)
        logging.info(f"Created new Excel workbook: {excel_filename}")
    return wb, ws


def append_excel_row(wb, ws, row_data, excel_filename):
    """
    Append a row of data to the Excel sheet and embed the screenshot.
    Simplified image embedding using a BytesIO stream that remains attached.
    """
    row_num = ws.max_row + 1

    for col_idx, col_name in enumerate(EXCEL_COLUMNS, start=1):
        key = col_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "")
        ws.cell(row=row_num, column=col_idx, value=row_data.get(key, ""))

    # Embed screenshot in column 7 if available
    screenshot_path = row_data.get("screenshot_path", "")
    if screenshot_path:
        try:
            with open(screenshot_path, "rb") as f:
                image_data = f.read()
            image_stream = BytesIO(image_data)
            image_stream.seek(0)
            img = Image(image_stream)
            # Keep a reference to the stream so it isn't garbage-collected
            img._data_stream = image_stream
            img.width = 320
            img.height = 240
            cell_addr = f"{get_column_letter(7)}{row_num}"
            ws.add_image(img, cell_addr)
            ws[cell_addr].alignment = Alignment(horizontal='center', vertical='center')
        except Exception as e:
            logging.error(f"Error embedding screenshot '{screenshot_path}': {e}")

    ws.row_dimensions[row_num].height = 180
    ws.column_dimensions[get_column_letter(7)].width = 46
    ws.column_dimensions['A'].width = 20

    wb.save(excel_filename)


def main():
    parser = argparse.ArgumentParser(
        description="WebScreenGrab - Single-row-per-host with embedded screenshots and metadata."
    )
    parser.add_argument("ip_file", help="Path to the file containing IP addresses/hosts (one per line)")
    parser.add_argument("--local-chromedriver", required=True,
                        help="Path to the local chromedriver executable")
    parser.add_argument("--output-excel", default="results.xlsx",
                        help="Filename for the Excel output")
    parser.add_argument("--timeout", type=int, default=10,
                        help="Timeout in seconds for page loads/HTTP requests")
    parser.add_argument("--no-headless", action="store_true",
                        help="Run Chrome in visible mode (not headless).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s: %(message)s")

    try:
        with open(args.ip_file, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        unique_hosts = list(set(lines))
        logging.info(f"Found {len(lines)} lines, deduplicated to {len(unique_hosts)} entries.")
    except Exception as e:
        logging.error(f"Error reading IP file: {e}")
        sys.exit(1)

    driver = setup_driver(chrome_driver_path=args.local_chromedriver,
                          timeout=args.timeout,
                          headless=(not args.no_headless))
    wb, ws = init_excel(args.output_excel)

    for host in unique_hosts:
        if not ping_host(host):
            logging.info(f"{host} is unreachable. Skipping.")
            continue

        logging.info(f"{host} => probable protocol: {check_ip_protocol(host)}")

        http_res = test_protocol(driver, host, "http://", args.timeout)
        if http_res["works"]:
            protocol_used = "HTTP"
            https_res = test_protocol(driver, host, "https://", args.timeout)
        else:
            https_res = test_protocol(driver, host, "https://", args.timeout)
            protocol_used = "HTTPS" if https_res["works"] else "None"

        row_data = {
            "ip_address": host,
            "dev_type": "",
            "note": "",
            "password": "",
            "https_works": https_res["works"],
            "screenshot_path": "",
            "title_chosen_protocol": "",
            "https_title": https_res["title"],
            "https_status_code": https_res["status_code"],
            "https_content_length": https_res["content_length"],
            "https_content_type": https_res["content_type"],
            "https_cachecontrol": https_res["cache_control"],
            "https_remote_body": https_res["remote_body"],
            "https_remote_headers": https_res["remote_headers"],
            "http_title": http_res["title"],
            "http_status_code": http_res["status_code"],
            "http_content_length": http_res["content_length"],
            "http_content_type": http_res["content_type"],
            "http_cachecontrol": http_res["cache_control"],
            "http_remote_body": http_res["remote_body"],
            "http_remote_headers": http_res["remote_headers"],
            "ping_success": "Yes",
            "protocol_used": protocol_used
        }

        if http_res["works"] and http_res["screenshot_path"]:
            row_data["screenshot_path"] = http_res["screenshot_path"]
            row_data["title_chosen_protocol"] = http_res["title"]
        elif https_res["works"] and https_res["screenshot_path"]:
            row_data["screenshot_path"] = https_res["screenshot_path"]
            row_data["title_chosen_protocol"] = https_res["title"]
        else:
            row_data["screenshot_path"] = ""
            row_data["title_chosen_protocol"] = http_res["title"] or https_res["title"]

        append_excel_row(wb, ws, row_data, args.output_excel)

    driver.quit()
    logging.info("All done.")
    logging.info("Note: Excel images are anchored to cells and won’t move with sorting.")


if __name__ == "__main__":
    main()
