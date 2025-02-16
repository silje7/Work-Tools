#!/usr/bin/env python
import argparse
import base64
import logging
import os
import sys
import time
import urllib3
import xml.etree.ElementTree as ET  # Correct case for ET
from time import sleep

import requests
from openpyxl import Workbook, load_workbook  # Correct case
from openpyxl.drawing.image import Image  # Correct case
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment  # Correct case
from selenium import webdriver
from selenium.common.exceptions import TimeoutException  # Correct case
from selenium.webdriver.chrome.options import Options  # Correct case
from selenium.webdriver.chrome.service import Service  # Correct case
import csv

# disable InsecureRequestWarnings from requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# global columns for excel/csv
excel_columns = [
    "ip/host",
    "https works",
    "title (chosen protocol)",
    "screenshot",
    "https title",
    "https status code",
    "https content-length",
    "https content-type",
    "https cache-control",
    "https remote body",
    "https remote headers",
    "http title",
    "http status code",
    "http content-length",
    "http content-type",
    "http cache-control",
    "http remote body",
    "http remote headers",
]


def setup_driver(chrome_driver_path, timeout):
    """initialize a headless chrome driver with a given timeout."""
    options = Options()
    options.headless = True  # headless mode
    # if using newer chrome, might need: options.add_argument("--headless=new")

    try:
        service = Service(executable_path=chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(timeout)
        driver.set_script_timeout(timeout)
        driver.implicitly_wait(2)
    except Exception as e:
        logging.error(f"error initializing chrome driver: {e}")
        sys.exit(1)
    return driver


def test_protocol(driver, base_url, protocol, timeout):
    """
    attempt to load the given host+protocol in selenium, take a screenshot,
    and also do a requests.get for response metadata.

    returns a dictionary with:
      - works (bool): whether selenium load succeeded
      - title (str): page title from selenium
      - screenshot_path (str): path to saved screenshot png (empty if fail)
      - status_code (int or empty): http status from requests
      - content_length, content_type, cache_control (str): from requests
      - remote_body (str): entire response body from requests
      - remote_headers (str): stringified response headers
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
    logging.info(f"testing {full_url}...")

    # 1) selenium load
    try:
        driver.get(full_url)
        # small wait to allow page to render if needed
        sleep(2)
        result["title"] = driver.title
        result["works"] = True
    except TimeoutException as te:
        logging.error(f"timeout loading {full_url}: {te}")
    except Exception as e:
        logging.error(f"error loading {full_url} with selenium: {e}")

    # 2) screenshot if selenium worked
    if result["works"]:
        try:
            screenshot_b64 = driver.get_screenshot_as_base64()
            # build a unique screenshot filename
            ts = int(time.time() * 1000)
            filename = os.path.join(
                "screenshots",
                f"{protocol.replace('://','')}_{base_url}_{ts}.png"
            )
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "wb") as f:
                f.write(base64.b64decode(screenshot_b64))
            result["screenshot_path"] = filename
            logging.info(f"screenshot saved to {filename}")
        except Exception as e:
            logging.error(f"error taking screenshot for {full_url}: {e}")

    # 3) requests-based metadata
    try:
        r = requests.get(full_url, verify=False, timeout=timeout)
        result["status_code"] = r.status_code
        result["content_length"] = r.headers.get("content-length", "")
        result["content_type"] = r.headers.get("content-type", "")
        result["cache_control"] = r.headers.get("cache-control", "")
        result["remote_body"] = r.text
        result["remote_headers"] = str(r.headers)
    except Exception as e:
        logging.error(f"error fetching headers/body for {full_url}: {e}")

    return result


def init_excel(excel_filename):
    """
    if the excel file does not exist, create it and write headers.
    otherwise, load it.
    returns (workbook, worksheet).
    """
    if os.path.exists(excel_filename):
        wb = load_workbook(excel_filename)
        ws = wb.active
        logging.info(f"loaded existing excel workbook: {excel_filename}")
    else:
        wb = Workbook()
        ws = wb.active
        ws.title = "results"
        ws.append(excel_columns)
        wb.save(excel_filename)
        logging.info(f"created new excel workbook: {excel_filename}")
    return wb, ws


def append_excel_row(wb, ws, row_data, excel_filename):
    """
    append a single row to the excel sheet with embedded screenshot,
    auto-width for that row’s cells, then save immediately.
    """
    row_num = ws.max_row + 1

    # put data in cells
    ws.cell(row=row_num, column=1, value=row_data["ip_host"])
    ws.cell(row=row_num, column=2, value=str(row_data["https_works"]))
    ws.cell(row=row_num, column=3, value=row_data["chosen_title"])
    # column 4 (d) is for screenshot embedding

    ws.cell(row=row_num, column=5, value=row_data["https_title"])
    ws.cell(row=row_num, column=6, value=str(row_data["https_status_code"]))
    ws.cell(row=row_num, column=7, value=row_data["https_content_length"])
    ws.cell(row=row_num, column=8, value=row_data["https_content_type"])
    ws.cell(row=row_num, column=9, value=row_data["https_cache_control"])
    ws.cell(row=row_num, column=10, value=row_data["https_remote_body"])
    ws.cell(row=row_num, column=11, value=row_data["https_remote_headers"])

    ws.cell(row=row_num, column=12, value=row_data["http_title"])
    ws.cell(row=row_num, column=13, value=str(row_data["http_status_code"]))
    ws.cell(row=row_num, column=14, value=row_data["http_content_length"])
    ws.cell(row=row_num, column=15, value=row_data["http_content_type"])
    ws.cell(row=row_num, column=16, value=row_data["http_cache_control"])
    ws.cell(row=row_num, column=17, value=row_data["http_remote_body"])
    ws.cell(row=row_num, column=18, value=row_data["http_remote_headers"])

    # embed screenshot
    if row_data["screenshot_path"]:
        try:
            img = Image(row_data["screenshot_path"])
            # optionally resize the embedded image:
            img.width = 320
            img.height = 240
            cell_addr = f"d{row_num}"  # column 4 is 'd'
            ws.add_image(img, cell_addr)
        except Exception as e:
            logging.error(f"error embedding screenshot '{row_data['screenshot_path']}': {e}")

    # wrap text for the newly added row, update column widths for that row
    for col_idx in range(1, len(excel_columns) + 1):
        cell = ws.cell(row=row_num, column=col_idx)
        cell.alignment = Alignment(wrap_text=True)

        # attempt to expand column width if needed
        val = str(cell.value) if cell.value else ""
        col_letter = get_column_letter(col_idx)
        current_width = ws.column_dimensions[col_letter].width or 10
        needed_width = min(len(val) + 2, 100)  # cap at 100
        if needed_width > current_width:
            ws.column_dimensions[col_letter].width = needed_width

    # force the screenshot column (d) to be a bit wider
    ws.column_dimensions['d'].width = 45

    # save workbook
    wb.save(excel_filename)


def init_xml(xml_filename):
    """
    if xml file doesn't exist, create a root <results> and save it.
    """
    if not os.path.exists(xml_filename):
        root = ET.Element("results")
        tree = ET.ElementTree(root)
        tree.write(xml_filename, encoding="utf-8", xml_declaration=True)
        logging.info(f"created new xml file: {xml_filename}")


def append_xml_entry(xml_filename, row_data):
    """
    load existing xml, append a single <entry>, save immediately.
    """
    tree = ET.parse(xml_filename)
    root = tree.getroot()

    entry = ET.SubElement(root, "entry")
    ET.SubElement(entry, "ip_host").text = row_data["ip_host"]
    ET.SubElement(entry, "https_works").text = str(row_data["https_works"])
    ET.SubElement(entry, "chosen_title").text = row_data["chosen_title"]
    ET.SubElement(entry, "screenshot_path").text = row_data["screenshot_path"]

    # https info
    https_elem = ET.SubElement(entry, "https_info")
    ET.SubElement(https_elem, "title").text = row_data["https_title"]
    ET.SubElement(https_elem, "status_code").text = str(row_data["https_status_code"])
    ET.SubElement(https_elem, "content_length").text = row_data["https_content_length"]
    ET.SubElement(https_elem, "content_type").text = row_data["https_content_type"]
    ET.SubElement(https_elem, "cache_control").text = row_data["https_cache_control"]
    ET.SubElement(https_elem, "remote_body").text = row_data["https_remote_body"]
    ET.SubElement(https_elem, "remote_headers").text = row_data["https_remote_headers"]

    # http info
    http_elem = ET.SubElement(entry, "http_info")
    ET.SubElement(http_elem, "title").text = row_data["http_title"]
    ET.SubElement(http_elem, "status_code").text = str(row_data["http_status_code"])
    ET.SubElement(http_elem, "content_length").text = row_data["http_content_length"]
    ET.SubElement(http_elem, "content_type").text = row_data["http_content_type"]
    ET.SubElement(http_elem, "cache_control").text = row_data["http_cache_control"]
    ET.SubElement(http_elem, "remote_body").text = row_data["http_remote_body"]
    ET.SubElement(http_elem, "remote_headers").text = row_data["http_remote_headers"]

    tree.write(xml_filename, encoding="utf-8", xml_declaration=True)


def init_csv(csv_filename):
    """
    if csv doesn't exist, create it and write the header row.
    otherwise do nothing.
    """
    if not os.path.exists(csv_filename):
        with open(csv_filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(excel_columns)
        logging.info(f"created new csv file: {csv_filename}")


def append_csv_row(csv_filename, row_data):
    """
    append one row to csv. we won't embed images in csv (only store path).
    """
    with open(csv_filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            row_data["ip_host"],
            str(row_data["https_works"]),
            row_data["chosen_title"],
            row_data["screenshot_path"],

            row_data["https_title"],
            row_data["https_status_code"],
            row_data["https_content_length"],
            row_data["https_content_type"],
            row_data["https_cache_control"],
            row_data["https_remote_body"],
            row_data["https_remote_headers"],

            row_data["http_title"],
            row_data["http_status_code"],
            row_data["http_content_length"],
            row_data["http_content_type"],
            row_data["http_cache_control"],
            row_data["http_remote_body"],
            row_data["http_remote_headers"]
        ])


def main():
    parser = argparse.ArgumentParser(
        description="webscreengrab - single-row-per-host with embedded screenshots and metadata, updated per-entry."
    )
    parser.add_argument("ip_file", help="path to the file containing ip addresses/hosts (one per line)")
    parser.add_argument("--local-chromedriver", required=True, help="path to the local chromedriver executable")
    parser.add_argument("--output-excel", default="results.xlsx", help="filename for the excel output")
    parser.add_argument("--output-xml", default="results.xml", help="filename for the xml output")
    parser.add_argument("--output-csv", default="results.csv", help="filename for the csv output")
    parser.add_argument("--timeout", type=int, default=10, help="timeout in seconds for page loads/http requests")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

    # read ips/hosts, remove duplicates
    try:
        with open(args.ip_file, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        unique_hosts = list(set(lines))  # remove duplicates
        logging.info(f"found {len(lines)} ip/host lines, deduplicated to {len(unique_hosts)} entries.")
    except Exception as e:
        logging.error(f"error reading ip file: {e}")
        sys.exit(1)

    # initialize selenium driver
    driver = setup_driver(args.local_chromedriver, args.timeout)

    # initialize or load excel, xml, csv
    wb, ws = init_excel(args.output_excel)
    init_xml(args.output_xml)
    init_csv(args.output_csv)

    # process each unique host
    for host in unique_hosts:
        https_res = test_protocol(driver, host, "https://", args.timeout)
        http_res = test_protocol(driver, host, "http://", args.timeout)

        # construct a single row of data
        row_data = {
            "ip_host": host,
            "https_works": https_res["works"],  # True/False
            "screenshot_path": "",
            "chosen_title": "",
            # https columns
            "https_title": https_res["title"],
            "https_status_code": https_res["status_code"],
            "https_content_length": https_res["content_length"],
            "https_content_type": https_res["content_type"],
            "https_cache_control": https_res["cache_control"],
            "https_remote_body": https_res["remote_body"],
            "https_remote_headers": https_res["remote_headers"],
            # http columns
            "http_title": http_res["title"],
            "http_status_code": http_res["status_code"],
            "http_content_length": http_res["content_length"],
            "http_content_type": http_res["content_type"],
            "http_cache_control": http_res["cache_control"],
            "http_remote_body": http_res["remote_body"],
            "http_remote_headers": http_res["remote_headers"]
        }

        # decide which screenshot to embed (prefer https if it worked)
        if https_res["works"] and https_res["screenshot_path"]:
            row_data["screenshot_path"] = https_res["screenshot_path"]
            row_data["chosen_title"] = https_res["title"]
        elif http_res["works"] and http_res["screenshot_path"]:
            row_data["screenshot_path"] = http_res["screenshot_path"]
            row_data["chosen_title"] = http_res["title"]
        else:
            row_data["screenshot_path"] = ""
            # if neither protocol loaded in selenium, fallback to whichever title we have
            row_data["chosen_title"] = https_res["title"] or http_res["title"]

        # append to excel, xml, csv one entry at a time
        append_excel_row(wb, ws, row_data, args.output_excel)
        append_xml_entry(args.output_xml, row_data)
        append_csv_row(args.output_csv, row_data)

    driver.quit()
    logging.info("all done.")


if __name__ == "__main__":
    main()

# If you receive a "Not available" error for the page move on after 5 seconds
# I dont want to scan IPs that are down so if you cant reach it via https or http move on.
# Intent, i dont want to waste time trying to scan IPs that are down. Integrate a Ping column and Protocol column showing whether its http or https
