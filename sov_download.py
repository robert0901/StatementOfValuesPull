from playwright.sync_api import sync_playwright
import os
import pandas as pd
import fig_tree
from bs4 import BeautifulSoup
import re

def sov_scrape():
    user_dir = '/tmp/playwright'
    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(user_dir,headless=False, slow_mo=50)
        page = browser.new_page()
        page.goto('https://erisk.duffandphelps.com/login.aspx?ReturnUrl=%2fportal%2fDetail.aspx%3fType%3dBuildingList&Type=BuildingList')
        page.fill("id=ctl00_cphBody_username", fig_tree.username)#Username
        page.fill("id=ctl00_cphBody_password", fig_tree.password)#Password
        page.click('id=ctl00_cphBody_btnLogin')# Login
        page.click("id=ctl00_btnDashboardBuildings")#Access Buildings
        verification_table = pd.read_html(page.inner_html("id=ctl00_cphBody_pnlBuildings"))[1]#Pull Kroll's Building Summary
        with page.expect_download() as download_info: #Download XLS Fle
            page.click("id=ctl00_cphBody_btnPrintSummary")
        download = download_info.value
        file_name = download.suggested_filename
        destination_folder_path = "I:/DW & Systems/American Appraisal/AA Downloads/current sov" #Store current Building file
        download.save_as(os.path.join(destination_folder_path, file_name))
        page.click("id=ctl00_btnDashboardVehicles")#Same with vehicles
        vehicle_count=page.inner_text("id=ctl00_cphBody_lblResultCount")
        pattern = '\(|records selected|\s|\)|,'
        vehicle_count=re.sub(pattern,"",vehicle_count) #Vehicle Count for validation
        verification_table["Vehicle Count"]= int(vehicle_count)
        with page.expect_download() as download_info:
            page.click("id=ctl00_cphBody_btnPrintVehiclesMass")
        download = download_info.value
        file_name = download.suggested_filename
        destination_folder_path = "I:/DW & Systems/American Appraisal/AA Downloads/current sov"
        download.save_as(os.path.join(destination_folder_path, file_name))
    return(verification_table)
if __name__ == '__main__':
    # test1.py executed as script
    # do something
    sov_scrape()