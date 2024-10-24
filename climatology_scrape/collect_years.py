
import re
import time
import math
import pickle
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

def compute_pages_to_scrape(text):
    """
    Computes the total number of pages to scrape from the pagination information provided in the format:
    'Showing X to Y of Z entries'.
    """
    total_entries = int(text.split('of')[-1].split('entries')[0].strip())
    entries_per_page = int(text.split('to')[1].split('of')[0].strip())
    total_pages = math.ceil(total_entries / entries_per_page)
    return total_pages

def extract_pagination_text(driver):
    """
    Extracts the pagination information text from the page using Selenium.
    """
    pagination_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.dataTables_info"))
    )
    pagination_text = pagination_element.text
    return pagination_text

def setup_webdriver(url):
    """
    Function to set up the WebDriver and open the given URL.
    """
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
    driver.get(url)
    return driver

def click_pagination_link(driver, page_number):
    """
    Clicks on the pagination link for the given page number.
    
    Parameters:
    - driver: The Selenium WebDriver instance.
    - page_number (int): The page number to click.
    """
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, str(page_number)))
    ).click()
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
    )

# Example usage:
access_url = "https://www.ncei.noaa.gov/oa/local-climatological-data/index.html#v2/access/"
driver = setup_webdriver(access_url)

# Compute the master number of pages to scrape
pages = compute_pages_to_scrape(extract_pagination_text(driver))

# Regex to match URLs ending with a 4-digit number followed by a '/'
regex = r".*/[0-9]{4}/$"

# Dictionary to store results where keys are page numbers and values are lists of URLs
hrefs = {}

# Pages you want to process
pages = [1, 2, 3, 4, 5]

# Loop through each page, wait for it to load, and then process hyperlinks
for page in pages:
    print(f"Currently in Master Page :::: {page}")
    
    # Click on the master page link
    click_pagination_link(driver, page)
    
    # Wait for all hyperlinks to be present on the page
    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
    
    # Get all the hyperlinks on the page
    hyperlinks = driver.find_elements(By.TAG_NAME, "a")
    
    # Initialize a list for the current page's URLs
    hrefs[page] = []
    
    # Filter and add URLs that match the regex
    for link in hyperlinks:
        href = link.get_attribute('href')
        if href and re.search(regex, href):
            hrefs[page].append(href)

# Close the driver
driver.quit()

# Save the 'hrefs' dictionary as a pickle file
with open('hrefs_dict.pkl', 'wb') as f:
    pickle.dump(hrefs, f)

print("Pickle file saved as 'hrefs_dict.pkl'")