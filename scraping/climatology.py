
import math
import re
import os
import subprocess
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from multiprocessing import Pool
from tqdm import tqdm
from datetime import datetime

def extract_pagination_text(driver):
    """
    Extracts the pagination information text from the page using Selenium.
    """
    pagination_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.dataTables_info"))
    )
    pagination_text = pagination_element.text
    return pagination_text

def compute_pages_to_scrape(text):
    """
    Computes the total number of pages to scrape from the pagination information provided in the format:
    'Showing X to Y of Z entries'.
    """
    total_entries = int(text.split('of')[-1].split('entries')[0].strip())
    entries_per_page = int(text.split('to')[1].split('of')[0].strip())
    total_pages = math.ceil(total_entries / entries_per_page)
    return total_pages

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

def get_all_download_links(driver, master_url):
    #https://www.ncei.noaa.gov/oa/local-climatological-data/index.html#v2/archive/
    """Returns list of all .tar.gz files (from all pages)"""
    
    # Navigate to the base URL
    driver.get(master_url)

    # Compute the master number of pages to scrape
    pages_to_collect = compute_pages_to_scrape(extract_pagination_text(driver))

    # Regex to match URLs ending with a 4-digit number followed by a '/'
    regex = r"lcd_v2.0.0_d.*.tar.gz$"

    # Dictionary to store results where keys are page numbers and values are lists of URLs
    download_links = []

    # Loop through each page, wait for it to load, and then process hyperlinks
    for page in range(1, pages_to_collect+1):
        print(f"Currently in Master Page: {page}")
        
        # Click on the master page link
        click_pagination_link(driver, page)
        
        # Wait for all hyperlinks to be present on the page
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
        
        # Get all the hyperlinks on the page
        hyperlinks = driver.find_elements(By.TAG_NAME, "a")
        
        # Filter and add URLs that match the regex
        for link in hyperlinks:
            href = link.get_attribute('href')
            if href and re.search(regex, href):
                download_links.append(href)

    print("Finished collecting all download links from associated pages ...\n")
    return download_links

# Setup the WebDriver
def setup_webdriver():
    """Sets up and returns a WebDriver instance using cached geckodriver."""
    geckodriver_path = "/home/stochastic1017/.wdm/drivers/geckodriver/linux64/v0.35.0/geckodriver"
    service = FirefoxService(executable_path=geckodriver_path)
    options = FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Firefox(service=service, options=options)
    return driver

def setup_logging():
    """Sets up basic logging configuration."""
    log_base_dir = 'logs'
    os.makedirs(log_base_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logging.basicConfig(
        filename=os.path.join(log_base_dir, f'scrape_{timestamp}.log'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler())

def try_download_url(url, output_path):
    """Attempts to download from a specific URL."""
    try:
        subprocess.run(['wget', '-O', output_path, url], check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError:
        return False

def download_tar_gz_file(year, output_folder, retry_count=3):
    """Downloads a single .tar.gz file for the given year with retry mechanism."""
    # Define the date patterns to try in order
    date_patterns = ['20240724', '20240731', '20240726']
    
    for date_pattern in date_patterns:
        url = f"https://www.ncei.noaa.gov/oa/local-climatological-data/v2/archive/lcd_v2.0.0_d{year}_c{date_pattern}.tar.gz"
        file_name = url.split('/')[-1]
        output_path = os.path.join(output_folder, file_name)
        
        logging.info(f"Trying pattern {date_pattern} for year {year}: {url}")
        
        # Try multiple times for each URL
        for attempt in range(retry_count):
            if try_download_url(url, output_path):
                logging.info(f"Successfully downloaded {file_name} with pattern {date_pattern} after {attempt + 1} attempt(s)")
                return True
            
            if attempt < retry_count - 1:
                logging.info(f"Retrying download attempt {attempt + 1} for {file_name}")
        
        # If we've exhausted retries for this pattern, try the next pattern
        logging.info(f"Pattern {date_pattern} failed for year {year}, trying next pattern if available")
    
    # If we get here, all patterns have failed
    logging.error(f"Failed to download file for year {year} with all patterns after {retry_count} attempts each")
    return False

def download_tar_gz_file_wrapper(args):
    """Wrapper function for passing arguments to Pool."""
    return download_tar_gz_file(*args)

def download_tar_gz_files_parallel(years, output_folder, num_workers=8):
    """Parallelizes the downloading of .tar.gz files for a list of years."""
    os.makedirs(output_folder, exist_ok=True)
    
    # Prepare the arguments for multiprocessing
    download_args = [(year, output_folder) for year in years]
    
    # Use a multiprocessing pool with progress bar
    with Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(download_tar_gz_file_wrapper, download_args),
            total=len(years),
            desc="Downloading files"
        ))
    
    # Check for any failed downloads
    failed_years = [years[i] for i, success in enumerate(results) if not success]
    if failed_years:
        logging.error(f"Failed to download files for the following years: {failed_years}")
    else:
        logging.info("All files downloaded successfully.")
    
    # Return failed years for potential retry
    return failed_years

if __name__ == '__main__':
    # Set up logging for the entire script
    setup_logging()
    
    # Define the base URL for scraping
    master_url = "https://www.ncei.noaa.gov/oa/local-climatological-data/index.html#v2/archive/"
    
    # Set the output folder for downloaded files
    output_folder = 'tar_gz_downloads'
    
    # Define the number of parallel workers
    num_workers = 8
    
    # Set up Selenium WebDriver for headless browsing
    driver = setup_webdriver()
    
    try:
        # Collect all .tar.gz download links using Selenium
        download_links = get_all_download_links(driver, master_url)
    finally:
        driver.quit()  # Ensure the driver quits after the scraping

    # Log total download links found
    logging.info(f"Total .tar.gz files found: {len(download_links)}")
    
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Define a wrapper function to download all collected .tar.gz files
    def download_link_wrapper(args):
        url, output_folder = args
        file_name = url.split('/')[-1]
        output_path = os.path.join(output_folder, file_name)
        return try_download_url(url, output_path)
    
    # Prepare download arguments for multiprocessing
    download_args = [(link, output_folder) for link in download_links]
    
    # Parallel download using Pool and tqdm for progress bar
    with Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(download_link_wrapper, download_args),
            total=len(download_links),
            desc="Downloading .tar.gz files"
        ))
    
    # Check for any failed downloads
    failed_links = [download_links[i] for i, success in enumerate(results) if not success]
    if failed_links:
        logging.error(f"Failed to download {len(failed_links)} files.")
        logging.error(f"Failed links: {failed_links}")
    else:
        logging.info("All files downloaded successfully.")
