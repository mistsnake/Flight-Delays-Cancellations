
import re
import time
import pickle
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

# Set up the WebDriver
driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
access_url = "https://www.ncei.noaa.gov/oa/local-climatological-data/index.html#v2/access/"

# Open the page
driver.get(access_url)

# Regex to match URLs ending with a 4-digit number followed by a '/'
regex = r".*/[0-9]{4}/$"

# Dictionary to store results
hrefs = {'pages': [1, 2, 3, 4, 5], 'href_list': []}

# Loop through each page, wait for it to load, and then process hyperlinks
for page in hrefs['pages']:
    # Scroll back to the top and click the page number link
    driver.execute_script("window.scrollTo(0, 0);")
    
    # Wait until the page number link is clickable and click it
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, str(page)))).click()
    
    # Wait for the page to load (You can adjust this based on the actual elements that appear)
    time.sleep(1)  # Alternatively, you could use WebDriverWait for specific elements if needed
    
    # Scroll back to the top after clicking
    driver.execute_script("window.scrollTo(0, 0);")
    
    # Wait for all hyperlinks to be present on the page
    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
    
    # Get all the hyperlinks on the page
    hyperlinks = driver.find_elements(By.TAG_NAME, "a")
    
    # Filter and add URLs that match the regex
    for link in hyperlinks:
        href = link.get_attribute('href')
        if href and re.search(regex, href):
            hrefs['href_list'].append(href)

# Close the driver
driver.quit()

# Save the 'hrefs' dictionary as a pickle file
with open('hrefs_dict.pkl', 'wb') as f:
    pickle.dump(hrefs, f)

print("Pickle file saved as 'hrefs_dict.pkl'")
