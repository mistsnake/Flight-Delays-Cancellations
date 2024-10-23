
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

# Set up the WebDriver (make sure to adjust the path to your WebDriver)
driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
access_url = "https://www.ncei.noaa.gov/oa/local-climatological-data/index.html#v2/access/"

# Open the page
driver.get(access_url)

# Find all hyperlink elements on the page
hyperlinks = driver.find_elements(By.TAG_NAME, "a")

# Regex to match URLs ending with a 4-digit number followed by a '/'
regex = r".*/[0-9]{4}/$"

# Loop through each hyperlink and filter based on the regex
hrefs = {'pages': [1,2,3,4,5], 'href': []}
for link in hyperlinks:
    href = link.get_attribute('href')
    if href and re.search(regex, href):
        print(f"Matching Hyperlink URL: {href}")

driver.find_element(By.LINK_TEXT, "2").click()

# Close the driver (if desired)
# driver.quit()