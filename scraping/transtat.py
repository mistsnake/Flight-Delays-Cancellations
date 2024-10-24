
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

# Set up the WebDriver (make sure to adjust the path to your WebDriver)
driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))

# Open the page
driver.get("https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK&QO_fu146_anzr=b0-gvzr")

# Wait for the page to load completely (optional)
wait = WebDriverWait(driver, 10)

# Find all the checkboxes on the page
checkboxes = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")

# Scroll and click each checkbox if it's not selected
for checkbox in checkboxes:
    if "chk" not in checkbox.get_attribute('name') and not checkbox.is_selected():
        driver.execute_script("arguments[0].scrollIntoView(true);", checkbox)  # Scroll into view
        checkbox.click()

# Scroll back up
driver.execute_script("window.scrollTo(0, 0);")  # This scrolls back to the top

#time.sleep(1)
#driver.find_element(By.ID, "btnDownload").click()

#driver.quit()