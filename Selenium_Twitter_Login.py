from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--user-data-dir=ChromeUserData")
options.add_argument("--remote-debugging-port=9222")
options.page_load_strategy = 'normal'

driver = webdriver.Chrome(options=options)
driver.get("https://www.twitter.com/")