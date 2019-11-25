import os
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DRIVER_PATH = os.path.join(BASE_DIR, 'driver', 'chromedriver.exe')

class Login(object):
    def __init__(self):
        self.driver = None

    def get_token(self):
        options = Options()
        options.headless = True

        # options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])
        # options.add_argument('--disable-gpu')
        # options.add_argument('--headless')

        self.driver = webdriver.Chrome(executable_path=DRIVER_PATH, chrome_options=options)
        self.driver.get("https://www.fireant.vn/")
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'btn-sm')]")))
        btn_submit = self.driver.find_element_by_xpath("//a[contains(@class, 'btn-sm')]")

        self.driver.execute_script("arguments[0].click();", btn_submit)

        # time.sleep(1)

        wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@class='form-group']/button")))
        usename = self.driver.find_element_by_xpath("//input[@id='username']")
        password = self.driver.find_element_by_xpath("//input[@id='password']")
        btn_login = self.driver.find_element_by_xpath("//div[@class='form-group']/button")

        # action.send_keys_to_element('kid1412bk@gmail.com', usename).perform()
        # action.send_keys_to_element('tuan1412', password).perform()
        usename.send_keys('kid1412bk@gmail.com')
        password.send_keys('tuan1412')
        btn_login.click()

        time.sleep(3)

        cookies = self.driver.get_cookie('__RequestVerificationToken')
        print(cookies.get('value'))
        return cookies.get('value')

    def close(self):
        self.driver.quit()
