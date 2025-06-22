#!/usr/bin/env python3
"""
Selenium Utilities for Web Scraping
A comprehensive collection of Selenium helper functions for various web scraping tasks
"""

import os
import time
import logging
import pandas as pd
import platform
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SeleniumUtils:
    """Main Selenium utilities class"""
    
    def __init__(self, headless=True, download_dir=None, user_agent=None, chrome_path=None):
        """
        Initialize Selenium utilities
        
        Args:
            headless (bool): Run browser in headless mode
            download_dir (str): Directory for downloads (defaults to current directory)
            user_agent (str): Custom user agent string
            chrome_path (str): Custom path to Chrome executable
        """
        self.headless = headless
        self.download_dir = download_dir or os.getcwd()
        self.user_agent = user_agent or 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        self.chrome_path = chrome_path
        self.driver = None
        
        logger.info(f"Initialized SeleniumUtils with download_dir: {self.download_dir}")
    
    def find_chrome_executable(self):
        """
        Find Chrome executable path based on operating system
        
        Returns:
            str: Path to Chrome executable or None if not found
        """
        system = platform.system().lower()
        
        # Common Chrome paths by operating system
        chrome_paths = {
            'darwin': [  # macOS
                '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                '/Applications/Chromium.app/Contents/MacOS/Chromium',
                '/usr/bin/google-chrome',
                '/usr/bin/chromium-browser'
            ],
            'linux': [  # Linux
                '/usr/bin/google-chrome',
                '/usr/bin/chromium-browser',
                '/usr/bin/chromium',
                '/snap/bin/chromium',
                '/usr/bin/google-chrome-stable'
            ],
            'windows': [  # Windows
                r'C:\Program Files\Google\Chrome\Application\chrome.exe',
                r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
                r'C:\Users\{}\AppData\Local\Google\Chrome\Application\chrome.exe'.format(os.getenv('USERNAME', '')),
                r'C:\Program Files\Chromium\Application\chrome.exe'
            ]
        }
        
        # Check custom path first
        if self.chrome_path and os.path.exists(self.chrome_path):
            logger.info(f"Using custom Chrome path: {self.chrome_path}")
            return self.chrome_path
        
        # Check common paths for the current OS
        paths_to_check = chrome_paths.get(system, [])
        
        for path in paths_to_check:
            if os.path.exists(path):
                logger.info(f"Found Chrome at: {path}")
                return path
        
        # Try to find Chrome using 'which' command (Unix-like systems)
        if system in ['darwin', 'linux']:
            try:
                import subprocess
                result = subprocess.run(['which', 'google-chrome'], capture_output=True, text=True)
                if result.returncode == 0:
                    chrome_path = result.stdout.strip()
                    logger.info(f"Found Chrome using 'which': {chrome_path}")
                    return chrome_path
            except Exception as e:
                logger.warning(f"Failed to find Chrome using 'which': {e}")
        
        logger.warning("Chrome executable not found in common locations")
        return None
    
    def create_driver(self, window_size=(1920, 1080), auto_download=True):
        """
        Create and configure Chrome driver
        
        Args:
            window_size (tuple): Browser window size (width, height)
            auto_download (bool): Automatically download ChromeDriver if needed
            
        Returns:
            webdriver.Chrome: Configured Chrome driver
        """
        chrome_options = Options()
        
        # Basic options
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(f"--window-size={window_size[0]},{window_size[1]}")
        
        # Anti-detection options
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Download preferences
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.notifications": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Custom user agent
        chrome_options.add_argument(f"--user-agent={self.user_agent}")
        
        # Set Chrome executable path if found
        chrome_path = self.find_chrome_executable()
        if chrome_path:
            chrome_options.binary_location = chrome_path
            logger.info(f"Using Chrome binary: {chrome_path}")
        
        try:
            # Try to create driver with automatic ChromeDriver management
            if auto_download:
                try:
                    # Use webdriver_manager to automatically download and manage ChromeDriver
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    logger.info("Chrome driver created with automatic ChromeDriver management")
                except Exception as e:
                    logger.warning(f"Automatic ChromeDriver management failed: {e}")
                    # Fallback to manual ChromeDriver
                    self.driver = webdriver.Chrome(options=chrome_options)
                    logger.info("Chrome driver created with manual ChromeDriver")
            else:
                # Manual ChromeDriver path
                self.driver = webdriver.Chrome(options=chrome_options)
                logger.info("Chrome driver created with manual configuration")
            
            # Apply stealth settings
            stealth(self.driver,
                   languages=["en-US", "en"],
                   vendor="Google Inc.",
                   platform="Win32",
                   webgl_vendor="Intel Inc.",
                   renderer="Intel Iris OpenGL Engine",
                   fix_hairline=True)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Chrome driver created successfully")
            return self.driver
            
        except Exception as e:
            logger.error(f"Failed to create Chrome driver: {e}")
            
            # Provide helpful error messages
            if "chromedriver" in str(e).lower():
                logger.error("ChromeDriver not found. Try installing it manually or use auto_download=True")
                logger.error("You can download ChromeDriver from: https://chromedriver.chromium.org/")
            elif "chrome" in str(e).lower():
                logger.error("Chrome browser not found. Please install Google Chrome")
                logger.error("Download from: https://www.google.com/chrome/")
            
            raise
    
    def install_chromedriver(self):
        """
        Install ChromeDriver using webdriver_manager
        
        Returns:
            str: Path to installed ChromeDriver
        """
        try:
            driver_path = ChromeDriverManager().install()
            logger.info(f"ChromeDriver installed at: {driver_path}")
            return driver_path
        except Exception as e:
            logger.error(f"Failed to install ChromeDriver: {e}")
            return None
    
    def check_chrome_installation(self):
        """
        Check if Chrome is properly installed
        
        Returns:
            dict: Status information about Chrome installation
        """
        status = {
            'chrome_found': False,
            'chrome_path': None,
            'chromedriver_available': False,
            'chromedriver_path': None,
            'system': platform.system(),
            'architecture': platform.architecture()[0]
        }
        
        # Check Chrome
        chrome_path = self.find_chrome_executable()
        if chrome_path:
            status['chrome_found'] = True
            status['chrome_path'] = chrome_path
        
        # Check ChromeDriver
        try:
            driver_path = ChromeDriverManager().install()
            status['chromedriver_available'] = True
            status['chromedriver_path'] = driver_path
        except Exception as e:
            logger.warning(f"ChromeDriver not available: {e}")
        
        return status

    def get_cookies(self, url):
        """
        Get cookies from a website
        
        Args:
            url (str): URL to visit and get cookies from
            
        Returns:
            str: Cookie string in format "name=value; name2=value2;"
        """
        if not self.driver:
            self.create_driver()
        
        if not self.driver:
            logger.error("Failed to create driver")
            return None
        
        try:
            self.driver.get(url)
            time.sleep(3)  # Wait for page to load
            
            cookies = self.driver.get_cookies()
            cookie_string = ""
            for cookie in cookies:
                cookie_string += cookie['name'] + "=" + cookie['value'] + "; "
            
            logger.info(f"Successfully obtained {len(cookies)} cookies from {url}")
            return cookie_string.strip()
            
        except Exception as e:
            logger.error(f"Failed to get cookies from {url}: {e}")
            return None
    
    def wait_for_element(self, by, value, timeout=10):
        """
        Wait for an element to be present on the page
        
        Args:
            by: Selenium By strategy (By.ID, By.CLASS_NAME, etc.)
            value (str): Element identifier
            timeout (int): Maximum time to wait in seconds
            
        Returns:
            WebElement: Found element or None if timeout
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return None
            
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            logger.warning(f"Element {value} not found within {timeout} seconds")
            return None
    
    def wait_for_element_clickable(self, by, value, timeout=10):
        """
        Wait for an element to be clickable
        
        Args:
            by: Selenium By strategy
            value (str): Element identifier
            timeout (int): Maximum time to wait in seconds
            
        Returns:
            WebElement: Clickable element or None if timeout
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return None
            
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            return element
        except TimeoutException:
            logger.warning(f"Element {value} not clickable within {timeout} seconds")
            return None
    
    def scroll_to_element(self, element):
        """
        Scroll to a specific element
        
        Args:
            element: WebElement to scroll to
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return
            
        try:
            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(1)
        except Exception as e:
            logger.error(f"Failed to scroll to element: {e}")
    
    def scroll_page(self, scroll_pause_time=1, scroll_amount=1000):
        """
        Scroll down the page gradually
        
        Args:
            scroll_pause_time (int): Time to pause between scrolls
            scroll_amount (int): Amount to scroll each time
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return
            
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while True:
                # Scroll down
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(scroll_pause_time)
                
                # Calculate new scroll height
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                
                # Break if no more content
                if new_height == last_height:
                    break
                last_height = new_height
                
        except Exception as e:
            logger.error(f"Failed to scroll page: {e}")
    
    def download_file(self, url, filename=None):
        """
        Download a file using Selenium
        
        Args:
            url (str): URL of the file to download
            filename (str): Optional filename to save as
            
        Returns:
            str: Path to downloaded file or None if failed
        """
        if not self.driver:
            self.create_driver()
        
        if not self.driver:
            logger.error("Failed to create driver")
            return None
        
        try:
            self.driver.get(url)
            time.sleep(5)  # Wait for download to start
            
            # Get page source (for CSV/text files)
            content = self.driver.page_source
            
            # If it's HTML, the download might have failed
            if '<html' in content.lower():
                logger.warning("Got HTML instead of file content")
                return None
            
            # Save content to file
            if not filename:
                filename = url.split('/')[-1]
            
            file_path = os.path.join(self.download_dir, filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"File downloaded to: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to download file from {url}: {e}")
            return None
    
    def extract_table_data(self, table_selector, headers=None):
        """
        Extract data from HTML table
        
        Args:
            table_selector (str): CSS selector for the table
            headers (list): Optional list of column headers
            
        Returns:
            pd.DataFrame: Extracted table data
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return pd.DataFrame()
            
        try:
            table = self.wait_for_element(By.CSS_SELECTOR, table_selector)
            if not table:
                return pd.DataFrame()
            
            rows = table.find_elements(By.TAG_NAME, "tr")
            data = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells:
                    row_data = [cell.text.strip() for cell in cells]
                    data.append(row_data)
            
            if headers:
                df = pd.DataFrame(data, columns=headers)
            else:
                df = pd.DataFrame(data)
            
            logger.info(f"Extracted {len(df)} rows from table")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract table data: {e}")
            return pd.DataFrame()
    
    def take_screenshot(self, filename="screenshot.png"):
        """
        Take a screenshot of the current page
        
        Args:
            filename (str): Name of the screenshot file
            
        Returns:
            str: Path to screenshot file
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return None
            
        try:
            file_path = os.path.join(self.download_dir, filename)
            self.driver.save_screenshot(file_path)
            logger.info(f"Screenshot saved to: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to take screenshot: {e}")
            return None
    
    def handle_popup(self, popup_selector=None):
        """
        Handle common popups (cookies, ads, etc.)
        
        Args:
            popup_selector (str): CSS selector for popup close button
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return
            
        try:
            # Common popup selectors
            popup_selectors = [
                popup_selector,
                "[data-testid='close-button']",
                ".close",
                ".popup-close",
                ".modal-close",
                "#close",
                ".cookie-accept",
                ".ad-close"
            ]
            
            for selector in popup_selectors:
                if selector:
                    try:
                        close_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                        close_button.click()
                        logger.info(f"Closed popup with selector: {selector}")
                        time.sleep(1)
                        break
                    except NoSuchElementException:
                        continue
                        
        except Exception as e:
            logger.warning(f"Failed to handle popup: {e}")
    
    def wait_for_page_load(self, timeout=30):
        """
        Wait for page to fully load
        
        Args:
            timeout (int): Maximum time to wait in seconds
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return
            
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            logger.info("Page loaded completely")
        except TimeoutException:
            logger.warning(f"Page did not load completely within {timeout} seconds")
    
    def get_page_source(self):
        """
        Get the current page source
        
        Returns:
            str: Page HTML source
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return ""
        return self.driver.page_source
    
    def execute_script(self, script):
        """
        Execute JavaScript on the page
        
        Args:
            script (str): JavaScript code to execute
            
        Returns:
            Result of JavaScript execution
        """
        if not self.driver:
            logger.error("Driver not initialized")
            return None
            
        try:
            return self.driver.execute_script(script)
        except Exception as e:
            logger.error(f"Failed to execute script: {e}")
            return None
    
    def close(self):
        """Close the browser and clean up"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed successfully")
            except Exception as e:
                logger.error(f"Failed to close browser: {e}")
            finally:
                self.driver = None

# Convenience functions for common tasks
def create_nse_driver(headless=True):
    """
    Create a driver specifically configured for NSE website
    
    Args:
        headless (bool): Run in headless mode
        
    Returns:
        SeleniumUtils: Configured Selenium utilities instance
    """
    utils = SeleniumUtils(headless=headless)
    utils.create_driver()
    return utils

def fetch_csv_with_selenium(url, filename=None):
    """
    Fetch CSV file using Selenium
    
    Args:
        url (str): URL of the CSV file
        filename (str): Optional filename to save as
        
    Returns:
        pd.DataFrame: Parsed CSV data
    """
    utils = SeleniumUtils()
    try:
        utils.create_driver()
        
        # Get cookies first (for NSE)
        if 'nseindia.com' in url and utils.driver:
            utils.get_cookies('https://www.nseindia.com/')
        
        # Download file
        file_path = utils.download_file(url, filename)
        if file_path:
            df = pd.read_csv(file_path)
            logger.info(f"Successfully parsed CSV with {len(df)} rows")
            return df
        else:
            logger.error("Failed to download CSV file")
            return pd.DataFrame()
            
    finally:
        utils.close()

def extract_data_with_retry(url, max_retries=3, delay=5):
    """
    Extract data with retry mechanism
    
    Args:
        url (str): URL to extract data from
        max_retries (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds
        
    Returns:
        str: Page source or None if all retries failed
    """
    utils = SeleniumUtils()
    
    for attempt in range(max_retries):
        try:
            utils.create_driver()
            if not utils.driver:
                logger.error("Failed to create driver")
                continue
                
            utils.driver.get(url)
            utils.wait_for_page_load()
            
            # Handle any popups
            utils.handle_popup()
            
            page_source = utils.get_page_source()
            logger.info(f"Successfully extracted data on attempt {attempt + 1}")
            return page_source
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
            utils.close()
    
    logger.error(f"All {max_retries} attempts failed")
    return None

def diagnose_chrome_issues():
    """
    Diagnose Chrome and ChromeDriver installation issues
    
    Returns:
        dict: Diagnostic information
    """
    utils = SeleniumUtils()
    status = utils.check_chrome_installation()
    
    print("=== Chrome Installation Diagnosis ===")
    print(f"Operating System: {status['system']}")
    print(f"Architecture: {status['architecture']}")
    print(f"Chrome Found: {status['chrome_found']}")
    print(f"Chrome Path: {status['chrome_path']}")
    print(f"ChromeDriver Available: {status['chromedriver_available']}")
    print(f"ChromeDriver Path: {status['chromedriver_path']}")
    
    if not status['chrome_found']:
        print("\n❌ Chrome not found!")
        print("Please install Google Chrome:")
        print("- macOS: https://www.google.com/chrome/")
        print("- Linux: sudo apt install google-chrome-stable")
        print("- Windows: Download from https://www.google.com/chrome/")
    
    if not status['chromedriver_available']:
        print("\n❌ ChromeDriver not available!")
        print("Trying to install ChromeDriver automatically...")
        driver_path = utils.install_chromedriver()
        if driver_path:
            print(f"✅ ChromeDriver installed at: {driver_path}")
        else:
            print("❌ Failed to install ChromeDriver automatically")
            print("Please install manually from: https://chromedriver.chromium.org/")
    
    return status

# Example usage
if __name__ == "__main__":
    # Diagnose Chrome installation
    diagnose_chrome_issues()
    
    # Example: Fetch NSE equity list
    utils = create_nse_driver(headless=True)
    try:
        # Get cookies
        cookies = utils.get_cookies('https://www.nseindia.com/')
        print(f"Got cookies: {cookies[:100]}...")
        
        # Download equity list
        csv_url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        file_path = utils.download_file(csv_url, "EQUITY_L.csv")
        
        if file_path:
            df = pd.read_csv(file_path)
            print(f"Downloaded {len(df)} equity records")
            print(df.head())
        
    finally:
        utils.close() 