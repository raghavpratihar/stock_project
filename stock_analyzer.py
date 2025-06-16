from abc import ABC, abstractmethod
import pandas as pd
import requests
import json
from tqdm import tqdm
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class StockData:
    """Data class to hold stock information"""
    symbol: str
    price: float
    name: str
    eps: Optional[List[float]] = None

class WebDriverManager:
    """Manages the web driver instance and its configuration"""
    def __init__(self):
        self.driver = None
        self._setup_driver()

    def _setup_driver(self):
        """Setup and configure the Chrome web driver"""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Mozilla",
                platform="Win32",
                webgl_vendor="Mozilla",
                renderer="Mozilla",
                fix_hairline=True)

    def get_cookies(self) -> str:
        """Get cookies from the current session"""
        self.driver.get('https://www.nseindia.com/market-data/52-week-low-equity-market')
        time.sleep(2)
        cookies = self.driver.get_cookies()
        return "; ".join([f"{item['name']}={item['value']}" for item in cookies])

    def close(self):
        """Close the web driver"""
        if self.driver:
            self.driver.quit()

class NSEAPIClient:
    """Handles all NSE API related operations"""
    def __init__(self, cookie: str):
        self.cookie = cookie
        self.base_url = "https://www.nseindia.com/api"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': cookie,
            'priority': 'u=1, i',
            'referer': 'https://www.nseindia.com/market-data/52-week-low-equity-market',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }

    def get_52week_low_stocks(self) -> List[Dict[str, Any]]:
        """Fetch 52-week low stocks data"""
        url = f"{self.base_url}/live-analysis-data-52weeklowstock"
        response = requests.get(url, headers=self.headers)
        return json.loads(response.text)['data']

    def get_financial_reports(self, symbol: str, name: str) -> Dict[str, Any]:
        """Fetch financial reports for a given stock"""
        url = f"{self.base_url}/corporates-financial-results"
        params = {
            'index': 'equities',
            'symbol': symbol,
            'issuer': name.replace(' ', '%20'),
            'period': 'Annual'
        }
        response = requests.get(url, headers=self.headers, params=params)
        return json.loads(response.text)

class FinancialDataParser:
    """Parses financial data from XML reports"""
    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver

    def parse_eps_data(self, url: str) -> Optional[float]:
        """Parse EPS data from XML report"""
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, 'xml')
        
        # Try to find EPS in different tags
        eps_tags = [
            'in-bse-fin:BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations',
            'in-bse-fin:DilutedEarningsPerShareAfterExtraordinaryItems'
        ]
        
        for tag_name in eps_tags:
            tag = soup.find(tag_name, attrs={'contextRef': 'FourD'})
            if tag and tag.text.strip():
                return float(tag.text.strip())
        return None

class StockAnalyzer:
    """Main class to analyze stocks"""
    def __init__(self):
        self.web_driver = WebDriverManager()
        self.cookie = self.web_driver.get_cookies()
        self.api_client = NSEAPIClient(self.cookie)
        self.parser = FinancialDataParser(self.web_driver.driver)
        self.stocks_data: List[StockData] = []

    def fetch_52week_low_stocks(self) -> pd.DataFrame:
        """Fetch and process 52-week low stocks"""
        data = self.api_client.get_52week_low_stocks()
        rows = []
        for item in data[1:]:  # Skip header
            rows.append([item['symbol'], item['ltp'], item['comapnyName']])
        return pd.DataFrame(rows, columns=['symbol', 'price', 'name'])

    def analyze_stocks(self):
        """Main method to analyze stocks"""
        try:
            # Fetch 52-week low stocks
            df = self.fetch_52week_low_stocks()
            symbol_to_name = df.set_index('symbol')['name'].to_dict()

            # Process each stock
            for symbol, name in tqdm(symbol_to_name.items(), desc="Processing symbols"):
                stock_data = StockData(symbol=symbol, price=df[df['symbol'] == symbol]['price'].iloc[0], name=name)
                
                # Get financial reports
                reports = self.api_client.get_financial_reports(symbol, name)
                if reports:
                    # Process consolidated reports
                    consolidated_data = [item for item in reports if item['consolidated'] != 'Non-Consolidated']
                    urls = [item['xbrl'] for item in consolidated_data if item['xbrl'].rsplit('/', 1)[-1] != '-']
                    
                    eps_values = []
                    for url in urls:
                        eps = self.parser.parse_eps_data(url)
                        if eps is not None:
                            eps_values.append(eps)
                    
                    stock_data.eps = eps_values
                
                self.stocks_data.append(stock_data)

        except Exception as e:
            logger.error(f"Error during stock analysis: {str(e)}")
            raise
        finally:
            self.web_driver.close()

    def get_analysis_results(self) -> pd.DataFrame:
        """Return analysis results as a DataFrame"""
        results = []
        for stock in self.stocks_data:
            results.append({
                'symbol': stock.symbol,
                'price': stock.price,
                'name': stock.name,
                'eps': stock.eps
            })
        return pd.DataFrame(results)

def main():
    """Main function to run the stock analysis"""
    analyzer = StockAnalyzer()
    analyzer.analyze_stocks()
    results_df = analyzer.get_analysis_results()
    print(results_df)

if __name__ == "__main__":
    main() 