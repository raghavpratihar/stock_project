#!/usr/bin/env python3
"""
Enhanced EPS Extraction using requests_html with multithreading
Replaces Selenium-based extraction with faster, more reliable requests_html approach
"""

import pandas as pd
import time
import random
import logging
from tqdm import tqdm
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import concurrent.futures
import threading
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Thread-local storage for sessions
thread_local = threading.local()

def create_session_with_retry():
    """Create a session with retry strategy and timeouts"""
    session = HTMLSession()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,  # number of retries
        backoff_factor=1,  # wait 1, 2, 4 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
    )
    
    # Create adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_session():
    """Get or create a session for the current thread"""
    if not hasattr(thread_local, "session"):
        thread_local.session = create_session_with_retry()
    return thread_local.session

def extract_eps_from_url(url):
    """Extract EPS value from XBRL URL using requests_html with timeout"""
    try:
        # Get session for current thread
        session = get_session()
        
        # Get the XML content with timeout
        r = session.get(url)
        r.raise_for_status()
        
        # Parse the XML with BeautifulSoup
        soup = BeautifulSoup(r.text, 'xml')

        # Define the namespace prefix
        namespace = {'in-bse-fin': 'http://example.com/ns'}

        # Find the tag with contextRef="FourD"
        tag = soup.find('in-bse-fin:BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations',
                        attrs={'contextRef': 'FourD'})

        # Extract the value of the tag if found
        value = tag.text.strip() if tag else None
        
        if value == None:
            tag = soup.find('in-bse-fin:DilutedEarningsPerShareAfterExtraordinaryItems',
                        attrs={'contextRef': 'FourD'})
            value = tag.text.strip() if tag else None
        
        return value
        
    except requests.exceptions.Timeout:
        # Log timeout specifically
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_msg = f"[{timestamp}] Timeout extracting EPS from {url}\n"
        
        with open('failed_eps_extractions.log', 'a', encoding='utf-8') as f:
            f.write(error_msg)
        
        logger.warning(f"Timeout extracting EPS from {url}")
        return None
        
    except Exception as e:
        # Log failed extraction to file
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_msg = f"[{timestamp}] Error extracting EPS from {url}: {e}\n"
        
        with open('failed_eps_extractions.log', 'a', encoding='utf-8') as f:
            f.write(error_msg)
        
        logger.error(f"Error extracting EPS from {url}: {e}")
        return None

def process_url_batch(urls, timeout=30):
    """Process a batch of URLs and return EPS values"""
    eps_values = []
    for url in urls:
        eps_value = extract_eps_from_url(url)
        eps_values.append(eps_value)
        # Add small delay between requests to avoid overwhelming the server
        time.sleep(0.1)
    return eps_values



def extract_eps_with_requests_html_chunked(grouped_df, chunk_size=50, max_workers=3):
    """
    Extract EPS data using requests_html with chunked processing to avoid getting stuck
    
    Args:
        grouped_df: DataFrame with 'xbrl' column containing URLs
        chunk_size: Number of batches to process at once (default: 50)
        max_workers: Maximum number of worker threads (default: 3, reduced further)
        timeout: Timeout for each request in seconds (default: 30)
        
    Returns:
        DataFrame with added 'eps' column
    """
    
    # Initialize EPS column
    grouped_df['eps'] = ''
    
    # Prepare data for multithreading
    url_batches = []
    indices = []
    
    for i in range(grouped_df.shape[0]):
        if len(grouped_df['xbrl'][i]):
            url_batches.append(grouped_df['xbrl'][i])
            indices.append(i)
    
    logger.info(f"Processing {len(url_batches)} batches in chunks of {chunk_size} with {max_workers} workers")
    
    # Process in chunks
    for chunk_start in range(0, len(url_batches), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(url_batches))
        chunk_urls = url_batches[chunk_start:chunk_end]
        chunk_indices = indices[chunk_start:chunk_end]
        
        logger.info(f"Processing chunk {chunk_start//chunk_size + 1}/{(len(url_batches) + chunk_size - 1)//chunk_size} "
                   f"(batches {chunk_start+1}-{chunk_end})")
        
        # Process current chunk using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for current chunk
            future_to_index = {
                executor.submit(process_url_batch, urls): idx 
                for urls, idx in zip(chunk_urls, chunk_indices)
            }
            
            # Process completed tasks with progress bar
            completed_count = 0
            for future in tqdm(concurrent.futures.as_completed(future_to_index), 
                              total=len(future_to_index), 
                              desc=f"Chunk {chunk_start//chunk_size + 1}"):
                idx = future_to_index[future]
                try:
                    eps_values = future.result()
                    grouped_df.at[idx, 'eps'] = eps_values
                    completed_count += 1
                
                except Exception as e:
                    logger.error(f"Error processing batch at index {idx}: {e}")
                    grouped_df.at[idx, 'eps'] = []
        
    
    # Clean up thread-local sessions
    if hasattr(thread_local, "session"):
        try:
            thread_local.session.close()
        except:
            pass
    
    logger.info(f"Completed processing all chunks")
    return grouped_df

