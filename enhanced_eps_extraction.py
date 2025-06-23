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
    """Process a batch of URLs and return EPS values with improved error handling"""
    eps_values = []
    for i, url in enumerate(urls):
        try:
            eps_value = extract_eps_from_url(url)
            eps_values.append(eps_value)
            # Add small delay between requests to avoid overwhelming the server
            time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error processing URL {i+1}/{len(urls)} in batch: {url[:100]}... - {e}")
            eps_values.append(None)
            # Continue with next URL instead of failing entire batch
            continue
    return eps_values

def extract_eps_with_requests_html_chunked(grouped_df, chunk_size=50, max_workers=3, chunk_timeout=60):
    """
    Extract EPS data using requests_html with chunked processing to avoid getting stuck
    
    Args:
        grouped_df: DataFrame with 'xbrl' column containing URLs
        chunk_size: Number of batches to process at once (default: 50)
        max_workers: Maximum number of worker threads (default: 3, reduced further)
        chunk_timeout: Timeout for entire chunk processing in seconds (default: 60)
        
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
        
        # Process current chunk using ThreadPoolExecutor with timeout
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks for current chunk
                future_to_index = {
                    executor.submit(process_url_batch, urls): idx 
                    for urls, idx in zip(chunk_urls, chunk_indices)
                }
                
                # Process completed tasks with progress bar and timeout
                completed_count = 0
                for future in tqdm(concurrent.futures.as_completed(future_to_index, timeout=chunk_timeout), 
                                  total=len(future_to_index), 
                                  desc=f"Chunk {chunk_start//chunk_size + 1}"):
                    idx = future_to_index[future]
                    try:
                        eps_values = future.result(timeout=60)  # 60 second timeout per batch
                        grouped_df.at[idx, 'eps'] = eps_values
                        completed_count += 1
                    
                    except concurrent.futures.TimeoutError:
                        logger.error(f"Timeout processing batch at index {idx}")
                        grouped_df.at[idx, 'eps'] = []
                        # Cancel the future to free up resources
                        future.cancel()
                    
                    except Exception as e:
                        logger.error(f"Error processing batch at index {idx}: {e}")
                        grouped_df.at[idx, 'eps'] = []
                        # Cancel the future to free up resources
                        future.cancel()
                
                logger.info(f"Completed chunk {chunk_start//chunk_size + 1}: {completed_count}/{len(chunk_urls)} batches processed")
        
        except concurrent.futures.TimeoutError:
            logger.error(f"Chunk {chunk_start//chunk_size + 1} timed out after {chunk_timeout} seconds")
            # Mark remaining unprocessed batches as failed
            for idx in chunk_indices:
                if grouped_df.at[idx, 'eps'] == '':
                    grouped_df.at[idx, 'eps'] = []
        
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_start//chunk_size + 1}: {e}")
            # Mark remaining unprocessed batches as failed
            for idx in chunk_indices:
                if grouped_df.at[idx, 'eps'] == '':
                    grouped_df.at[idx, 'eps'] = []
    
    # Clean up thread-local sessions
    if hasattr(thread_local, "session"):
        try:
            thread_local.session.close()
        except:
            pass
    
    logger.info(f"Completed processing all chunks")
    return grouped_df

def check_processing_progress(grouped_df):
    """
    Check the progress of EPS extraction to see which batches have been processed
    
    Args:
        grouped_df: DataFrame with 'eps' column
        
    Returns:
        Dictionary with progress information
    """
    total_batches = len(grouped_df)
    processed_batches = len(grouped_df[grouped_df['eps'] != ''])
    failed_batches = len(grouped_df[grouped_df['eps'] == []])
    pending_batches = total_batches - processed_batches - failed_batches
    
    progress_info = {
        'total_batches': total_batches,
        'processed_batches': processed_batches,
        'failed_batches': failed_batches,
        'pending_batches': pending_batches,
        'completion_percentage': (processed_batches / total_batches * 100) if total_batches > 0 else 0
    }
    
    logger.info(f"Progress: {processed_batches}/{total_batches} batches processed ({progress_info['completion_percentage']:.1f}%)")
    logger.info(f"Failed: {failed_batches}, Pending: {pending_batches}")
    
    return progress_info

def resume_eps_extraction(grouped_df, max_workers=3, chunk_size=50):
    """
    Resume EPS extraction for batches that haven't been processed yet
    
    Args:
        grouped_df: DataFrame with 'eps' column (some may already be processed)
        max_workers: Maximum number of worker threads
        chunk_size: Number of batches to process at once
        
    Returns:
        DataFrame with completed 'eps' column
    """
    # Check current progress
    progress = check_processing_progress(grouped_df)
    
    if progress['pending_batches'] == 0:
        logger.info("All batches have been processed!")
        return grouped_df
    
    # Create a copy with only unprocessed batches
    unprocessed_df = grouped_df[grouped_df['eps'] == ''].copy()
    
    logger.info(f"Resuming processing for {len(unprocessed_df)} unprocessed batches")
    
    # Process only the unprocessed batches
    processed_unprocessed = extract_eps_with_requests_html_chunked(
        unprocessed_df, 
        chunk_size=chunk_size, 
        max_workers=max_workers
    )
    
    # Update the original DataFrame with results
    for idx in processed_unprocessed.index:
        original_idx = grouped_df.index[grouped_df.index == idx][0]
        grouped_df.at[original_idx, 'eps'] = processed_unprocessed.at[idx, 'eps']
    
    # Check final progress
    final_progress = check_processing_progress(grouped_df)
    
    return grouped_df

