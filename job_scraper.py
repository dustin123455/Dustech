# Asynchronous web scraper for fetching Python job postings from WeWorkRemotely and Jobspresso.
# It uses aiohttp for static HTML fetching and requests-html for dynamic JavaScript rendering.
# Results are deduplicated and saved to a CSV file.

import asyncio
import logging
import aiohttp
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession
from urllib.parse import urljoin
import pandas as pd
import re
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SCRAPER CONFIGURATION ---
# This dictionary centralizes all configuration for the scrapers,
# including base URLs, search terms, request headers, and pagination limits.
SCRAPER_CONFIG = {
    "WEWORKREMOTELY_BASE_URL": "https://weworkremotely.com/remote-jobs/search", # Base URL for WWR job searches
    "WEWORKREMOTELY_SEARCH_TERM": "python", # Search term for WWR
    "WEWORKREMOTELY_MAX_PAGES": 3, # Max number of pages to scrape from WWR for the search term
    "JOBPRESSO_BASE_URL": "https://jobspresso.co/jobs/", # Base URL for Jobspresso job searches
    "JOBPRESSO_SEARCH_TERM": "python", # Search term for Jobspresso
    "REQUEST_HEADERS": { # Standard headers to use for all HTTP requests
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
}
# --- END SCRAPER CONFIGURATION ---

# Helper function for dynamic sessions
def get_dynamic_session():
    """
    Factory function to create and return a new AsyncHTMLSession for dynamic content rendering.
    This helps in managing session lifecycles, especially if sessions need specific setup
    or if a new session is desired for each dynamic fetch attempt.
    """
    return AsyncHTMLSession()

async def _fetch_static_internal(url, session, headers):
    """
    Performs a raw asynchronous GET request to fetch static HTML content using aiohttp.
    This function is intended for fetching pages that do not require JavaScript rendering.
    It raises an exception on HTTP errors, which is handled by the caller.
    """
    logging.info(f"Attempting to fetch {url} statically.")
    try:
        async with session.get(url, headers=headers, timeout=20) as response: # Increased timeout
            response.raise_for_status()
            html_content = await response.text()
            logging.info(f"Successfully fetched {url} statically, content length: {len(html_content)}.")
            return html_content
    except Exception as e:
        logging.error(f"Error fetching {url} statically: {e}")
        raise

async def _dynamic_fetch_html_internal(url, session_factory, headers):
    """
    Asynchronously fetches HTML content from a URL using requests-html for dynamic content.
    Renamed from dynamic_fetch_html and adapted to use a session factory. Now accepts headers.
    This function handles JavaScript rendering using requests-html.
    Returns an empty string if fetching or rendering fails.
    """
    logging.info(f"Attempting to fetch {url} dynamically.")
    session = session_factory()
    try:
        r = await session.get(url, headers=headers, timeout=30)
        await r.html.arender(timeout=60, sleep=3, keep_page=True) # Increased timeout and sleep
        html_content = r.html.raw_html.decode('utf-8')
        logging.info(f"Successfully fetched {url} dynamically, content length: {len(html_content)}.")
        return html_content
    except Exception as e:
        logging.error(f"Error fetching {url} dynamically: {e}")
        return ""
    finally:
        await session.close()

async def fetch_page_content(url: str, static_session: aiohttp.ClientSession, dynamic_session_factory, headers: dict, *, initial_fetch_type: str = 'static', force_dynamic: bool = False) -> str:
    """
    Fetches page content from a given URL. It can try static fetching first and fall back
    to dynamic fetching, or vice-versa, or force a specific method.

    Args:
        url (str): The URL to fetch content from.
        static_session (aiohttp.ClientSession): An active aiohttp session for static requests.
        dynamic_session_factory (callable): A function that returns a new AsyncHTMLSession.
        headers (dict): HTTP headers to use for the request.
        initial_fetch_type (str, optional): 'static' or 'dynamic'. Determines the first method tried.
                                           Defaults to 'static'.
        force_dynamic (bool, optional): If True, only dynamic fetching is attempted. Defaults to False.

    Returns:
        str: The HTML content of the page, or an empty string if all attempts fail.
    """
    html_content = ""

    if force_dynamic:
        # If dynamic fetching is forced, only attempt that.
        logging.info(f"Forcing dynamic fetch for {url}.")
        html_content = await _dynamic_fetch_html_internal(url, dynamic_session_factory, headers)
        if not html_content:
            logging.warning(f"Forced dynamic fetch failed for {url}. No fallback.")
        return html_content

    if initial_fetch_type == 'dynamic':
        # Try dynamic fetching first.
        logging.info(f"Initial fetch attempt: dynamic for {url}.")
        html_content = await _dynamic_fetch_html_internal(url, dynamic_session_factory, headers)
        if html_content:
            return html_content
        else:
            # Fallback to static if dynamic fails.
            logging.warning(f"Dynamic fetch failed for {url}. Falling back to static.")
            try:
                html_content = await _fetch_static_internal(url, static_session, headers)
                return html_content
            except Exception as e:
                logging.error(f"Static fallback failed for {url} after dynamic attempt: {e}")
                return "" # Return empty if static fallback also fails.

    elif initial_fetch_type == 'static':
        # Try static fetching first (default behavior).
        logging.info(f"Initial fetch attempt: static for {url}.")
        try:
            html_content = await _fetch_static_internal(url, static_session, headers)
            return html_content
        except Exception as e:
            # Fallback to dynamic if static fails.
            logging.warning(f"Static fetch failed for {url}: {e}. Falling back to dynamic.")
            html_content = await _dynamic_fetch_html_internal(url, dynamic_session_factory, headers)
            if html_content:
                return html_content
            else:
                logging.error(f"Dynamic fallback failed for {url} after static attempt.")
                return "" # Return empty if dynamic fallback also fails.
    
    # If none of the above strategies succeeded.
    logging.error(f"All fetch attempts failed for {url}.")
    return ""

# Function to identify ad containers based on class or text content
def is_ad_container(element) -> bool:
    '''
    Checks if a given BeautifulSoup element is likely an advertisement container.
    It checks for common keywords like "ad" in class names or "sponsored" in the text.
    '''
    # Check for 'ad' in class names
    classes = element.get("class", [])
    if any("ad" in cls.lower() for cls in classes):
        logging.debug(f"Ad detected by class in element: {element.name} with classes {classes}")
        return True
    # Check for 'sponsored' in text content (recursively)
    if element.find(string=lambda t: t and "sponsored" in t.lower()):
        logging.debug(f"Ad detected by text 'sponsored' in element: {element.name}")
        return True
    return False

async def scrape_weworkremotely(static_session):
    """
    Scrapes job listings for a configured search term from We Work Remotely.

    Flow:
    1. Constructs search URLs for each page up to `WEWORKREMOTELY_MAX_PAGES`.
    2. Fetches page content using `fetch_page_content` (static first, then dynamic fallback).
    3. Parses HTML using BeautifulSoup.
    4. Extracts job details (title, company, URL, date, region) from `<li>` elements.
       - Uses specific CSS selectors like 'span.title', 'span.company'.
       - Job URLs are resolved to be absolute.
    5. If initial fetch yields no listings, it attempts a `force_dynamic` fetch for the page.
    6. Appends extracted job data to a list.
    7. No site-specific deduplication is performed here; it's handled globally in `main`.
    """
    base_url = SCRAPER_CONFIG["WEWORKREMOTELY_BASE_URL"]
    search_term = SCRAPER_CONFIG["WEWORKREMOTELY_SEARCH_TERM"]
    max_pages = SCRAPER_CONFIG["WEWORKREMOTELY_MAX_PAGES"]
    request_headers = SCRAPER_CONFIG["REQUEST_HEADERS"]
    all_jobs = []

    for page_num in range(1, max_pages + 1):
        page_url = f"{base_url}?term={search_term}&page={page_num}"
        logging.info(f"Scraping WeWorkRemotely: {search_term} - Page {page_num} from {page_url}")
        
        html_content = await fetch_page_content(page_url, static_session, get_dynamic_session, request_headers, initial_fetch_type='static')

        if not html_content: # Enhanced error handling
            logging.warning(f"No HTML content fetched for URL: {page_url}. Skipping page.")
            continue

        soup = BeautifulSoup(html_content, 'html.parser')
    # Adjusted selector based on typical WWR search result structure.
    # Jobs are usually in `<li>` elements within a `<ul>` inside a `<section class="jobs">`.
        job_listings = soup.select('section.jobs ul li') 

        if not job_listings: 
        # If static fetch (or its initial dynamic fallback) didn't find listings,
        # try forcing a dynamic fetch, as content might be JS-rendered.
            logging.info(f"No job listings found for {page_url} with initial strategy. Trying with force_dynamic=True.")
            html_content = await fetch_page_content(page_url, static_session, get_dynamic_session, request_headers, force_dynamic=True)
            if html_content:
                soup = BeautifulSoup(html_content, 'html.parser')
                job_listings = soup.select('section.jobs ul li') # Re-select after dynamic fetch
            elif not job_listings: 
                logging.warning(f"force_dynamic fetch also failed for {page_url} or returned no listings. Skipping page.")
                continue
        
    if not job_listings: 
        # If still no listings after trying dynamic, it might be the last page of results or an empty search.
            logging.info(f"No job listings found on page {page_num} for {search_term} even after dynamic fetch. This might be the last page.")
        break # Exit pagination loop if a page is genuinely empty.

        logging.info(f"Found {len(job_listings)} job listings on page {page_num} for {search_term}.")

        for job in job_listings:
            # Skip if the listing is identified as an ad/sponsored content
            if is_ad_container(job):
                logging.info(f"Skipping a job listing on {page_url} because it was identified as an ad.")
                continue

            title_element = job.find('span', class_='title')
            company_element = job.find('span', class_='company')
            date_element = job.find('span', class_='date')
            region_element = job.find('span', class_='region')
            # WWR search result links are usually direct 'a' tags with href within the 'li'
            job_url_element = job.find('a', href=re.compile(r'/remote-jobs/'))

            if not (title_element and company_element and job_url_element and job_url_element.get('href')):
                title_text = title_element.text.strip() if title_element else "None"
                company_text = company_element.text.strip() if company_element else "None"
                link_href = job_url_element.get('href') if job_url_element else "None"
                logging.warning(f"Missing essential data for a job listing on {page_url}. Title: {title_text}, Company: {company_text}, Link: {link_href}. Skipping listing.")
                continue

            title = title_element.text.strip()
            company = company_element.text.strip()
    # Base URL for WWR job links is "https://weworkremotely.com", so urljoin is used for relative links.
            job_url = urljoin("https://weworkremotely.com", job_url_element['href'])
            date_posted = date_element.text.strip() if date_element else "N/A"
            region = region_element.text.strip() if region_element else "N/A"
            
            all_jobs.append({
                'title': title,
                'company': company,
                'url': job_url,
                'source': 'WeWorkRemotely',
                'date_posted': date_posted,
                'region': region
            })
    
    logging.info(f"Total jobs scraped from WeWorkRemotely for '{search_term}': {len(all_jobs)}")
    return all_jobs

async def scrape_jobspresso(static_session):
    """
    Scrapes job listings for a configured search term from Jobspresso.

    Flow:
    1. Constructs the search URL using the base URL and search term.
    2. Fetches page content using `fetch_page_content` (static first, dynamic fallback).
    3. Parses HTML using BeautifulSoup.
    4. Extracts job details (title, company, URL, date) from `div.job-listing` elements.
       - Uses specific CSS selectors like 'h3.job-listing__title'.
       - Jobspresso links are typically absolute.
    5. If initial fetch yields no listings, it attempts a `force_dynamic` fetch.
    6. Appends extracted job data to a list.
    7. Performs deduplication of jobs based on the job URL before returning.
    """
    base_url = SCRAPER_CONFIG["JOBPRESSO_BASE_URL"]
    search_term = SCRAPER_CONFIG["JOBPRESSO_SEARCH_TERM"]
    request_headers = SCRAPER_CONFIG["REQUEST_HEADERS"]
    # Construct the URL for Jobspresso. Note: Jobspresso's search might be different,
    # this is a common pattern. Adjust if their URL structure is different.
    # Example: https://jobspresso.co/jobs/?search_keywords=python
    # Or: https://jobspresso.co/remote-work/python/ (if it uses path segments for search)
    # For this example, I'll use the query parameter style.
    url = f"{base_url}?search_keywords={search_term}" 
    # If Jobspresso uses a different structure like /remote-work/ for all jobs and then filters, 
    # the original URL might be better: "https://jobspresso.co/remote-work/" and then hope the search term is a filter on that page
    # For now, sticking to the provided structure with search term in query.
    # Let's assume the task meant to use the search term for Jobspresso as well.
    # Original URL from previous version: "https://jobspresso.co/remote-work/"
    # Let's try to keep previous behavior if search term is empty, or use new one.
    # The config has "JOBPRESSO_BASE_URL": "https://jobspresso.co/jobs/", which implies search.
    
    all_jobs = []
    
    logging.info(f"Scraping Jobspresso from {url} for term '{search_term}'")
    
    html_content = await fetch_page_content(url, static_session, get_dynamic_session, request_headers, initial_fetch_type='static')

    if not html_content: # Enhanced error handling
        logging.warning(f"No HTML content fetched for URL: {url}. Skipping page.")
        return all_jobs

    soup = BeautifulSoup(html_content, 'html.parser')
    job_listings = soup.find_all('div', class_='job-listing') # Standard container for Jobspresso listings.

    if not job_listings:
        # If static fetch (or its initial dynamic fallback) didn't find listings,
        # try forcing a dynamic fetch, as content might be JS-rendered.
        logging.info(f"No job listings found for {url} with initial strategy. Trying with force_dynamic=True.")
        html_content = await fetch_page_content(url, static_session, get_dynamic_session, request_headers, force_dynamic=True)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            job_listings = soup.find_all('div', class_='job-listing')
        elif not job_listings: 
            logging.warning(f"force_dynamic fetch also failed for {url} or returned no listings. Skipping Jobspresso.")
            return all_jobs

    logging.info(f"Found {len(job_listings)} job listings on Jobspresso for '{search_term}'.")

    for job in job_listings:
        title_element = job.find('h3', class_='job-listing__title')
        company_element = job.find('span', class_='job-listing__company')
        job_url_element = job.find('a', class_='job-listing__title-link') 
        date_posted_element = job.find('span', class_='job-listing__date') 

        if not (title_element and company_element and job_url_element and job_url_element.get('href')):
            title_text = title_element.text.strip() if title_element else "None"
            company_text = company_element.text.strip() if company_element else "None"
            link_href = job_url_element.get('href') if job_url_element else "None"
            logging.warning(f"Missing essential data for a job listing on {url} (term: {search_term}). Title: {title_text}, Company: {company_text}, Link: {link_href}. Skipping listing.")
            continue
            
        title = title_element.text.strip()
        company = company_element.text.strip()
        job_url = job_url_element['href'] # Jobspresso links are usually absolute
        date_posted = date_posted_element.text.strip() if date_posted_element else "N/A"
        
        all_jobs.append({
            'title': title,
            'company': company,
            'url': job_url,
            'source': 'Jobspresso',
            'date_posted': date_posted,
            'region': "N/A" # Jobspresso doesn't always specify region easily
        })
            
    logging.info(f"Total jobs scraped from Jobspresso for '{search_term}': {len(all_jobs)}")
    
    if all_jobs: 
        # Deduplicate jobs from Jobspresso based on their URL before returning.
        # This is a site-specific deduplication step.
        logging.info(f"Deduplicating {len(all_jobs)} jobs from Jobspresso based on URL.")
        unique_jobs = {job["url"]: job for job in all_jobs}
        all_jobs = list(unique_jobs.values())
        logging.info(f"Returning {len(all_jobs)} unique jobs from Jobspresso.")
    return all_jobs

async def main():
    """
    Main function to orchestrate the scraping process.
    It initializes an aiohttp session, calls the scraper functions for each site,
    combines the results, performs global deduplication, and saves the data to a CSV file.
    """
    # Create a single aiohttp session to be reused for all static requests.
    # Headers are now sourced from SCRAPER_CONFIG within individual scraper functions.
    async with aiohttp.ClientSession() as static_session: 
        # Run scrapers concurrently.
        weworkremotely_jobs = await scrape_weworkremotely(static_session)
        jobspresso_jobs = await scrape_jobspresso(static_session)

    # Combine results from all scrapers.
    all_jobs = weworkremotely_jobs + jobspresso_jobs
    
    if all_jobs:
        # Create a Pandas DataFrame for easier data manipulation and CSV export.
        df = pd.DataFrame(all_jobs)
        
        # --- Data cleaning and normalization ---
        df.replace('N/A', pd.NA, inplace=True) # Standardize N/A values to Pandas NA.
        df['date_posted'] = df['date_posted'].astype(str) # Ensure date_posted is string type.

        # Placeholder for a more robust date parsing function if needed.
        # Currently, it just returns the string; actual conversion would require more complex logic.
        def convert_relative_date(date_str):
            if not date_str or pd.isna(date_str):
                return None
            # Example: Implement parsing for "1d ago", "2w ago", etc.
            return date_str 

        df['parsed_date'] = df['date_posted'].apply(convert_relative_date)
        
        # Global deduplication: Remove duplicates based on job title and company name,
        # keeping the first occurrence. This helps if the same job is posted on multiple sites
        # or if site-specific deduplication was not exhaustive.
        df.drop_duplicates(subset=['title', 'company'], keep='first', inplace=True)

        # --- Save to CSV ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"job_listings_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        logging.info(f"Saved {len(df)} unique job listings to {filename}")
    else:
        logging.info("No job listings were scraped.")

# Standard Python idiom: defines the entry point for when the script is executed directly.
if __name__ == "__main__":
    # Runs the main asynchronous function using asyncio.run().
    asyncio.run(main())
