# Asynchronous web scraper for fetching Python job postings from WeWorkRemotely and Jobspresso.
# It uses aiohttp for static HTML fetching and requests-html for dynamic JavaScript rendering.
# Results are deduplicated and saved to a CSV file.

import asyncio
import logging
import aiohttp
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession
from urllib.parse import urljoin, urlencode as urllib_urlencode # Renamed to avoid conflict
import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Callable # Added type hints
import json # Added for JSON output

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SCRAPER CONFIGURATION ---
# This dictionary centralizes all configuration for the scrapers.
# It includes global settings like request headers and default search terms,
# and a list of site-specific configurations under the "SITES" key.
SCRAPER_CONFIG = {
    "REQUEST_HEADERS": { # Standard headers to use for all HTTP requests
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    },
    "DEFAULT_SEARCH_TERM": "python", # Default search term to use if not specified by a site
    "INCREMENTAL_SCRAPING_ENABLED": True, # Flag to enable/disable incremental scraping
    "SITES": [
        {
            "name": "WeWorkRemotely",
            "base_url": "https://weworkremotely.com/remote-jobs/search",
            "search_term_param": "term", 
            "search_term_default": "python", 
            "page_param": "page", 
            "max_pages": 3, 
            "job_listing_selector": "section.jobs ul li", 
            "title_selector": "span.title", 
            "company_selector": "span.company", 
            "link_selector": "a[href*='/remote-jobs/']", # Selects 'a' tags whose href contains '/remote-jobs/'
            "link_attribute": "href", 
            "link_base_url": "https://weworkremotely.com", 
            "date_selector": "span.date", # Selector for the date posted
            "region_selector": "span.region", # Selector for the region
            "ad_detection_rules": [
                {"type": "class", "value": "ad"}, 
                {"type": "text", "value": "sponsored"} 
            ],
            "enabled": True 
        },
        {
            "name": "Jobspresso",
            "base_url": "https://jobspresso.co/jobs/",
            "search_term_param": "search_keywords",
            "search_term_default": "python",
            "page_param": None, # Jobspresso search seems to be single-page or JS-paginated without URL param
            "max_pages": 1, 
            "job_listing_selector": "div.job-listing", 
            "title_selector": "h3.job-listing__title", 
            "company_selector": "span.job-listing__company", 
            "link_selector": "a.job-listing__title-link", 
            "link_attribute": "href",
            "link_base_url": None, # Jobspresso links are absolute
            "date_selector": "span.job-listing__date", # Selector for the date posted
            "region_selector": None, # Region is not consistently available or easily selectable
            "ad_detection_rules": [ # Generic rules, can be refined if Jobspresso has specific ad patterns
                {"type": "class", "value": "ad"},
                {"type": "text", "value": "sponsored"}
            ],
            "enabled": True
        }
    ]
}
# --- END SCRAPER CONFIGURATION ---

SEEN_JOBS_FILE = "seen_jobs.txt" # File to store URLs of already seen jobs

# --- Helper Functions for Incremental Scraping ---
def load_seen_job_urls(filename: str) -> set:
    seen_urls = set()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                seen_urls.add(line.strip())
        logging.info(f"Loaded {len(seen_urls)} seen job URLs from {filename}")
    except FileNotFoundError:
        logging.info(f"{filename} not found. Will be created on first successful run with new jobs.")
    except Exception as e:
        logging.error(f"Error loading seen job URLs from {filename}: {e}")
    return seen_urls

def save_job_urls(filename: str, job_urls: set):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            for url in job_urls:
                f.write(url + "\n")
        logging.info(f"Saved {len(job_urls)} job URLs to {filename}")
    except Exception as e:
        logging.error(f"Error saving job URLs to {filename}: {e}")

# --- END Helper Functions for Incremental Scraping ---


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

# Function to identify ad containers based on dynamic rules
def is_ad_container(element, rules: List[Dict]) -> bool:
    """
    Checks if a given BeautifulSoup element is likely an advertisement container
    based on a list of rules.
    Each rule in the list is a dictionary specifying a "type" (e.g., "class", "text")
    and a "value" to check for.

    Args:
        element: The BeautifulSoup element to check.
        rules: A list of dictionaries, where each dictionary is an ad detection rule.
               Example: [{"type": "class", "value": "ad"}, {"type": "text", "value": "sponsored"}]

    Returns:
        True if the element matches any of the ad detection rules, False otherwise.
    """
    if not rules:
        return False
        
    for rule in rules:
        rule_type = rule.get("type")
        rule_value = rule.get("value")
        if not rule_type or not rule_value:
            logging.warning(f"Skipping invalid ad detection rule: {rule}")
            continue

        if rule_type == "class":
            classes = element.get("class", [])
            if any(rule_value.lower() in cls.lower() for cls in classes):
                logging.debug(f"Ad detected by class rule '{rule_value}' in element: {element.name} with classes {classes}")
                return True
        elif rule_type == "text":
            if element.find(string=lambda t: t and rule_value.lower() in t.lower()):
                logging.debug(f"Ad detected by text rule '{rule_value}' in element: {element.name}")
                return True
        else:
            logging.warning(f"Unknown ad detection rule type: {rule_type}")
            
    return False

async def scrape_site(
    site_config: Dict,
    search_term: str,
    static_session: aiohttp.ClientSession,
    dynamic_session_factory: Callable
) -> List[Dict]:
    """
    Generic function to scrape job listings from a single site based on its configuration.

    Args:
        site_config: Configuration dictionary for the site.
        search_term: The job search term.
        static_session: aiohttp session for static requests.
        dynamic_session_factory: Callable that returns an AsyncHTMLSession.

    Returns:
        A list of unique job dictionaries found on the site.
    """
    all_jobs_for_site = []
    request_headers = SCRAPER_CONFIG["REQUEST_HEADERS"]
    base_url = site_config["base_url"]

    for page_num in range(1, site_config["max_pages"] + 1):
        params = {site_config["search_term_param"]: search_term}
        if site_config.get("page_param") and page_num > 1: # Add page param only if it exists and page > 1
            params[site_config["page_param"]] = page_num
        
        query_string = urllib_urlencode(params)
        # urljoin can handle if base_url already has query params, but it's cleaner if not.
        # Assuming base_url in config is clean (no trailing ? or params)
        page_url = f"{base_url}?{query_string}" if query_string else base_url 
        # If base_url for search doesn't use query params (e.g. https://site.com/search/term/page/1)
        # this URL construction will need to be more flexible based on config.
        # For now, assuming query parameter based search and pagination.

        logging.info(f"Scraping {site_config['name']}: {search_term} - Page {page_num} from {page_url}")

        html_content = await fetch_page_content(
            page_url, static_session, dynamic_session_factory, request_headers, 
            initial_fetch_type=site_config.get("initial_fetch_type", "static"), # Allow config override
            force_dynamic=site_config.get("force_dynamic", False)
        )

        if not html_content:
            logging.warning(f"No HTML content for {page_url} on {site_config['name']}. Skipping page.")
            continue

        soup = BeautifulSoup(html_content, "html.parser")
        job_listings_elements = soup.select(site_config["job_listing_selector"])
        logging.info(f"Found {len(job_listings_elements)} potential job listings on {page_url}.")

        if not job_listings_elements and site_config["max_pages"] > 1 : # If no jobs on a results page, likely end of results for this site
             logging.info(f"No job listings found on {page_url} for {site_config['name']}. This might be the last page of results.")
             break


        for job_elem in job_listings_elements:
            try:
                if is_ad_container(job_elem, site_config.get("ad_detection_rules", [])):
                    logging.info(f"Skipping ad listing on {page_url} for site {site_config['name']}.")
                    continue

                title_elem = job_elem.select_one(site_config["title_selector"])
                title = title_elem.get_text(strip=True) if title_elem else "N/A"

                company_elem = job_elem.select_one(site_config["company_selector"])
                company = company_elem.get_text(strip=True) if company_elem else "N/A"

                link_elem = job_elem.select_one(site_config["link_selector"])
                relative_url = link_elem.get(site_config["link_attribute"]) if link_elem else None
                
                if not relative_url:
                    logging.warning(f"No link found for a job on {site_config['name']} using selector '{site_config['link_selector']}'. Skipping.")
                    continue
                
                # Construct absolute URL. Use site's base_url if link_base_url is not specified.
                effective_link_base = site_config.get("link_base_url") or base_url
                absolute_url = urljoin(effective_link_base, relative_url)

                date_elem = job_elem.select_one(site_config.get("date_selector", "")) # Handle if not defined
                date_posted = date_elem.get_text(strip=True) if date_elem else "N/A"
                
                region_elem = job_elem.select_one(site_config.get("region_selector", "")) # Handle if not defined
                region = region_elem.get_text(strip=True) if region_elem else "N/A"

                if title == "N/A" and company == "N/A": # Skip if essential info is missing
                    logging.debug(f"Skipping job on {site_config['name']} due to missing title and company. URL: {absolute_url}")
                    continue

                all_jobs_for_site.append({
                    "source": site_config["name"],
                    "title": title,
                    "company": company,
                    "url": absolute_url,
                    "date_posted": date_posted,
                    "region": region
                })
            except Exception as e:
                logging.error(f"Error parsing a job listing for {site_config['name']} on {page_url}: {e}", exc_info=True)
    
    # Deduplication for the current site
    if all_jobs_for_site:
        logging.info(f"Deduplicating {len(all_jobs_for_site)} jobs from {site_config['name']} based on URL.")
        unique_jobs = {job["url"]: job for job in all_jobs_for_site}
        all_jobs_for_site = list(unique_jobs.values())
        logging.info(f"Returning {len(all_jobs_for_site)} unique jobs from {site_config['name']}.")
        
    return all_jobs_for_site

async def main():
    """
    Main function to orchestrate the scraping process.
    It initializes an aiohttp session, iterates through configured sites,
    calls the generic scrape_site function for each, combines the results,
    filters for new jobs if incremental scraping is enabled,
    performs global deduplication, and saves the data to CSV and JSON files.
    """
    dynamic_session_factory = get_dynamic_session
    all_scraped_jobs = []
    
    seen_job_urls = set()
    incremental_enabled = SCRAPER_CONFIG.get("INCREMENTAL_SCRAPING_ENABLED", False)

    if incremental_enabled:
        seen_job_urls = load_seen_job_urls(SEEN_JOBS_FILE)

    async with aiohttp.ClientSession() as static_session:
        for site_conf in SCRAPER_CONFIG["SITES"]:
            if not site_conf.get("enabled", True):
                logging.info(f"Skipping {site_conf['name']} as it is disabled in the configuration.")
                continue

            logging.info(f"Starting to scrape {site_conf['name']}...")
            # Determine search term: site-specific default, then global default, then fallback "python"
            search_term = site_conf.get("search_term_default", SCRAPER_CONFIG.get("DEFAULT_SEARCH_TERM", "python"))
            
            try:
                jobs_from_site = await scrape_site(
                    site_conf,
                    search_term,
                    static_session,
                    dynamic_session_factory
                )
                logging.info(f"Found {len(jobs_from_site)} jobs from {site_conf['name']}.")
                all_scraped_jobs.extend(jobs_from_site)
            except Exception as e:
                logging.error(f"Error scraping site {site_conf['name']}: {e}", exc_info=True)
    
    logging.info(f"Total jobs scraped from all sites before any filtering: {len(all_scraped_jobs)}")

    if incremental_enabled:
        current_run_job_urls = {job['url'] for job in all_scraped_jobs if job.get('url')}
        new_jobs = [job for job in all_scraped_jobs if job.get('url') and job['url'] not in seen_job_urls]
        
        logging.info(f"Previously seen jobs: {len(seen_job_urls)}")
        logging.info(f"Jobs scraped in this run: {len(current_run_job_urls)}")
        logging.info(f"New jobs found: {len(new_jobs)}")
        
        all_scraped_jobs = new_jobs # Process only new jobs further
        
        updated_seen_urls = seen_job_urls.union(current_run_job_urls)
        save_job_urls(SEEN_JOBS_FILE, updated_seen_urls)
    else:
        logging.info("Incremental scraping is disabled. Processing all scraped jobs.")

    if all_scraped_jobs: # This will now be true only if there are new jobs (if incremental) or any jobs (if not incremental)
        # Create a Pandas DataFrame for easier data manipulation and CSV export.
        df = pd.DataFrame(all_scraped_jobs)
        
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
        # Note: Site-specific URL deduplication is already done in `scrape_site`.
        df.drop_duplicates(subset=['title', 'company'], keep='first', inplace=True) # Global deduplication on remaining jobs

        # --- Save to CSV ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"job_listings_{timestamp}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        logging.info(f"Saved {len(df)} unique job listings to {csv_filename} after global deduplication.")

        # --- Save to JSON file ---
        final_jobs_for_json = df.to_dict(orient='records')
        json_output_filename = "jobs.json" # Static filename as per prompt
        try:
            with open(json_output_filename, "w", encoding="utf-8") as f:
                json.dump(final_jobs_for_json, f, ensure_ascii=False, indent=4)
            logging.info(f"Successfully saved {len(final_jobs_for_json)} jobs to {json_output_filename}")
        except IOError as e:
            logging.error(f"Error saving jobs to JSON file {json_output_filename}: {e}")
        except TypeError as e:
            logging.error(f"TypeError while serializing jobs to JSON (check data types): {e}")

    else:
        if incremental_enabled:
            logging.info("No new job listings to save.")
        else:
            logging.info("No job listings were scraped from any site.")

# Standard Python idiom: defines the entry point for when the script is executed directly.
if __name__ == "__main__":
    # Runs the main asynchronous function using asyncio.run().
    asyncio.run(main())
