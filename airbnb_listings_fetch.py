# %%
import requests
import json
import os
from dotenv import load_dotenv
import time
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd

# Load environment variables
load_dotenv()

brightdata_api_key = os.getenv("BRIGHTDATA_API_KEY")
if not brightdata_api_key:
    raise ValueError("BRIGHTDATA_API_KEY environment variable is required. Please check your .env file.")

# %%
def get_brightdata_snapshot_by_location(location: str, limit_per_input: int, api_key: str) -> str:
    """
    Triggers a BrightData Airbnb dataset scrape by location.
    
    Parameters
    ----------
    location : str
        The location to search for Airbnb listings (e.g., "Beltline, Calgary").
    limit_per_input : int
        Maximum number of listings to retrieve per location.
    api_key : str
        BrightData API key.
    
    Returns
    -------
    str
        The snapshot ID for retrieving results.
    """
    api_url = "https://api.brightdata.com/datasets/v3/trigger"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    params = {
        "dataset_id": "gd_ld7ll037kqy322v05",
        "include_errors": "false",
        "type": "discover_new",
        "discover_by": "location",
        "limit_per_input": str(limit_per_input),
    }
    data = {
        "input": [{"location": location, "currency": "CAD", "country": "CA", "num_of_infants": ""}],
        "custom_output_fields": [
            "name",
            "price",
            # "image",
            "description",
            "category",
            "availability",
            "discount",
            "reviews",
            "ratings",
            # "seller_info",
            # "breadcrumbs",
            "location",
            "lat",
            "long",
            "guests",
            "pets_allowed",
            "description_items",
            "category_rating",
            "house_rules",
            "details",
            "highlights",
            "arrangement_details",
            "amenities",
            # "images",
            # "available_dates",
            "url",
            # "final_url",
            "listing_title",
            "property_id",
            "listing_name",
            "location_details",
            "description_by_sections",
            # "description_html",
            # "location_details_html",
            "is_supperhost",
            "host_number_of_reviews",
            "host_rating",
            "hosts_year",
            "host_response_rate",
            "is_guest_favorite",
            "travel_details",
            "pricing_details",
            "total_price",
            "currency",
            "cancellation_policy",
            "property_number_of_reviews",
            # "country",
            # "postcode_map_url",
            # "host_image",
            "host_details",
            "reviews_details",
            "timestamp",
            # "input",
            # "discovery_input",
            # "error",
            # "error_code",
            # "warning",
            # "warning_code"
        ],
    }

    response = requests.post(api_url, headers=headers, params=params, json=data)
    response.raise_for_status()
    response_json = response.json()
    return response_json['snapshot_id']

# %%
def get_brightdata_snapshot_by_url(url: str, api_key: str, country: str = "CA") -> str:
    """
    Triggers a BrightData Airbnb dataset scrape by listing URL.
    
    Parameters
    ----------
    url : str
        The Airbnb listing URL to scrape (e.g., "https://www.airbnb.ca/rooms/1300059188064308611").
    api_key : str
        BrightData API key.
    country : str, optional
        Country code (default: "CA").
    
    Returns
    -------
    str
        The snapshot ID for retrieving results.
    """
    api_url = "https://api.brightdata.com/datasets/v3/trigger"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    params = {
        "dataset_id": "gd_ld7ll037kqy322v05",
    }
    data = {
        "input": [{"url": url, "country": country}],
        "custom_output_fields": [
            "name",
            "price",
            # "image",
            "description",
            "category",
            "availability",
            "discount",
            "reviews",
            "ratings",
            # "seller_info",
            # "breadcrumbs",
            "location",
            "lat",
            "long",
            "guests",
            "pets_allowed",
            "description_items",
            "category_rating",
            "house_rules",
            "details",
            "highlights",
            "arrangement_details",
            "amenities",
            # "images",
            # "available_dates",
            "url",
            # "final_url",
            "listing_title",
            "property_id",
            "listing_name",
            "location_details",
            "description_by_sections",
            # "description_html",
            # "location_details_html",
            "is_supperhost",
            "host_number_of_reviews",
            "host_rating",
            "hosts_year",
            "host_response_rate",
            "is_guest_favorite",
            "travel_details",
            "pricing_details",
            "total_price",
            "currency",
            "cancellation_policy",
            "property_number_of_reviews",
            # "country",
            # "postcode_map_url",
            # "host_image",
            "host_details",
            "reviews_details",
            "timestamp",
            # "input",
            # "discovery_input",
            # "error",
            # "error_code",
            # "warning",
            # "warning_code"
        ],
    }

    response = requests.post(api_url, headers=headers, params=params, json=data)
    response.raise_for_status()
    response_json = response.json()
    return response_json['snapshot_id']

# %%
def get_snapshot_output(snapshot_id: str, api_key: str, max_retries: int = 30, wait_time: int = 30) -> dict:
    """
    Retrieves snapshot output from Bright Data API with automatic retry logic.
    
    Parameters
    ----------
    snapshot_id : str
        The snapshot ID returned from triggering the dataset.
    api_key : str
        Bright Data API key.
    max_retries : int, optional
        Maximum number of retry attempts (default: 30).
    wait_time : int, optional
        Wait time in seconds between retries (default: 30).

    Returns
    -------
    dict
        The extracted Airbnb listings data once ready.
        
    Raises
    ------
    TimeoutError
        If snapshot is not ready after max_retries.
    requests.RequestException
        If API request fails.
    """
    url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    params = {
        "format": "json"
    }
    
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries}: Checking snapshot status...")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            data = response.json()

            # Status values that indicate the snapshot is still processing
            processing_statuses = {"building", "running", "pending", "queued", "STATUS"}
            
            # Check if snapshot is still running
            if isinstance(data, dict) and data.get("status") in processing_statuses:
                print(f"Snapshot still processing (status: {data.get('status')}). Waiting {wait_time} seconds...")
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    time.sleep(wait_time)
                continue
            
            # Data is ready - return the parsed response
            print("Snapshot ready! Data retrieved successfully.")
            return data

        except requests.RequestException as e:
            print(f"API request failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(wait_time)
                continue
            raise
    
    # If we've exhausted all retries
    raise TimeoutError(f"Snapshot {snapshot_id} was not ready after {max_retries} attempts ({max_retries * wait_time} seconds)")

# %%
def extract_airbnb_listings(json_output: Any) -> Tuple[List[Dict], pd.DataFrame]:
    """
    Extracts Airbnb listings from the BrightData API snapshot output.
    
    Parameters
    ----------
    json_output : str, dict, or list
        The JSON output from the Bright Data API, either as a string, dictionary, or list.
    
    Returns
    -------
    Tuple[List[Dict], pd.DataFrame]
        A tuple containing:
        - List of dictionaries with all listing data
        - DataFrame with all listings (one row per listing)
    """
    # If data is a JSON string, parse it into a Python object
    if isinstance(json_output, str):
        data = json.loads(json_output)
    else:
        data = json_output

    # Ensure data is a list
    if isinstance(data, dict):
        # If it's a single listing, wrap it in a list
        if 'property_id' in data:
            data = [data]
        # If it's a status response, it means data is not ready yet
        elif 'status' in data:
            raise ValueError(f"Data is not ready yet. Status: {data.get('status')}")
        else:
            # Try to extract listings from possible nested structure
            data = data.get('data', data)
            if not isinstance(data, list):
                raise ValueError(f"Unexpected data structure: {type(data)}")
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    print(f"Successfully extracted {len(data)} Airbnb listings")
    
    return data, df

# %%
def fetch_airbnb_listings_by_location(
    location: str, 
    limit_per_input: int = 100,
    api_key: Optional[str] = None,
    max_retries: int = 240, 
    wait_time: int = 30
) -> Tuple[str, List[Dict], pd.DataFrame]:
    """
    Orchestrates the full Airbnb listings scraping process by location using Bright Data API.
    
    This function triggers a BrightData scrape for Airbnb listings by location,
    waits for the snapshot to complete (typically 10-30 minutes), and returns the data.
    
    Parameters
    ----------
    location : str
        The location to search for Airbnb listings (e.g., "Beltline, Calgary").
    limit_per_input : int, optional
        Maximum number of listings to retrieve (default: 100).
    api_key : str, optional
        BrightData API key. If not provided, will use BRIGHTDATA_API_KEY from .env file.
    max_retries : int, optional
        Maximum retries for snapshot polling (default: 240, allows for 2 hours with 30s intervals).
    wait_time : int, optional
        Wait time in seconds between retries (default: 30).
    
    Returns
    -------
    Tuple[str, List[Dict], pd.DataFrame]
        A tuple containing:
        - Snapshot ID (str)
        - List of dictionaries with all listing data
        - DataFrame with all listings (one row per listing)
    
    Raises
    ------
    ValueError
        If API key is not provided and not found in environment.
    TimeoutError
        If snapshot is not ready after max_retries.
    
    Examples
    --------
    >>> # Using API key from .env file
    >>> snapshot_id, listings_data, listings_df = fetch_airbnb_listings_by_location("Beltline, Calgary", limit_per_input=50)
    >>> print(f"Found {len(listings_df)} listings")
    
    >>> # Using explicit API key
    >>> snapshot_id, listings_data, listings_df = fetch_airbnb_listings_by_location(
    ...     location="Downtown Vancouver", 
    ...     limit_per_input=100,
    ...     api_key="your-api-key-here"
    ... )
    """
    # Use provided API key or fall back to environment variable
    if api_key is None:
        api_key = brightdata_api_key
    
    if not api_key:
        raise ValueError("API key must be provided or set in BRIGHTDATA_API_KEY environment variable")
    
    print(f"Fetching Airbnb listings for location: {location}")
    print(f"Limit per input: {limit_per_input}")
    print(f"Max wait time: {max_retries * wait_time} seconds (~{(max_retries * wait_time) / 60:.1f} minutes)")
    print("-" * 80)
    
    # Step 1: Trigger the snapshot
    print("Step 1: Triggering BrightData API snapshot...")
    snapshot_id = get_brightdata_snapshot_by_location(location, limit_per_input, api_key)
    print(f"✓ Snapshot triggered successfully. Snapshot ID: {snapshot_id}")
    print()
    
    # Step 2: Wait for and retrieve the snapshot output
    print("Step 2: Waiting for snapshot to complete...")
    output = get_snapshot_output(snapshot_id, api_key, max_retries, wait_time)
    print("✓ Snapshot retrieved successfully")
    print()
    
    # Step 3: Extract and process the listings data
    print("Step 3: Extracting listings data...")
    listings_data, listings_df = extract_airbnb_listings(output)
    print(f"✓ Extraction complete")
    print()
    
    print("=" * 80)
    print(f"Location Listings: {location}")
    print(f"Total listings retrieved: {len(listings_data)}")
    print(f"Snapshot ID: {snapshot_id}")
    
    return snapshot_id, listings_data, listings_df

# %%
def fetch_airbnb_listings_by_url(
    url: str,
    api_key: Optional[str] = None,
    country: str = "CA",
    max_retries: int = 60, 
    wait_time: int = 30
) -> Tuple[str, List[Dict], pd.DataFrame]:
    """
    Orchestrates the full Airbnb listing scraping process by URL using Bright Data API.
    
    This function triggers a BrightData scrape for a specific Airbnb listing by URL,
    waits for the snapshot to complete (typically 2-5 minutes), and returns the data.
    
    Parameters
    ----------
    url : str
        The Airbnb listing URL to scrape (e.g., "https://www.airbnb.ca/rooms/1300059188064308611").
    api_key : str, optional
        BrightData API key. If not provided, will use BRIGHTDATA_API_KEY from .env file.
    country : str, optional
        Country code (default: "CA").
    max_retries : int, optional
        Maximum retries for snapshot polling (default: 60, allows for 30 minutes with 30s intervals).
    wait_time : int, optional
        Wait time in seconds between retries (default: 30).
    
    Returns
    -------
    Tuple[str, List[Dict], pd.DataFrame]
        A tuple containing:
        - Snapshot ID (str)
        - List of dictionaries with listing data
        - DataFrame with the listing (typically one row)
    
    Raises
    ------
    ValueError
        If API key is not provided and not found in environment.
    TimeoutError
        If snapshot is not ready after max_retries.
    
    Examples
    --------
    >>> # Using API key from .env file
    >>> snapshot_id, listing_data, listing_df = fetch_airbnb_listings_by_url("https://www.airbnb.ca/rooms/1300059188064308611")
    >>> print(f"Found {len(listing_df)} listing(s)")
    
    >>> # Using explicit API key
    >>> snapshot_id, listing_data, listing_df = fetch_airbnb_listings_by_url(
    ...     url="https://www.airbnb.ca/rooms/1300059188064308611",
    ...     api_key="your-api-key-here",
    ...     country="CA"
    ... )
    """
    # Use provided API key or fall back to environment variable
    if api_key is None:
        api_key = brightdata_api_key
    
    if not api_key:
        raise ValueError("API key must be provided or set in BRIGHTDATA_API_KEY environment variable")
    
    print(f"Fetching Airbnb listing from URL: {url}")
    print(f"Country: {country}")
    print(f"Max wait time: {max_retries * wait_time} seconds (~{(max_retries * wait_time) / 60:.1f} minutes)")
    print("-" * 80)
    
    # Step 1: Trigger the snapshot
    print("Step 1: Triggering BrightData API snapshot...")
    snapshot_id = get_brightdata_snapshot_by_url(url, api_key, country)
    print(f"✓ Snapshot triggered successfully. Snapshot ID: {snapshot_id}")
    print()
    
    # Step 2: Wait for and retrieve the snapshot output
    print("Step 2: Waiting for snapshot to complete (this typically takes 2-5 minutes)...")
    output = get_snapshot_output(snapshot_id, api_key, max_retries, wait_time)
    print("✓ Snapshot retrieved successfully")
    print()
    
    # Step 3: Extract and process the listings data
    print("Step 3: Extracting listing data...")
    listings_data, listings_df = extract_airbnb_listings(output)
    print(f"✓ Extraction complete")
    print()
    
    print("=" * 80)
    print(f"URL Listing: {url}")
    print(f"Total listings retrieved: {len(listings_data)}")
    print(f"Snapshot ID: {snapshot_id}")
    
    return snapshot_id, listings_data, listings_df


# %%
if __name__ == "__main__":
    """
    Main execution block for standalone usage.
    
    Modify the configuration variables below to fetch Airbnb listings and 
    automatically save them to JSON and Excel files in the Resources folder.
    """
    
    # ========================================
    # CONFIGURATION - Modify as needed
    # ========================================
    
    # Choose mode: "location" or "url"
    MODE = "location"  # Change to "url" for URL-based fetching
    
    # For location-based fetching
    LOCATION = "Beltline, Calgary"
    LIMIT_PER_INPUT = 100
    
    # For URL-based fetching
    LISTING_URL = "https://www.airbnb.ca/rooms/1300059188064308611"
    COUNTRY = "CA"
    
    # ========================================
    # EXECUTION
    # ========================================
    
    try:
        if MODE.lower() == "location":
            # Fetch listings by location
            snapshot_id, listings_data, listings_df = fetch_airbnb_listings_by_location(
                location=LOCATION,
                limit_per_input=LIMIT_PER_INPUT
            )
        elif MODE.lower() == "url":
            # Fetch listing by URL
            snapshot_id, listings_data, listings_df = fetch_airbnb_listings_by_url(
                url=LISTING_URL,
                country=COUNTRY
            )
        else:
            raise ValueError(f"Invalid MODE: {MODE}. Must be 'location' or 'url'")
        
        # ========================================
        # SAVE TO FILES
        # ========================================
        
        print("\n" + "=" * 80)
        print("Step 4: Saving data to files...")
        print("=" * 80)
        
        # Create Resources folder if it doesn't exist
        os.makedirs("Resources", exist_ok=True)
        
        # Define file paths with snapshot ID
        json_filepath = f"Resources/airbnb_listing_s_{snapshot_id}.json"
        excel_filepath = f"Resources/airbnb_listing_s_{snapshot_id}.xlsx"
        
        # Save to JSON
        with open(json_filepath, "w") as json_file:
            json.dump(listings_data, json_file, indent=2)
        print(f"✓ JSON saved: {json_filepath}")
        
        # Save to Excel
        listings_df.to_excel(excel_filepath, index=False)
        print(f"✓ Excel saved: {excel_filepath}")
        
        print("\n" + "=" * 80)
        print("✅ Script completed successfully!")
        print(f"Total listings: {len(listings_data)}")
        print(f"Snapshot ID: {snapshot_id}")
        print(f"Files saved:")
        print(f"  - {json_filepath}")
        print(f"  - {excel_filepath}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        raise

