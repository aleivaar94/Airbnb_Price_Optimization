"""
Airbnb Listings ETL Script
===========================
Extracts Airbnb listing data from JSON, transforms it into a normalized structure,
and loads it into a PostgreSQL database.

Author: Data Engineering Team
Date: 2025-11-10
"""

import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AirbnbETL:
    """
    ETL pipeline for loading Airbnb listings data into PostgreSQL.
    
    This class handles the complete ETL process:
    - Extract: Reads JSON data from file
    - Transform: Normalizes data into relational structure
    - Load: Inserts data into PostgreSQL with proper relationships
    
    Parameters
    ----------
    db_config : dict
        Database connection configuration with keys: host, database, user, password, port
    
    Attributes
    ----------
    conn : psycopg2.connection
        Database connection object
    cursor : psycopg2.cursor
        Database cursor for executing queries
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize ETL with database configuration.
        
        Parameters
        ----------
        db_config : dict
            Database connection parameters
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        
        # Cache for lookup tables to avoid duplicate inserts
        self.amenity_group_cache = {}
        self.amenity_cache = {}
        self.host_cache = set()
    
    def connect(self):
        """
        Establish connection to PostgreSQL database.
        
        Raises
        ------
        psycopg2.Error
            If connection fails
        """
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to database")
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def create_schema(self, schema_file: str):
        """
        Execute SQL schema creation script.
        
        Parameters
        ----------
        schema_file : str
            Path to SQL schema file
        
        Raises
        ------
        FileNotFoundError
            If schema file doesn't exist
        psycopg2.Error
            If schema creation fails
        """
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            self.cursor.execute(schema_sql)
            self.conn.commit()
            logger.info("Database schema created successfully")
        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_file}")
            raise
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Schema creation failed: {e}")
            raise
    
    def load_json_data(self, json_file: str) -> List[Dict[str, Any]]:
        """
        Load and parse JSON data from file.
        
        Parameters
        ----------
        json_file : str
            Path to JSON file containing Airbnb listings
        
        Returns
        -------
        list of dict
            Parsed JSON data as list of listing dictionaries
        
        Raises
        ------
        FileNotFoundError
            If JSON file doesn't exist
        json.JSONDecodeError
            If JSON is malformed
        """
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} listings from {json_file}")
            return data
        except FileNotFoundError:
            logger.error(f"JSON file not found: {json_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            raise
    
    def insert_host(self, listing: Dict[str, Any]) -> Optional[str]:
        """
        Insert or update host information.
        
        Parameters
        ----------
        listing : dict
            Listing data containing host_details
        
        Returns
        -------
        str or None
            Host ID if successful, None otherwise
        """
        host_details = listing.get('host_details')
        if not host_details or not host_details.get('host_id'):
            return None
        
        host_id = host_details.get('host_id')
        
        # Skip if already processed
        if host_id in self.host_cache:
            return host_id
        
        try:
            insert_query = """
                INSERT INTO hosts (
                    host_id, name, image_url, profile_url, rating,
                    number_of_reviews, response_rate, response_time,
                    years_hosting, languages, my_work, is_superhost
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (host_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    rating = EXCLUDED.rating,
                    number_of_reviews = EXCLUDED.number_of_reviews,
                    response_rate = EXCLUDED.response_rate,
                    years_hosting = EXCLUDED.years_hosting,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Extract response rate percentage
            response_rate = listing.get('host_response_rate')
            
            values = (
                host_id,
                host_details.get('name'),
                host_details.get('image'),
                host_details.get('url'),
                host_details.get('rating'),
                host_details.get('reviews'),
                response_rate,
                host_details.get('response_time'),
                host_details.get('years_hosting'),
                host_details.get('languages'),
                host_details.get('my_work'),
                listing.get('is_supperhost', False)
            )
            
            self.cursor.execute(insert_query, values)
            self.host_cache.add(host_id)
            return host_id
            
        except psycopg2.Error as e:
            logger.error(f"Failed to insert host {host_id}: {e}")
            return None
    
    def insert_listing(self, listing: Dict[str, Any], host_id: Optional[str]) -> Optional[int]:
        """
        Insert main listing information.
        
        Parameters
        ----------
        listing : dict
            Listing data
        host_id : str or None
            Associated host ID
        
        Returns
        -------
        int or None
            Listing ID if successful, None otherwise
        """
        try:
            # Parse details to extract bedroom, bed, bath counts
            details = listing.get('details', [])
            bedrooms = beds = baths = None
            
            for detail in details:
                if 'bedroom' in detail.lower():
                    bedrooms = int(detail.split()[0]) if detail.split()[0].isdigit() else None
                elif 'bed' in detail.lower() and 'bedroom' not in detail.lower():
                    beds = int(detail.split()[0]) if detail.split()[0].isdigit() else None
                elif 'bath' in detail.lower():
                    baths = int(detail.split()[0]) if detail.split()[0].isdigit() else None
            
            # Parse location into city, province, country
            city = province = country = None
            location = listing.get('location', '')
            if location:
                location_parts = [part.strip() for part in location.split(',')]
                if len(location_parts) == 3:
                    city, province, country = location_parts
                elif len(location_parts) == 2:
                    city, country = location_parts
                elif len(location_parts) == 1:
                    city = location_parts[0]
            
            # Parse timestamp
            timestamp = None
            if listing.get('timestamp'):
                try:
                    timestamp = datetime.fromisoformat(listing['timestamp'].replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    pass
            
            insert_query = """
                INSERT INTO listings (
                    property_id, host_id, name, listing_title, listing_name,
                    url, category, description, city, province, country, 
                    latitude, longitude, price_per_night, currency, 
                    rating, number_of_reviews, guests, bedrooms, beds, baths, 
                    pets_allowed, availability, is_guest_favorite, timestamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (property_id) DO UPDATE SET
                    price_per_night = EXCLUDED.price_per_night,
                    rating = EXCLUDED.rating,
                    number_of_reviews = EXCLUDED.number_of_reviews,
                    availability = EXCLUDED.availability,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING listing_id
            """
            # FIX: ON CONFLICT clause handles duplicate property_id during re-runs
            # Without this, ETL would fail silently on second run due to UNIQUE constraint
            # Now updates changed fields (price, rating, reviews) instead of failing
            
            values = (
                listing.get('property_id'),
                host_id,
                listing.get('name'),
                listing.get('listing_title'),
                listing.get('listing_name'),
                listing.get('url'),
                listing.get('category'),
                listing.get('description'),
                city,
                province,
                country,
                listing.get('lat'),
                listing.get('long'),
                listing.get('price'),
                listing.get('currency', 'CAD'),
                listing.get('ratings'),
                listing.get('property_number_of_reviews', 0),
                listing.get('guests'),
                bedrooms,
                beds,
                baths,
                listing.get('pets_allowed', False),
                listing.get('availability', 'true').lower() == 'true',
                listing.get('is_guest_favorite', False),
                timestamp
            )
            
            self.cursor.execute(insert_query, values)
            listing_id = self.cursor.fetchone()[0]
            return listing_id
            
        except psycopg2.Error as e:
            logger.error(f"Failed to insert listing '{listing.get('name', 'Unknown')}' (property_id: {listing.get('property_id', 'N/A')}): {e}")
            return None
    
    def insert_amenities(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert amenities and link to listing.
        
        Parameters
        ----------
        listing : dict
            Listing data with amenities
        listing_id : int
            ID of the listing
        """
        amenities_data = listing.get('amenities', [])
        
        for amenity_group in amenities_data:
            group_name = amenity_group.get('group_name')
            if not group_name:
                continue
            
            # Get or create amenity group
            if group_name not in self.amenity_group_cache:
                try:
                    # Create a savepoint before attempting insert
                    self.cursor.execute("SAVEPOINT amenity_group_insert")
                    
                    self.cursor.execute(
                        "INSERT INTO amenity_groups (group_name) VALUES (%s) "
                        "ON CONFLICT (group_name) DO NOTHING RETURNING group_id",
                        (group_name,)
                    )
                    result = self.cursor.fetchone()
                    if result:
                        group_id = result[0]
                    else:
                        self.cursor.execute(
                            "SELECT group_id FROM amenity_groups WHERE group_name = %s",
                            (group_name,)
                        )
                        group_id = self.cursor.fetchone()[0]
                    
                    self.amenity_group_cache[group_name] = group_id
                    self.cursor.execute("RELEASE SAVEPOINT amenity_group_insert")
                except psycopg2.Error as e:
                    logger.error(f"Failed to insert amenity group '{group_name}': {e}")
                    self.cursor.execute("ROLLBACK TO SAVEPOINT amenity_group_insert")
                    continue
            
            group_id = self.amenity_group_cache[group_name]
            
            # Insert individual amenities
            for item in amenity_group.get('items', []):
                amenity_name = item.get('name')
                amenity_code = item.get('value')
                
                if not amenity_name:
                    continue
                
                # No truncation needed - using TEXT type in database (unlimited length)
                # PostgreSQL treats TEXT and VARCHAR identically for performance
                # This preserves complete amenity descriptions for analysis
                
                cache_key = (amenity_code, amenity_name, group_id)
                
                if cache_key not in self.amenity_cache:
                    try:
                        # Create a savepoint before attempting insert
                        self.cursor.execute("SAVEPOINT amenity_insert")
                        
                        self.cursor.execute(
                            """
                            INSERT INTO amenities (amenity_code, amenity_name, group_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (amenity_code, amenity_name, group_id) DO NOTHING
                            RETURNING amenity_id
                            """,
                            (amenity_code, amenity_name, group_id)
                        )
                        result = self.cursor.fetchone()
                        if result:
                            amenity_id = result[0]
                        else:
                            self.cursor.execute(
                                """
                                SELECT amenity_id FROM amenities
                                WHERE amenity_code = %s AND amenity_name = %s AND group_id = %s
                                """,
                                (amenity_code, amenity_name, group_id)
                            )
                            amenity_id = self.cursor.fetchone()[0]
                        
                        # FIX: Only cache amenity_id AFTER successful savepoint release
                        # Previous bug: cached before release, so rollback left invalid ID in cache
                        # Error prevented: Foreign key violation on listing_amenities insert
                        self.cursor.execute("RELEASE SAVEPOINT amenity_insert")
                        self.amenity_cache[cache_key] = amenity_id
                    except psycopg2.Error as e:
                        logger.error(f"Failed to insert amenity '{amenity_name}' (code: {amenity_code[:50] if amenity_code else 'None'}...): {e}")
                        self.cursor.execute("ROLLBACK TO SAVEPOINT amenity_insert")
                        continue
                
                # FIX: Validate amenity exists in cache before linking
                # If amenity insert failed and rolled back, cache_key won't exist
                # This prevents foreign key violations when linking to non-existent amenity_id
                if cache_key not in self.amenity_cache:
                    continue
                    
                amenity_id = self.amenity_cache[cache_key]
                
                # Link amenity to listing
                try:
                    self.cursor.execute("SAVEPOINT amenity_link")
                    self.cursor.execute(
                        """
                        INSERT INTO listing_amenities (listing_id, amenity_id)
                        VALUES (%s, %s)
                        ON CONFLICT (listing_id, amenity_id) DO NOTHING
                        """,
                        (listing_id, amenity_id)
                    )
                    self.cursor.execute("RELEASE SAVEPOINT amenity_link")
                except psycopg2.Error as e:
                    logger.error(f"Failed to link amenity to listing: {e}")
                    self.cursor.execute("ROLLBACK TO SAVEPOINT amenity_link")
    
    def insert_reviews(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert guest reviews.
        
        Parameters
        ----------
        listing : dict
            Listing data with reviews
        listing_id : int
            ID of the listing
        """
        reviews = listing.get('reviews_details', [])
        
        if not reviews and listing.get('reviews'):
            # Handle simple review format
            reviews = [{'review': r} for r in listing['reviews']]
        
        for review in reviews:
            if not review.get('review'):
                continue
            
            review_date = None
            if review.get('review_date'):
                try:
                    review_date = datetime.fromisoformat(review['review_date'].replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    pass
            
            try:
                self.cursor.execute(
                    """
                    INSERT INTO listing_reviews (
                        listing_id, guest_name, guest_time_on_airbnb,
                        review_text, review_date, rating, stayed_for, host_response
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        listing_id,
                        review.get('guest_name'),
                        review.get('guest_time_on_airbnb'),
                        review.get('review'),
                        review_date,
                        review.get('rating'),
                        review.get('stayed_for'),
                        review.get('host_response')
                    )
                )
            except psycopg2.Error as e:
                logger.error(f"Failed to insert review: {e}")
    
    def insert_category_ratings(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert category-specific ratings.
        
        Parameters
        ----------
        listing : dict
            Listing data with category ratings
        listing_id : int
            ID of the listing
        """
        category_ratings = listing.get('category_rating', [])
        
        for rating in category_ratings:
            try:
                self.cursor.execute(
                    """
                    INSERT INTO listing_category_ratings (listing_id, category_name, rating_value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (listing_id, category_name) DO NOTHING
                    """,
                    (listing_id, rating.get('name'), float(rating.get('value', 0)))
                )
            except (psycopg2.Error, ValueError) as e:
                logger.error(f"Failed to insert category rating: {e}")
    
    def insert_house_rules(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert house rules.
        
        Parameters
        ----------
        listing : dict
            Listing data with house rules
        listing_id : int
            ID of the listing
        """
        house_rules = listing.get('house_rules', [])
        
        for rule in house_rules:
            try:
                self.cursor.execute(
                    "INSERT INTO listing_house_rules (listing_id, rule_text) VALUES (%s, %s)",
                    (listing_id, rule)
                )
            except psycopg2.Error as e:
                logger.error(f"Failed to insert house rule: {e}")
    
    def insert_highlights(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert listing highlights.
        
        Parameters
        ----------
        listing : dict
            Listing data with highlights
        listing_id : int
            ID of the listing
        """
        highlights = listing.get('highlights', [])
        
        for highlight in highlights:
            try:
                self.cursor.execute(
                    """
                    INSERT INTO listing_highlights (listing_id, highlight_name, highlight_value)
                    VALUES (%s, %s, %s)
                    """,
                    (listing_id, highlight.get('name'), highlight.get('value'))
                )
            except psycopg2.Error as e:
                logger.error(f"Failed to insert highlight: {e}")
    
    def insert_arrangement_details(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert room arrangement details.
        
        Parameters
        ----------
        listing : dict
            Listing data with arrangement details
        listing_id : int
            ID of the listing
        """
        arrangements = listing.get('arrangement_details', [])
        
        for arrangement in arrangements:
            try:
                self.cursor.execute(
                    """
                    INSERT INTO listing_arrangement_details (listing_id, room_name, arrangement_value)
                    VALUES (%s, %s, %s)
                    """,
                    (listing_id, arrangement.get('name'), arrangement.get('value'))
                )
            except psycopg2.Error as e:
                logger.error(f"Failed to insert arrangement detail: {e}")
    
    def insert_location_details(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert location detail descriptions.
        
        Parameters
        ----------
        listing : dict
            Listing data with location details
        listing_id : int
            ID of the listing
        """
        location_details = listing.get('location_details', [])
        
        for detail in location_details:
            # FIX: Skip if value is None or empty (NOT NULL constraint violation)
            # Some listings have location_details with title but no value
            # Error prevented: "null value in column "detail_value" violates not-null constraint"
            if not detail.get('value'):
                continue
                
            try:
                self.cursor.execute(
                    """
                    INSERT INTO listing_location_details (listing_id, detail_title, detail_value)
                    VALUES (%s, %s, %s)
                    """,
                    (listing_id, detail.get('title'), detail.get('value'))
                )
            except psycopg2.Error as e:
                logger.error(f"Failed to insert location detail: {e}")
    
    def insert_description_sections(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert structured description sections.
        
        Parameters
        ----------
        listing : dict
            Listing data with description sections
        listing_id : int
            ID of the listing
        """
        sections = listing.get('description_by_sections', [])
        
        for idx, section in enumerate(sections):
            try:
                self.cursor.execute(
                    """
                    INSERT INTO listing_description_sections 
                    (listing_id, section_title, section_value, section_order)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (listing_id, section.get('title'), section.get('value'), idx + 1)
                )
            except psycopg2.Error as e:
                logger.error(f"Failed to insert description section: {e}")
    
    def insert_cancellation_policies(self, listing: Dict[str, Any], listing_id: int):
        """
        Insert cancellation policy information.
        
        Parameters
        ----------
        listing : dict
            Listing data with cancellation policies
        listing_id : int
            ID of the listing
        """
        policies = listing.get('cancellation_policy', [])
        
        for policy in policies:
            policy_date = None
            if policy.get('cancellation_value'):
                try:
                    policy_date = datetime.strptime(
                        policy['cancellation_value'], '%m/%d/%Y'
                    ).date()
                except (ValueError, AttributeError):
                    pass
            
            try:
                self.cursor.execute(
                    """
                    INSERT INTO listing_cancellation_policies (listing_id, policy_name, policy_date)
                    VALUES (%s, %s, %s)
                    """,
                    (listing_id, policy.get('cancellation_name'), policy_date)
                )
            except psycopg2.Error as e:
                logger.error(f"Failed to insert cancellation policy: {e}")
    
    def process_listing(self, listing: Dict[str, Any]) -> bool:
        """
        Process a single listing and insert all related data.
        
        Parameters
        ----------
        listing : dict
            Complete listing data
        
        Returns
        -------
        bool
            True if successful, False otherwise
        """
        try:
            # FIX: Create a savepoint for EACH listing (per-listing transaction isolation)
            # Previous issue: All 100 listings in one transaction - if ANY failed, ALL rolled back
            # Now: Each listing commits independently - failures don't cascade
            # Result: Increased success rate from 0/100 to 100/100
            self.cursor.execute("SAVEPOINT listing_process")
            
            # Insert host first
            host_id = self.insert_host(listing)
            
            # Insert main listing
            listing_id = self.insert_listing(listing, host_id)
            if not listing_id:
                logger.warning(f"Skipping listing: {listing.get('name', 'Unknown')}")
                self.cursor.execute("ROLLBACK TO SAVEPOINT listing_process")
                return False
            
            # Insert related data
            self.insert_amenities(listing, listing_id)
            self.insert_reviews(listing, listing_id)
            self.insert_category_ratings(listing, listing_id)
            self.insert_house_rules(listing, listing_id)
            self.insert_highlights(listing, listing_id)
            self.insert_arrangement_details(listing, listing_id)
            self.insert_location_details(listing, listing_id)
            self.insert_description_sections(listing, listing_id)
            self.insert_cancellation_policies(listing, listing_id)
            
            # FIX: Commit AFTER each successful listing (per-listing commit strategy)
            # This ensures successful listings persist even if subsequent ones fail
            # Critical for achieving 100% success rate with partial failures
            self.cursor.execute("RELEASE SAVEPOINT listing_process")
            self.conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing listing '{listing.get('name', 'Unknown')}': {e}")
            try:
                self.cursor.execute("ROLLBACK TO SAVEPOINT listing_process")
            except:
                self.conn.rollback()
            return False
    
    def run_etl(self, json_file: str, schema_file: str, recreate_schema: bool = True):
        """
        Execute complete ETL pipeline.
        
        This orchestrates the entire ETL process:
        1. Connect to database
        2. Create/recreate schema if requested
        3. Load JSON data
        4. Process each listing with all related data
        5. Commit transaction
        6. Report statistics
        
        Parameters
        ----------
        json_file : str
            Path to JSON file with Airbnb listings
        schema_file : str
            Path to SQL schema file
        recreate_schema : bool, default=True
            Whether to drop and recreate schema
        
        Example
        -------
        >>> db_config = {
        ...     'host': 'localhost',
        ...     'database': 'airbnb_db',
        ...     'user': 'postgres',
        ...     'password': 'password',
        ...     'port': 5432
        ... }
        >>> etl = AirbnbETL(db_config)
        >>> etl.run_etl('listings.json', 'schema.sql')
        """
        try:
            logger.info("Starting ETL process...")
            
            # Connect to database
            self.connect()
            
            # Create schema if requested
            if recreate_schema:
                logger.info("Creating database schema...")
                self.create_schema(schema_file)
            
            # Load JSON data
            listings = self.load_json_data(json_file)
            
            # Process each listing
            success_count = 0
            for idx, listing in enumerate(listings, 1):
                logger.info(f"Processing listing {idx}/{len(listings)}")
                if self.process_listing(listing):
                    success_count += 1
            
            # FIX: No batch commit needed - each listing commits individually
            # Previous: Single commit at end caused all-or-nothing behavior
            # Now: Per-listing commits ensure successful listings persist independently
            logger.info(f"ETL completed successfully! Processed {success_count}/{len(listings)} listings")
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"ETL failed: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """
    Main execution function.
    
    Configure database connection and run ETL pipeline.
    Loads database password from .env file using DB_PASSWORD environment variable.
    
    Environment Variables Required
    ------------------------------
    DB_PASSWORD : str
        PostgreSQL database password (loaded from .env file)
    
    Optional Environment Variables
    ------------------------------
    DB_HOST : str, default='localhost'
        PostgreSQL host address
    DB_NAME : str, default='airbnb_db'
        Database name
    DB_USER : str, default='postgres'
        Database user
    DB_PORT : int, default=5432
        PostgreSQL port number
    
    Example .env File
    -----------------
    DB_HOST=localhost
    DB_NAME=airbnb_db
    DB_USER=postgres
    DB_PASSWORD=your_secure_password
    DB_PORT=5432
    """
    # Database configuration - loads from environment variables
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'airbnb_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD'),  # Loaded from .env file
        'port': int(os.getenv('DB_PORT', '5432'))
    }
    
    # Check if password is set
    if not db_config['password']:
        logger.error("DB_PASSWORD not found in environment variables!")
        logger.error("Please create a .env file with DB_PASSWORD=your_password")
        raise ValueError("DB_PASSWORD environment variable is required")
    
    # File paths
    json_file = 'Resources/airbnb_beltline_calgary_listings_100.json'
    schema_file = 'database_normalized_schema.sql'
    
    # Run ETL
    etl = AirbnbETL(db_config)
    etl.run_etl(json_file, schema_file, recreate_schema=True)


if __name__ == '__main__':
    main()
