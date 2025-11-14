"""
ETL Script: Normalized to Dimensional Database Transformation
==============================================================
Transforms Airbnb data from normalized (3NF) schema to dimensional (star schema)
for competitor analysis and price optimization.

Author: Data Engineering Team
Date: 2025-11-13

Environment Variables Required
------------------------------
DB_HOST : str
    PostgreSQL host address
DB_USER : str
    Database user
DB_PASSWORD : str
    Database password
DB_PORT : int
    PostgreSQL port number
SOURCE_DB_NAME : str
    Source database name (normalized schema)
TARGET_DB_NAME : str
    Target database name (dimensional schema)
"""

import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import numpy as np
from sklearn.cluster import KMeans
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DimensionalETL:
    """
    ETL pipeline for transforming normalized Airbnb data to dimensional model.
    
    This class handles the complete transformation from a normalized (3NF) database
    to a dimensional star schema optimized for analytics and competitor analysis.
    
    Parameters
    ----------
    source_db_config : dict
        Source database connection configuration (normalized schema)
    target_db_config : dict
        Target database connection configuration (dimensional schema)
    
    Attributes
    ----------
    source_conn : psycopg2.connection
        Connection to source database
    target_conn : psycopg2.connection
        Connection to target database
    """
    
    # Calgary downtown coordinates for distance calculations
    CALGARY_DOWNTOWN_LAT = 51.0447
    CALGARY_DOWNTOWN_LONG = -114.0719
    
    # Amenity classifications
    ESSENTIAL_AMENITIES = {
        'Wifi', 'Kitchen', 'Free parking', 'Air conditioning', 
        'Heating', 'Washer', 'Dryer', 'Dedicated workspace'
    }
    
    LUXURY_AMENITIES = {
        'Pool', 'Hot tub', 'Gym', 'EV charger', 'Sauna', 
        'BBQ grill', 'Outdoor furniture', 'Patio or balcony'
    }
    
    SAFETY_AMENITIES = {
        'Smoke alarm', 'Carbon monoxide alarm', 'First aid kit',
        'Fire extinguisher', 'Security cameras'
    }
    
    def __init__(self, source_db_config: Dict[str, str], target_db_config: Dict[str, str]):
        """
        Initialize ETL with source and target database configurations.
        
        Parameters
        ----------
        source_db_config : dict
            Source database connection parameters
        target_db_config : dict
            Target database connection parameters
        """
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config
        self.source_conn = None
        self.target_conn = None
        self.source_cursor = None
        self.target_cursor = None
        
        # Caches for dimension key lookups
        self.host_key_cache = {}
        self.property_key_cache = {}
        self.location_key_cache = {}
        self.rating_key_cache = {}
    
    def connect(self):
        """
        Establish connections to both source and target databases.
        
        Raises
        ------
        psycopg2.Error
            If connection fails
        """
        try:
            self.source_conn = psycopg2.connect(**self.source_db_config)
            self.source_cursor = self.source_conn.cursor()
            logger.info(f"Connected to source database: {self.source_db_config['database']}")
            
            self.target_conn = psycopg2.connect(**self.target_db_config)
            self.target_cursor = self.target_conn.cursor()
            logger.info(f"Connected to target database: {self.target_db_config['database']}")
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close all database connections and cursors."""
        if self.source_cursor:
            self.source_cursor.close()
        if self.source_conn:
            self.source_conn.close()
        if self.target_cursor:
            self.target_cursor.close()
        if self.target_conn:
            self.target_conn.close()
        logger.info("All database connections closed")
    
    def calculate_haversine_distance(self, lat1: float, lon1: float, 
                                    lat2: float, lon2: float) -> float:
        """
        Calculate distance between two coordinates using Haversine formula.
        
        Parameters
        ----------
        lat1, lon1 : float
            First coordinate (latitude, longitude)
        lat2, lon2 : float
            Second coordinate (latitude, longitude)
        
        Returns
        -------
        float
            Distance in kilometers
        """
        # Convert to float to handle Decimal types from PostgreSQL
        lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
        
        earth_radius_km = 6371.0
        
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        dlat = np.radians(lat2 - lat1)
        dlon = np.radians(lon2 - lon1)
        
        a = (np.sin(dlat/2)**2 + 
             np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2)
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
        
        return earth_radius_km * c
    
    def classify_host_tier(self, is_superhost: bool, rating: Optional[float]) -> str:
        """
        Classify host into tier based on superhost status and rating.
        
        Parameters
        ----------
        is_superhost : bool
            Whether host is a superhost
        rating : float or None
            Host rating (0-5 scale)
        
        Returns
        -------
        str
            Host tier: 'Elite', 'Premium', or 'Standard'
        """
        if rating is None:
            return 'Standard'
        
        if is_superhost and rating > 4.8:
            return 'Elite'
        elif rating > 4.5:
            return 'Premium'
        else:
            return 'Standard'
    
    def classify_experience_level(self, years_hosting: Optional[int]) -> str:
        """
        Classify host experience level based on years hosting.
        
        Parameters
        ----------
        years_hosting : int or None
            Number of years hosting
        
        Returns
        -------
        str
            Experience level: 'Expert', 'Experienced', or 'New'
        """
        if years_hosting is None or years_hosting <= 2:
            return 'New'
        elif years_hosting <= 5:
            return 'Experienced'
        else:
            return 'Expert'
    
    def classify_property_size_tier(self, bedrooms: Optional[int]) -> str:
        """
        Classify property size based on number of bedrooms.
        
        Parameters
        ----------
        bedrooms : int or None
            Number of bedrooms
        
        Returns
        -------
        str
            Size tier: 'Studio', 'Small', 'Medium', or 'Large'
        """
        if bedrooms is None or bedrooms == 0:
            return 'Studio'
        elif bedrooms == 1:
            return 'Small'
        elif bedrooms in [2, 3]:
            return 'Medium'
        else:
            return 'Large'
    
    def classify_location_tier(self, distance_km: float) -> str:
        """
        Classify location tier based on distance to downtown.
        
        Parameters
        ----------
        distance_km : float
            Distance to downtown in kilometers
        
        Returns
        -------
        str
            Location tier: 'Urban Core', 'Downtown Adjacent', 'Neighborhood', or 'Suburban'
        """
        if distance_km < 1:
            return 'Urban Core'
        elif distance_km < 3:
            return 'Downtown Adjacent'
        elif distance_km < 7:
            return 'Neighborhood'
        else:
            return 'Suburban'
    
    def classify_quality_tier(self, overall_quality_score: Optional[float]) -> str:
        """
        Classify quality tier based on overall quality score.
        
        Parameters
        ----------
        overall_quality_score : float or None
            Overall quality score (0-5 scale)
        
        Returns
        -------
        str
            Quality tier: 'Exceptional', 'Excellent', 'Good', or 'Fair'
        """
        if overall_quality_score is None:
            return 'Fair'
        
        if overall_quality_score > 4.8:
            return 'Exceptional'
        elif overall_quality_score > 4.5:
            return 'Excellent'
        elif overall_quality_score > 4.0:
            return 'Good'
        else:
            return 'Fair'
    
    def classify_amenity_tier(self, amenity_score: int) -> str:
        """
        Classify amenity tier based on amenity score.
        
        Parameters
        ----------
        amenity_score : int
            Calculated amenity score
        
        Returns
        -------
        str
            Amenity tier: 'Luxury', 'Premium', 'Standard', or 'Basic'
        """
        if amenity_score > 50:
            return 'Luxury'
        elif amenity_score > 30:
            return 'Premium'
        elif amenity_score > 15:
            return 'Standard'
        else:
            return 'Basic'
    
    # ========================================================================
    # DIMENSION LOADING METHODS
    # ========================================================================
    
    def load_dim_host(self):
        """
        Load dim_host dimension from normalized hosts table.
        
        Extracts host data and calculates derived attributes:
        - host_tier (Elite/Premium/Standard)
        - experience_level (Expert/Experienced/New)
        """
        logger.info("Loading dim_host...")
        
        # Extract from source
        self.source_cursor.execute("""
            SELECT 
                host_id, name, image_url, profile_url, rating,
                number_of_reviews, response_rate, response_time,
                years_hosting, languages, my_work, is_superhost
            FROM hosts
        """)
        
        hosts = self.source_cursor.fetchall()
        logger.info(f"Extracted {len(hosts)} hosts from source")
        
        # Transform and load
        insert_query = """
            INSERT INTO dim_host (
                host_id, host_name, host_rating, host_number_of_reviews,
                host_response_rate, host_response_time, host_years_hosting,
                languages, my_work, image_url, profile_url, is_superhost,
                host_tier, experience_level
            ) VALUES %s
            ON CONFLICT (host_id) DO UPDATE SET
                host_rating = EXCLUDED.host_rating,
                host_number_of_reviews = EXCLUDED.host_number_of_reviews,
                host_tier = EXCLUDED.host_tier,
                updated_at = CURRENT_TIMESTAMP
            RETURNING host_key, host_id
        """
        
        values = []
        for host in hosts:
            host_id, name, image, url, rating, reviews, response_rate, \
            response_time, years, languages, work, is_super = host
            
            host_tier = self.classify_host_tier(is_super, rating)
            experience = self.classify_experience_level(years)
            
            values.append((
                host_id, name, rating, reviews, response_rate,
                response_time, years, languages, work, image, url,
                is_super, host_tier, experience
            ))
        
        execute_values(self.target_cursor, insert_query, values)
        
        # Build cache for lookups
        self.target_cursor.execute("SELECT host_key, host_id FROM dim_host")
        self.host_key_cache = {host_id: host_key for host_key, host_id in self.target_cursor.fetchall()}
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} hosts into dim_host")
    
    def load_dim_property(self):
        """
        Load dim_property dimension from normalized listings table.
        
        Extracts property characteristics and calculates:
        - property_size_tier (Studio/Small/Medium/Large)
        - guest_per_bedroom_ratio
        - bath_to_bedroom_ratio
        """
        logger.info("Loading dim_property...")
        
        # Extract from source
        self.source_cursor.execute("""
            SELECT 
                property_id, name, listing_title, listing_name, category,
                url, description,
                guests, bedrooms, beds, baths, pets_allowed, is_guest_favorite
            FROM listings
        """)
        
        properties = self.source_cursor.fetchall()
        logger.info(f"Extracted {len(properties)} properties from source")
        
        # Transform and load
        insert_query = """
            INSERT INTO dim_property (
                property_id, name, listing_name, listing_title, category,
                url, description,
                guests_capacity, bedrooms, beds, baths, pets_allowed,
                is_guest_favorite, property_size_tier,
                guest_per_bedroom_ratio, bath_to_bedroom_ratio
            ) VALUES %s
            ON CONFLICT (property_id) DO UPDATE SET
                listing_name = EXCLUDED.listing_name,
                updated_at = CURRENT_TIMESTAMP
            RETURNING property_key, property_id
        """
        
        values = []
        for prop in properties:
            prop_id, name, title, listing_name, category, url, description, \
            guests, bedrooms, beds, baths, pets, is_fav = prop
            
            size_tier = self.classify_property_size_tier(bedrooms)
            
            # Calculate ratios (handle division by zero and None values)
            guest_ratio = guests / bedrooms if guests and bedrooms and bedrooms > 0 else None
            bath_ratio = baths / bedrooms if baths and bedrooms and bedrooms > 0 else None
            
            values.append((
                prop_id, name, listing_name or name, title, category,
                url, description,
                guests, bedrooms, beds, baths, pets, is_fav,
                size_tier, guest_ratio, bath_ratio
            ))
        
        execute_values(self.target_cursor, insert_query, values)
        
        # Build cache
        self.target_cursor.execute("SELECT property_key, property_id FROM dim_property")
        self.property_key_cache = {prop_id: prop_key for prop_key, prop_id in self.target_cursor.fetchall()}
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} properties into dim_property")
    
    def load_dim_location(self):
        """
        Load dim_location dimension with geographic clustering.
        
        Performs K-means clustering on coordinates and calculates:
        - location_cluster_id (K-means cluster assignment)
        - distance_to_downtown_km
        - location_tier (Urban Core/Downtown Adjacent/Neighborhood/Suburban)
        """
        logger.info("Loading dim_location...")
        
        # Extract unique locations from source
        self.source_cursor.execute("""
            SELECT DISTINCT
                city, province, country, latitude, longitude
            FROM listings
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        
        locations = self.source_cursor.fetchall()
        logger.info(f"Extracted {len(locations)} unique locations from source")
        
        if len(locations) == 0:
            logger.warning("No locations found with coordinates")
            return
        
        # Prepare coordinates for clustering
        coords = np.array([(lat, lon) for _, _, _, lat, lon in locations])
        
        # Perform K-means clustering (use min of 10 clusters or number of locations)
        n_clusters = min(10, len(locations))
        if len(locations) >= 3:
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            cluster_labels = kmeans.fit_predict(coords)
        else:
            cluster_labels = [0] * len(locations)
        
        logger.info(f"Performed K-means clustering with {n_clusters} clusters")
        
        # Transform and load
        insert_query = """
            INSERT INTO dim_location (
                city, province, country, latitude, longitude,
                location_cluster_id, distance_to_downtown_km, location_tier
            ) VALUES %s
            ON CONFLICT (latitude, longitude) DO UPDATE SET
                location_cluster_id = EXCLUDED.location_cluster_id,
                distance_to_downtown_km = EXCLUDED.distance_to_downtown_km,
                location_tier = EXCLUDED.location_tier,
                updated_at = CURRENT_TIMESTAMP
            RETURNING location_key, latitude, longitude
        """
        
        values = []
        for i, loc in enumerate(locations):
            city, province, country, lat, lon = loc
            cluster_id = int(cluster_labels[i])
            
            # Calculate distance to downtown
            distance = self.calculate_haversine_distance(
                lat, lon,
                self.CALGARY_DOWNTOWN_LAT, self.CALGARY_DOWNTOWN_LONG
            )
            
            location_tier = self.classify_location_tier(distance)
            
            values.append((
                city, province, country, lat, lon,
                cluster_id, float(round(distance, 2)), location_tier
            ))
        
        execute_values(self.target_cursor, insert_query, values)
        
        # Build cache
        self.target_cursor.execute("SELECT location_key, latitude, longitude FROM dim_location")
        self.location_key_cache = {
            (float(lat), float(lon)): loc_key 
            for loc_key, lat, lon in self.target_cursor.fetchall()
        }
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} locations into dim_location")
    
    def load_dim_category_ratings(self):
        """
        Load dim_category_ratings dimension from listing_category_ratings.
        
        Calculates:
        - overall_quality_score (weighted average of all ratings)
        - quality_tier (Exceptional/Excellent/Good/Fair)
        - value_index (value_rating / overall_quality_score)
        """
        logger.info("Loading dim_category_ratings...")
        
        # Extract category ratings from source (pivot from rows to columns)
        self.source_cursor.execute("""
            SELECT 
                listing_id,
                MAX(CASE WHEN category_name ILIKE '%clean%' THEN rating_value END) as cleanliness,
                MAX(CASE WHEN category_name ILIKE '%accura%' THEN rating_value END) as accuracy,
                MAX(CASE WHEN category_name ILIKE '%check%' THEN rating_value END) as checkin,
                MAX(CASE WHEN category_name ILIKE '%commun%' THEN rating_value END) as communication,
                MAX(CASE WHEN category_name ILIKE '%locat%' THEN rating_value END) as location,
                MAX(CASE WHEN category_name ILIKE '%value%' THEN rating_value END) as value
            FROM listing_category_ratings
            GROUP BY listing_id
        """)
        
        ratings = self.source_cursor.fetchall()
        logger.info(f"Extracted {len(ratings)} rating sets from source")
        
        if len(ratings) == 0:
            logger.warning("No category ratings found")
            return
        
        # Transform and load
        insert_query = """
            INSERT INTO dim_category_ratings (
                cleanliness_rating, accuracy_rating, checkin_rating,
                communication_rating, location_rating, value_rating,
                overall_quality_score, quality_tier, value_index
            ) VALUES %s
            RETURNING rating_key
        """
        
        values = []
        listing_rating_map = {}
        
        for rating in ratings:
            listing_id, clean, accuracy, checkin, comm, location, value = rating
            
            # Convert Decimal to float for calculations
            clean = float(clean) if clean is not None else None
            accuracy = float(accuracy) if accuracy is not None else None
            checkin = float(checkin) if checkin is not None else None
            comm = float(comm) if comm is not None else None
            location = float(location) if location is not None else None
            value = float(value) if value is not None else None
            
            # Calculate overall quality score (weighted average)
            scores = []
            weights = []
            
            if clean: scores.append(clean); weights.append(0.25)
            if accuracy: scores.append(accuracy); weights.append(0.15)
            if checkin: scores.append(checkin); weights.append(0.10)
            if comm: scores.append(comm); weights.append(0.15)
            if location: scores.append(location); weights.append(0.15)
            if value: scores.append(value); weights.append(0.20)
            
            if scores:
                # Normalize weights
                total_weight = sum(weights)
                overall = sum(s * w for s, w in zip(scores, weights)) / total_weight
            else:
                overall = None
            
            quality_tier = self.classify_quality_tier(overall)
            value_index = (value / overall) if overall and value else None
            
            values.append((
                clean, accuracy, checkin, comm, location, value,
                overall, quality_tier, value_index
            ))
            
            # Map listing_id to position for later lookup
            listing_rating_map[listing_id] = len(values) - 1
        
        execute_values(self.target_cursor, insert_query, values)
        
        # Get generated keys and build cache
        self.target_cursor.execute("""
            SELECT rating_key FROM dim_category_ratings 
            ORDER BY rating_key DESC LIMIT %s
        """, (len(values),))
        
        rating_keys = [row[0] for row in self.target_cursor.fetchall()]
        rating_keys.reverse()  # Match insertion order
        
        # Map listing_id to rating_key
        for listing_id, idx in listing_rating_map.items():
            if idx < len(rating_keys):
                self.rating_key_cache[listing_id] = rating_keys[idx]
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} rating sets into dim_category_ratings")
    
    # ========================================================================
    # FACT TABLE LOADING
    # ========================================================================
    
    def load_fact_listing_metrics(self):
        """
        Load central fact table with listing performance metrics.
        
        Joins with dimensions and calculates:
        - price_per_guest, price_per_bedroom, price_per_bed
        - review_velocity
        - competitiveness_score
        - value_score
        - popularity_index
        """
        logger.info("Loading fact_listing_metrics...")
        
        # Extract from source with all necessary data
        self.source_cursor.execute("""
            SELECT 
                l.listing_id,
                l.property_id,
                l.host_id,
                l.price_per_night,
                l.currency,
                l.rating,
                l.number_of_reviews,
                l.guests,
                l.bedrooms,
                l.beds,
                l.baths,
                l.availability,
                l.is_guest_favorite,
                l.latitude,
                l.longitude,
                l.timestamp
            FROM listings l
            WHERE l.property_id IS NOT NULL
        """)
        
        listings = self.source_cursor.fetchall()
        logger.info(f"Extracted {len(listings)} listings from source")
        
        # Calculate today's date_key
        today = datetime.now()
        date_key = int(today.strftime('%Y%m%d'))
        
        # Transform and load
        insert_query = """
            INSERT INTO fact_listing_metrics (
                property_id, host_key, property_key, location_key, rating_key,
                date_key, price_per_night, currency, listing_rating, number_of_reviews,
                is_available, price_per_guest, price_per_bedroom, price_per_bed,
                review_velocity, competitiveness_score, value_score, popularity_index,
                data_scraped_at, snapshot_date
            ) VALUES %s
            RETURNING listing_key, property_id
        """
        
        values = []
        skipped = 0
        
        for listing in listings:
            listing_id, prop_id, host_id, price, currency, rating, reviews, \
            guests, bedrooms, beds, baths, avail, is_fav, lat, lon, timestamp = listing
            
            # Convert Decimal types to float for calculations
            price = float(price) if price is not None else None
            rating = float(rating) if rating is not None else None
            lat = float(lat) if lat is not None else None
            lon = float(lon) if lon is not None else None
            
            # Lookup dimension keys
            host_key = self.host_key_cache.get(host_id)
            property_key = self.property_key_cache.get(prop_id)
            location_key = self.location_key_cache.get((lat, lon)) if lat and lon else None
            rating_key = self.rating_key_cache.get(listing_id)
            
            if not all([host_key, property_key, location_key]):
                skipped += 1
                continue
            
            # Calculate derived measures
            price_per_guest = price / guests if price and guests and guests > 0 else None
            price_per_bedroom = price / bedrooms if price and bedrooms and bedrooms > 0 else None
            price_per_bed = price / beds if price and beds and beds > 0 else None
            
            # Review velocity (reviews per day since creation)
            if timestamp:
                days_since = (datetime.now() - timestamp).days
                review_velocity = reviews / days_since if days_since > 0 else 0
            else:
                review_velocity = None
            
            # Competitiveness score (simplified - will be enhanced with amenities)
            comp_score = 0
            if rating: comp_score += (rating / 5.0) * 30
            if reviews: comp_score += min(reviews / 100, 1.0) * 25
            if is_fav: comp_score += 10
            # Will add host and amenity components later
            
            # Value score (quality vs price) - placeholder
            value_score = None
            if rating and price and price > 0:
                value_score = (rating / 5.0) / (price / 200) * 100
                value_score = min(value_score, 100)
            
            # Popularity index - placeholder
            popularity_index = None
            if rating and reviews:
                popularity_index = (reviews * rating) / 10  # Simplified
            
            values.append((
                prop_id, host_key, property_key, location_key, rating_key,
                date_key, price, currency or 'CAD', rating, reviews, avail,
                price_per_guest, price_per_bedroom, price_per_bed,
                review_velocity, comp_score, value_score, popularity_index,
                timestamp, today.date()
            ))
        
        execute_values(self.target_cursor, insert_query, values)
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} listings into fact_listing_metrics (skipped {skipped})")
    
    def load_fact_listing_amenities_summary(self):
        """
        Load amenity summary fact table.
        
        Aggregates amenities per listing and calculates:
        - Counts for essential, luxury, and safety amenities
        - amenity_score (weighted sum)
        - amenity_tier (Luxury/Premium/Standard/Basic)
        """
        logger.info("Loading fact_listing_amenities_summary...")
        
        # Get listing_key to listing_id mapping
        self.target_cursor.execute("""
            SELECT listing_key, property_id FROM fact_listing_metrics
        """)
        listing_key_map = {prop_id: listing_key for listing_key, prop_id in self.target_cursor.fetchall()}
        
        # Get property_id to listing_id mapping from source
        self.source_cursor.execute("""
            SELECT listing_id, property_id FROM listings
        """)
        prop_to_listing = {prop_id: listing_id for listing_id, prop_id in self.source_cursor.fetchall()}
        
        # Extract amenities per listing
        self.source_cursor.execute("""
            SELECT 
                la.listing_id,
                a.amenity_name,
                ag.group_name
            FROM listing_amenities la
            JOIN amenities a ON la.amenity_id = a.amenity_id
            LEFT JOIN amenity_groups ag ON a.group_id = ag.group_id
        """)
        
        amenities_raw = self.source_cursor.fetchall()
        
        # Group by listing
        listing_amenities = {}
        for listing_id, amenity_name, group_name in amenities_raw:
            if listing_id not in listing_amenities:
                listing_amenities[listing_id] = []
            listing_amenities[listing_id].append(amenity_name)
        
        logger.info(f"Extracted amenities for {len(listing_amenities)} listings")
        
        # Transform and load
        insert_query = """
            INSERT INTO fact_listing_amenities_summary (
                listing_key, total_amenities_count, essential_amenities_count,
                luxury_amenities_count, safety_amenities_count,
                amenity_score, amenity_tier
            ) VALUES %s
            ON CONFLICT (listing_key) DO UPDATE SET
                total_amenities_count = EXCLUDED.total_amenities_count,
                amenity_score = EXCLUDED.amenity_score,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = []
        for listing_id, amenity_list in listing_amenities.items():
            # Find corresponding property_id and listing_key
            prop_id = next((k for k, v in prop_to_listing.items() if v == listing_id), None)
            if not prop_id:
                continue
            
            listing_key = listing_key_map.get(prop_id)
            if not listing_key:
                continue
            
            # Classify amenities
            essential_count = sum(1 for a in amenity_list if any(e in a for e in self.ESSENTIAL_AMENITIES))
            luxury_count = sum(1 for a in amenity_list if any(l in a for l in self.LUXURY_AMENITIES))
            safety_count = sum(1 for a in amenity_list if any(s in a for s in self.SAFETY_AMENITIES))
            
            # Calculate amenity score
            amenity_score = essential_count * 2 + luxury_count * 3 + safety_count * 1
            
            amenity_tier = self.classify_amenity_tier(amenity_score)
            
            values.append((
                listing_key, len(amenity_list), essential_count,
                luxury_count, safety_count, amenity_score, amenity_tier
            ))
        
        execute_values(self.target_cursor, insert_query, values)
        
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} amenity summaries into fact_listing_amenities_summary")
    
    # ========================================================================
    # COMPETITOR ANALYSIS
    # ========================================================================
    
    def calculate_competitor_similarity(self):
        """
        Calculate similarity scores and identify top 25 competitors for each listing.
        
        Uses multi-dimensional similarity:
        - Location (35%): geographic distance and cluster
        - Property (25%): bedrooms, beds, baths, capacity
        - Quality (20%): ratings alignment
        - Amenity (10%): shared amenities
        - Price (10%): price range overlap
        """
        logger.info("Calculating competitor similarities...")
        
        # Get all listings with their attributes for comparison
        self.target_cursor.execute("""
            SELECT 
                f.listing_key,
                f.property_id,
                f.price_per_night,
                f.listing_rating,
                p.bedrooms,
                p.beds,
                p.baths,
                p.guests_capacity,
                l.latitude,
                l.longitude,
                l.location_cluster_id,
                r.overall_quality_score,
                a.amenity_score
            FROM fact_listing_metrics f
            JOIN dim_property p ON f.property_key = p.property_key
            JOIN dim_location l ON f.location_key = l.location_key
            LEFT JOIN dim_category_ratings r ON f.rating_key = r.rating_key
            LEFT JOIN fact_listing_amenities_summary a ON f.listing_key = a.listing_key
        """)
        
        listings = self.target_cursor.fetchall()
        logger.info(f"Calculating similarities for {len(listings)} listings")
        
        # Build similarity matrix
        similarities = []
        
        for i, listing1 in enumerate(listings):
            key1, prop_id1, price1, rating1, bed1, beds1, bath1, guests1, \
            lat1, lon1, cluster1, quality1, amenity1 = listing1
            
            # Convert Decimal types to float for calculations
            price1 = float(price1) if price1 is not None else None
            rating1 = float(rating1) if rating1 is not None else None
            lat1 = float(lat1) if lat1 is not None else None
            lon1 = float(lon1) if lon1 is not None else None
            quality1 = float(quality1) if quality1 is not None else None
            amenity1 = float(amenity1) if amenity1 is not None else None
            
            listing_similarities = []
            
            for j, listing2 in enumerate(listings):
                if i == j:  # Skip self-comparison
                    continue
                
                key2, prop_id2, price2, rating2, bed2, beds2, bath2, guests2, \
                lat2, lon2, cluster2, quality2, amenity2 = listing2
                
                # Convert Decimal types to float for calculations
                price2 = float(price2) if price2 is not None else None
                rating2 = float(rating2) if rating2 is not None else None
                lat2 = float(lat2) if lat2 is not None else None
                lon2 = float(lon2) if lon2 is not None else None
                quality2 = float(quality2) if quality2 is not None else None
                amenity2 = float(amenity2) if amenity2 is not None else None
                
                # 1. Location Similarity (0-100)
                same_cluster_bonus = 50 if cluster1 == cluster2 else 0
                distance = self.calculate_haversine_distance(lat1, lon1, lat2, lon2)
                distance_score = float(100 * np.exp(-distance / 2))
                location_sim = float(min(same_cluster_bonus + distance_score, 100))
                
                # 2. Property Similarity (0-100)
                bedroom_match = 40 if bed1 == bed2 else 0
                guest_diff = abs((guests1 or 0) - (guests2 or 0))
                guest_score = 30 if guest_diff <= 2 else max(0, 30 - guest_diff * 5)
                bed_bath_diff = abs((beds1 or 0) - (beds2 or 0)) + abs((bath1 or 0) - (bath2 or 0))
                bed_bath_score = max(0, 30 - bed_bath_diff * 5)
                property_sim = bedroom_match + guest_score + bed_bath_score
                
                # 3. Quality Similarity (0-100)
                if rating1 and rating2:
                    rating_diff = abs(rating1 - rating2)
                    quality_sim = max(0, 100 - rating_diff * 20)
                else:
                    quality_sim = 50  # Neutral if ratings missing
                
                # 4. Amenity Similarity (0-100)
                if amenity1 and amenity2:
                    amenity_diff = abs(amenity1 - amenity2)
                    amenity_sim = max(0, 100 - amenity_diff * 2)
                else:
                    amenity_sim = 50
                
                # 5. Price Similarity (0-100)
                if price1 and price2 and price1 > 0:
                    price_diff_pct = abs(price1 - price2) / price1 * 100
                    price_sim = max(0, 100 - price_diff_pct * 2)
                else:
                    price_sim = 50
                
                # Calculate overall similarity (weighted)
                overall_sim = float(
                    location_sim * 0.35 +
                    property_sim * 0.25 +
                    quality_sim * 0.20 +
                    amenity_sim * 0.10 +
                    price_sim * 0.10
                )
                
                listing_similarities.append({
                    'listing_key': key1,
                    'competitor_key': key2,
                    'overall_similarity': overall_sim,
                    'location_similarity': location_sim,
                    'property_similarity': float(property_sim),
                    'quality_similarity': float(quality_sim),
                    'amenity_similarity': float(amenity_sim),
                    'price_similarity': float(price_sim),
                    'competitor_price': price2
                })
            
            # Sort by similarity and take top 25
            listing_similarities.sort(key=lambda x: x['overall_similarity'], reverse=True)
            top_25 = listing_similarities[:25]
            
            # Add rank and weight
            total_similarity = sum(c['overall_similarity'] for c in top_25)
            for rank, competitor in enumerate(top_25, 1):
                competitor['rank'] = rank
                competitor['weight'] = float(competitor['overall_similarity'] / total_similarity if total_similarity > 0 else 1/25)
                similarities.append(competitor)
            
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1}/{len(listings)} listings")
        
        logger.info(f"Calculated {len(similarities)} competitor relationships")
        
        # Load into bridge table
        self.load_bridge_listing_competitors(similarities)
    
    def load_bridge_listing_competitors(self, similarities: List[Dict]):
        """
        Load competitor relationships into bridge table.
        
        Parameters
        ----------
        similarities : list of dict
            List of competitor relationships with similarity scores
        """
        logger.info("Loading bridge_listing_competitors...")
        
        insert_query = """
            INSERT INTO bridge_listing_competitors (
                listing_key, competitor_listing_key, similarity_rank,
                overall_similarity_score, location_similarity, property_similarity,
                quality_similarity, amenity_similarity, price_similarity, weight
            ) VALUES %s
            ON CONFLICT (listing_key, competitor_listing_key) DO UPDATE SET
                similarity_rank = EXCLUDED.similarity_rank,
                overall_similarity_score = EXCLUDED.overall_similarity_score,
                last_updated = CURRENT_TIMESTAMP
        """
        
        values = [
            (
                s['listing_key'], s['competitor_key'], s['rank'],
                round(s['overall_similarity'], 2),
                round(s['location_similarity'], 2),
                round(s['property_similarity'], 2),
                round(s['quality_similarity'], 2),
                round(s['amenity_similarity'], 2),
                round(s['price_similarity'], 2),
                round(s['weight'], 4)
            )
            for s in similarities
        ]
        
        execute_values(self.target_cursor, insert_query, values)
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} competitor relationships")
    
    def load_fact_competitor_pricing_analysis(self):
        """
        Load competitor pricing analysis fact table.
        
        Aggregates competitor prices and calculates:
        - Statistical measures (avg, median, percentiles)
        - Weighted average price
        - Price recommendations (optimal, lower, upper bounds)
        """
        logger.info("Loading fact_competitor_pricing_analysis...")
        
        # Get today's date_key
        today = datetime.now()
        date_key = int(today.strftime('%Y%m%d'))
        
        # Calculate pricing statistics per listing
        self.target_cursor.execute("""
            WITH competitor_prices AS (
                SELECT 
                    b.listing_key,
                    f.price_per_night as competitor_price,
                    b.overall_similarity_score,
                    b.weight
                FROM bridge_listing_competitors b
                JOIN fact_listing_metrics f ON b.competitor_listing_key = f.listing_key
                WHERE b.is_active = TRUE
            )
            SELECT 
                listing_key,
                COUNT(*) as competitor_count,
                AVG(competitor_price) as avg_price,
                MIN(competitor_price) as min_price,
                MAX(competitor_price) as max_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY competitor_price) as median_price,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY competitor_price) as p25_price,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY competitor_price) as p75_price,
                SUM(competitor_price * weight) as weighted_avg_price
            FROM competitor_prices
            GROUP BY listing_key
        """)
        
        pricing_stats = self.target_cursor.fetchall()
        logger.info(f"Calculated pricing stats for {len(pricing_stats)} listings")
        
        # Get current prices for comparison
        self.target_cursor.execute("""
            SELECT listing_key, price_per_night, listing_rating
            FROM fact_listing_metrics
        """)
        current_prices = {key: (price, rating) for key, price, rating in self.target_cursor.fetchall()}
        
        # Transform and load
        insert_query = """
            INSERT INTO fact_competitor_pricing_analysis (
                listing_key, analysis_date_key, competitor_count,
                avg_competitor_price, min_competitor_price, max_competitor_price,
                median_competitor_price, percentile_25_price, percentile_75_price,
                weighted_avg_price, price_premium_discount,
                recommended_price_lower, recommended_price_upper, recommended_optimal_price
            ) VALUES %s
            ON CONFLICT (listing_key, analysis_date_key) DO UPDATE SET
                avg_competitor_price = EXCLUDED.avg_competitor_price,
                weighted_avg_price = EXCLUDED.weighted_avg_price,
                recommended_optimal_price = EXCLUDED.recommended_optimal_price,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = []
        for stats in pricing_stats:
            listing_key, count, avg, min_p, max_p, median, p25, p75, weighted = stats
            
            # Convert Decimal types to float for calculations
            avg = float(avg) if avg is not None else None
            min_p = float(min_p) if min_p is not None else None
            max_p = float(max_p) if max_p is not None else None
            median = float(median) if median is not None else None
            p25 = float(p25) if p25 is not None else None
            p75 = float(p75) if p75 is not None else None
            weighted = float(weighted) if weighted is not None else None
            
            current_price, rating = current_prices.get(listing_key, (None, None))
            current_price = float(current_price) if current_price is not None else None
            rating = float(rating) if rating is not None else None
            
            # Calculate premium/discount
            premium = None
            if current_price and weighted and weighted > 0:
                premium = ((current_price - weighted) / weighted) * 100
            
            # Calculate recommendations
            lower_bound = p25 * 0.95 if p25 else None
            upper_bound = p75 * 1.05 if p75 else None
            
            # Optimal price with quality adjustment
            optimal = weighted
            if optimal and rating:
                quality_factor = min(max(rating / 4.5, 0.85), 1.15)  # ±15% adjustment
                optimal = optimal * quality_factor
            
            values.append((
                listing_key, date_key, count,
                round(avg, 2) if avg else None,
                round(min_p, 2) if min_p else None,
                round(max_p, 2) if max_p else None,
                round(median, 2) if median else None,
                round(p25, 2) if p25 else None,
                round(p75, 2) if p75 else None,
                round(weighted, 2) if weighted else None,
                round(premium, 2) if premium else None,
                round(lower_bound, 2) if lower_bound else None,
                round(upper_bound, 2) if upper_bound else None,
                round(optimal, 2) if optimal else None
            ))
        
        execute_values(self.target_cursor, insert_query, values)
        self.target_conn.commit()
        logger.info(f"Loaded {len(values)} pricing analyses")
    
    def refresh_materialized_views(self):
        """Refresh all materialized views in the target database."""
        logger.info("Refreshing materialized views...")
        
        self.target_cursor.execute("REFRESH MATERIALIZED VIEW view_top_competitors")
        self.target_conn.commit()
        
        logger.info("Materialized views refreshed")
    
    # ========================================================================
    # ORCHESTRATION
    # ========================================================================
    
    def run_full_etl(self):
        """
        Execute complete ETL pipeline from normalized to dimensional model.
        
        Steps:
        1. Load all dimension tables
        2. Load central fact table
        3. Load aggregate fact tables
        4. Calculate competitor similarities
        5. Load competitor pricing analysis
        6. Refresh materialized views
        """
        start_time = datetime.now()
        logger.info("="*70)
        logger.info("Starting ETL: Normalized → Dimensional")
        logger.info("="*70)
        
        try:
            self.connect()
            
            # Step 1: Load Dimensions
            logger.info("\n--- PHASE 1: Loading Dimensions ---")
            self.load_dim_host()
            self.load_dim_property()
            self.load_dim_location()
            self.load_dim_category_ratings()
            
            # Step 2: Load Central Fact
            logger.info("\n--- PHASE 2: Loading Central Fact ---")
            self.load_fact_listing_metrics()
            
            # Step 3: Load Aggregate Facts
            logger.info("\n--- PHASE 3: Loading Aggregate Facts ---")
            self.load_fact_listing_amenities_summary()
            
            # Step 4: Competitor Analysis
            logger.info("\n--- PHASE 4: Competitor Analysis ---")
            self.calculate_competitor_similarity()
            
            # Step 5: Pricing Analysis
            logger.info("\n--- PHASE 5: Pricing Analysis ---")
            self.load_fact_competitor_pricing_analysis()
            
            # Step 6: Refresh Views
            logger.info("\n--- PHASE 6: Finalizing ---")
            self.refresh_materialized_views()
            
            elapsed = datetime.now() - start_time
            logger.info("="*70)
            logger.info(f"ETL completed successfully in {elapsed}")
            logger.info("="*70)
            
        except Exception as e:
            logger.error(f"ETL failed: {e}")
            if self.target_conn:
                self.target_conn.rollback()
            raise
        finally:
            self.disconnect()


def main():
    """
    Main execution function.
    
    Environment Variables Required
    ------------------------------
    DB_HOST : PostgreSQL host
    DB_USER : Database user
    DB_PASSWORD : Database password
    DB_PORT : PostgreSQL port
    SOURCE_DB_NAME : Source database (normalized schema)
    TARGET_DB_NAME : Target database (dimensional schema)
    """
    # Source database configuration (normalized schema)
    source_db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('SOURCE_DB_NAME', 'airbnb_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', '5432'))
    }
    
    # Target database configuration (dimensional schema)
    target_db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('TARGET_DB_NAME', 'airbnb_dimensional'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', '5432'))
    }
    
    # Validate password
    if not source_db_config['password']:
        logger.error("DB_PASSWORD not found in environment variables!")
        logger.error("Please create a .env file with DB_PASSWORD=your_password")
        raise ValueError("DB_PASSWORD environment variable is required")
    
    # Run ETL
    etl = DimensionalETL(source_db_config, target_db_config)
    etl.run_full_etl()


if __name__ == '__main__':
    main()
