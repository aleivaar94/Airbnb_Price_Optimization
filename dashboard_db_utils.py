"""
Database Utilities for Airbnb Competitor Analysis Dashboard
============================================================

This module provides database connection and query functions for the Streamlit
dashboard, accessing the dimensional database for property, competitor, and
pricing data.

Functions
---------
create_connection : Create PostgreSQL database connection
get_property_list : Get list of all properties with filter options
get_property_overview : Get comprehensive property details
get_top_competitors : Get top 25 competitors for a property
get_pricing_analysis : Get pricing analysis and recommendations
close_connection : Close database connection

Environment Variables Required
------------------------------
DB_HOST : str
    PostgreSQL host address
DB_PORT : int
    PostgreSQL port number
DB_NAME : str
    Target database name (dimensional schema)
DB_USER : str
    Database user
DB_PASSWORD : str
    Database password

Example
-------
>>> import dashboard_db_utils as db_utils
>>> conn = db_utils.create_connection()
>>> properties = db_utils.get_property_list(conn)
>>> property_data = db_utils.get_property_overview(conn, property_id='123')
>>> db_utils.close_connection(conn)
"""

import os
import psycopg2
import pandas as pd
import streamlit as st
from typing import Optional, Tuple, Dict, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def create_connection() -> Optional[psycopg2.extensions.connection]:
    """
    Create and return a PostgreSQL database connection.
    
    Uses environment variables for connection parameters. Connection is
    configured for the dimensional database (airbnb_dimensional).
    
    Returns
    -------
    psycopg2.connection or None
        Database connection object if successful, None otherwise
    
    Environment Variables
    --------------------
    DB_HOST : str
        Database host (default: localhost)
    DB_PORT : int
        Database port (default: 5432)
    TARGET_DB_NAME : str
        Database name (default: airbnb_dimensional)
    DB_USER : str
        Database user (default: postgres)
    DB_PASSWORD : str
        Database password (required)
    
    Example
    -------
    >>> conn = create_connection()
    >>> if conn:
    ...     print("Connected successfully")
    
    Notes
    -----
    This function uses the TARGET_DB_NAME environment variable to connect
    to the dimensional database, not the normalized source database.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('TARGET_DB_NAME', 'airbnb_dimensional'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except psycopg2.Error as e:
        st.error(f"Database connection failed: {e}")
        return None


def close_connection(conn: psycopg2.extensions.connection) -> None:
    """
    Close database connection.
    
    Parameters
    ----------
    conn : psycopg2.connection
        Active database connection to close
    
    Example
    -------
    >>> conn = create_connection()
    >>> close_connection(conn)
    """
    if conn:
        conn.close()


@st.cache_data(ttl=300)
def get_property_list(_conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Get list of all properties with key identifiers for filtering.
    
    Retrieves property_id, listing_title, name, and url for all properties
    in the dimensional database. Used to populate filter dropdowns.
    
    Parameters
    ----------
    _conn : psycopg2.connection
        Active database connection (prefixed with _ to prevent Streamlit hashing)
    
    Returns
    -------
    pd.DataFrame
        DataFrame with columns: property_id, listing_title, name, url
    
    Example
    -------
    >>> conn = create_connection()
    >>> properties = get_property_list(conn)
    >>> print(properties[['property_id', 'listing_title']].head())
    
    Notes
    -----
    Results are cached for 5 minutes (ttl=300) to improve performance.
    The underscore prefix on _conn prevents Streamlit from trying to hash
    the database connection object.
    """
    query = """
        SELECT DISTINCT
            p.property_id,
            p.listing_title,
            p.name,
            p.url
        FROM dim_property p
        JOIN fact_listing_metrics f ON p.property_key = f.property_key
        ORDER BY p.listing_title
    """
    
    try:
        df = pd.read_sql_query(query, _conn)
        return df
    except Exception as e:
        st.error(f"Error fetching property list: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_property_overview(_conn: psycopg2.extensions.connection, 
                         property_id: str) -> pd.DataFrame:
    """
    Retrieve comprehensive property details from dimensional database.
    
    Queries view_listing_summary which denormalizes all dimension tables
    for complete property information including physical characteristics,
    location, host details, ratings, pricing, and amenities.
    
    Parameters
    ----------
    _conn : psycopg2.connection
        Active database connection
    property_id : str
        Unique property identifier
    
    Returns
    -------
    pd.DataFrame
        Single-row DataFrame with all property details, or empty if not found
    
    Columns Included
    ----------------
    - Property: property_id, listing_name, category, guests_capacity, bedrooms, 
                beds, baths, property_size_tier
    - Location: city, province, latitude, longitude, location_tier, 
                location_cluster_id, distance_to_downtown_km
    - Host: host_id, host_name, host_rating, is_superhost, host_tier, 
            experience_level
    - Ratings: listing_rating, number_of_reviews, cleanliness_rating,
               accuracy_rating, location_rating, value_rating,
               overall_quality_score, quality_tier
    - Pricing: price_per_night, price_per_guest, price_per_bedroom,
               competitiveness_score, value_score, popularity_index
    - Amenities: total_amenities_count, amenity_tier, amenity_score
    - Status: is_available, is_guest_favorite, pets_allowed
    
    Example
    -------
    >>> conn = create_connection()
    >>> property_data = get_property_overview(conn, '1426378005713860735')
    >>> if not property_data.empty:
    ...     print(f"Property: {property_data['listing_name'].iloc[0]}")
    ...     print(f"Price: ${property_data['price_per_night'].iloc[0]:.0f}")
    
    Notes
    -----
    Uses view_listing_summary which joins:
    - fact_listing_metrics (central fact)
    - dim_property (physical characteristics)
    - dim_location (geographic data)
    - dim_host (host reputation)
    - dim_category_ratings (quality metrics)
    - fact_listing_amenities_summary (amenity aggregates)
    """
    query = """
        SELECT 
            listing_key,
            property_id,
            listing_name,
            category,
            guests_capacity,
            bedrooms,
            beds,
            baths,
            property_size_tier,
            
            -- Location
            city,
            province,
            latitude,
            longitude,
            location_tier,
            location_cluster_id,
            distance_to_downtown_km,
            
            -- Host
            host_id,
            host_name,
            host_rating,
            is_superhost,
            host_tier,
            experience_level,
            
            -- Ratings
            listing_rating,
            number_of_reviews,
            cleanliness_rating,
            accuracy_rating,
            location_rating,
            value_rating,
            overall_quality_score,
            quality_tier,
            
            -- Pricing & Metrics
            price_per_night,
            price_per_guest,
            price_per_bedroom,
            competitiveness_score,
            value_score,
            popularity_index,
            
            -- Amenities
            total_amenities_count,
            amenity_tier,
            amenity_score,
            
            -- Status
            is_available,
            is_guest_favorite,
            pets_allowed
            
        FROM view_listing_summary
        WHERE property_id = %s
    """
    
    try:
        df = pd.read_sql_query(query, _conn, params=(property_id,))
        return df
    except Exception as e:
        st.error(f"Error fetching property overview: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_top_competitors(_conn: psycopg2.extensions.connection,
                       property_id: str) -> pd.DataFrame:
    """
    Retrieve top 25 competitors for a given property with similarity metrics.
    
    Queries view_top_competitors materialized view which contains pre-computed
    similarity scores and competitor relationships from bridge_listing_competitors.
    
    Parameters
    ----------
    _conn : psycopg2.connection
        Active database connection
    property_id : str
        Unique property identifier
    
    Returns
    -------
    pd.DataFrame
        Up to 25 rows with competitor details and similarity metrics
    
    Columns Included
    ----------------
    - Ranking: similarity_rank (1-25, where 1 is most similar)
    - Identity: competitor_property_id, competitor_name
    - Property: competitor_bedrooms, competitor_beds, competitor_baths,
                competitor_guests
    - Location: competitor_location_tier, distance_km
    - Pricing: competitor_price, price_difference, price_diff_pct
    - Ratings: competitor_rating, competitor_reviews
    - Similarity: overall_similarity_score, location_similarity,
                  property_similarity, quality_similarity, amenity_similarity,
                  price_similarity, weight
    
    Example
    -------
    >>> conn = create_connection()
    >>> competitors = get_top_competitors(conn, '1426378005713860735')
    >>> if not competitors.empty:
    ...     top_5 = competitors.head(5)
    ...     print(f"Top 5 competitors by similarity:")
    ...     print(top_5[['similarity_rank', 'competitor_name', 'overall_similarity_score']])
    
    Notes
    -----
    Similarity components are weighted:
    - Location: 35% (geographic distance + cluster)
    - Property: 25% (bedrooms, beds, baths, capacity)
    - Quality: 20% (ratings alignment)
    - Amenity: 10% (shared amenities)
    - Price: 10% (price range overlap)
    """
    query = """
        SELECT 
            vtc.similarity_rank,
            vtc.competitor_property_id,
            
            -- Competitor details
            p.listing_name as competitor_name,
            p.bedrooms as competitor_bedrooms,
            p.beds as competitor_beds,
            p.baths as competitor_baths,
            p.guests_capacity as competitor_guests,
            
            -- Location
            l.location_tier as competitor_location_tier,
            vtc.distance_km,
            
            -- Pricing
            vtc.source_price as my_price,
            vtc.competitor_price,
            (vtc.competitor_price - vtc.source_price) as price_difference,
            ROUND(((vtc.competitor_price - vtc.source_price) / 
                   NULLIF(vtc.source_price, 0) * 100), 2) as price_diff_pct,
            
            -- Ratings
            vtc.competitor_rating,
            f.number_of_reviews as competitor_reviews,
            
            -- Similarity scores
            vtc.overall_similarity_score,
            vtc.location_similarity,
            vtc.property_similarity,
            vtc.quality_similarity,
            vtc.amenity_similarity,
            vtc.price_similarity,
            vtc.weight
            
        FROM view_top_competitors vtc
        -- Join to get listing_key for source property
        JOIN fact_listing_metrics f_source ON vtc.listing_key = f_source.listing_key
        -- Join to get competitor details
        JOIN fact_listing_metrics f ON vtc.competitor_listing_key = f.listing_key
        JOIN dim_property p ON f.property_key = p.property_key
        JOIN dim_location l ON f.location_key = l.location_key
        
        WHERE f_source.property_id = %s
        ORDER BY vtc.similarity_rank
    """
    
    try:
        df = pd.read_sql_query(query, _conn, params=(property_id,))
        return df
    except Exception as e:
        st.error(f"Error fetching competitors: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_pricing_analysis(_conn: psycopg2.extensions.connection,
                        property_id: str) -> pd.DataFrame:
    """
    Retrieve detailed pricing analysis and recommendations.
    
    Queries view_price_recommendations which combines current pricing with
    competitor statistics and generates optimal price recommendations.
    
    Parameters
    ----------
    _conn : psycopg2.connection
        Active database connection
    property_id : str
        Unique property identifier
    
    Returns
    -------
    pd.DataFrame
        Single-row DataFrame with pricing analysis metrics
    
    Columns Included
    ----------------
    - Current: property_id, listing_name, current_price, listing_rating,
               number_of_reviews
    - Market: competitor_count, avg_competitor_price, median_competitor_price,
              weighted_avg_price, percentile_25_price, percentile_75_price
    - Recommendations: recommended_optimal_price, recommended_price_lower,
                      recommended_price_upper, price_premium_discount,
                      price_difference, pricing_status
    - Context: bedrooms, location_tier, analysis_date
    
    Pricing Status Values
    ---------------------
    OPTIMAL : Price within recommended range
    OVERPRICED : Price above upper bound
    UNDERPRICED : Price below lower bound
    
    Example
    -------
    >>> conn = create_connection()
    >>> pricing = get_pricing_analysis(conn, '1426378005713860735')
    >>> if not pricing.empty:
    ...     status = pricing['pricing_status'].iloc[0]
    ...     optimal = pricing['recommended_optimal_price'].iloc[0]
    ...     print(f"Pricing Status: {status}")
    ...     print(f"Recommended Price: ${optimal:.0f}")
    
    Notes
    -----
    Pricing algorithm:
    1. Calculate weighted average competitor price (using similarity weights)
    2. Apply quality adjustment factor (±15% based on rating differential)
    3. Determine price bounds (25th and 75th percentiles ± 5%)
    4. Generate optimal price recommendation
    """
    query = """
        SELECT 
            vpr.property_id,
            vpr.listing_name,
            vpr.current_price,
            vpr.listing_rating,
            vpr.number_of_reviews,
            
            -- Competitor pricing statistics
            vpr.competitor_count,
            vpr.avg_competitor_price,
            vpr.median_competitor_price,
            vpr.weighted_avg_price,
            vpr.percentile_25_price,
            vpr.percentile_75_price,
            
            -- Recommendations
            vpr.recommended_optimal_price,
            vpr.recommended_price_lower,
            vpr.recommended_price_upper,
            vpr.price_premium_discount,
            vpr.price_difference,
            vpr.pricing_status,
            
            -- Context
            vpr.bedrooms,
            vpr.location_tier,
            vpr.analysis_date
            
        FROM view_price_recommendations vpr
        WHERE vpr.property_id = %s
    """
    
    try:
        df = pd.read_sql_query(query, _conn, params=(property_id,))
        return df
    except Exception as e:
        st.error(f"Error fetching pricing analysis: {e}")
        return pd.DataFrame()


def get_connection_status() -> Dict[str, str]:
    """
    Check database connection status and return configuration info.
    
    Returns
    -------
    dict
        Dictionary with connection configuration details
    
    Keys
    ----
    database : str
        Target database name
    host : str
        Database host
    port : str
        Database port
    user : str
        Database user
    status : str
        Connection status ('Connected' or 'Failed')
    
    Example
    -------
    >>> status = get_connection_status()
    >>> print(f"Database: {status['database']}")
    >>> print(f"Status: {status['status']}")
    """
    conn = create_connection()
    status = {
        'database': os.getenv('TARGET_DB_NAME', 'airbnb_dimensional'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'user': os.getenv('DB_USER', 'postgres'),
        'status': 'Connected' if conn else 'Failed'
    }
    
    if conn:
        close_connection(conn)
    
    return status
