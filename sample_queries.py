"""
Example Analytics Queries for Airbnb Database
==============================================
Demonstrates various SQL queries for analyzing the normalized Airbnb data.
"""

# Query 1: Top 10 highest-rated listings with prices
TOP_RATED_QUERY = """
SELECT 
    l.listing_id,
    l.name,
    l.rating,
    l.price,
    l.guests,
    l.bedrooms,
    h.name as host_name,
    h.is_superhost
FROM listings l
LEFT JOIN hosts h ON l.host_id = h.host_id
WHERE l.rating IS NOT NULL
ORDER BY l.rating DESC, l.price ASC
LIMIT 10;
"""

# Query 2: Average price by number of bedrooms
PRICE_BY_BEDROOMS_QUERY = """
SELECT 
    bedrooms,
    COUNT(*) as listing_count,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(MIN(price), 2) as min_price,
    ROUND(MAX(price), 2) as max_price,
    ROUND(AVG(rating), 2) as avg_rating
FROM listings
WHERE bedrooms IS NOT NULL
GROUP BY bedrooms
ORDER BY bedrooms;
"""

# Query 3: Most common amenities across all listings
POPULAR_AMENITIES_QUERY = """
SELECT 
    ag.group_name,
    a.amenity_name,
    COUNT(DISTINCT la.listing_id) as listing_count,
    ROUND(COUNT(DISTINCT la.listing_id) * 100.0 / (SELECT COUNT(*) FROM listings), 2) as percentage
FROM amenity_groups ag
JOIN amenities a ON ag.group_id = a.group_id
JOIN listing_amenities la ON a.amenity_id = la.amenity_id
GROUP BY ag.group_name, a.amenity_name
HAVING COUNT(DISTINCT la.listing_id) >= 10
ORDER BY listing_count DESC
LIMIT 20;
"""

# Query 4: Superhosts performance comparison
SUPERHOST_COMPARISON_QUERY = """
SELECT 
    h.is_superhost,
    COUNT(DISTINCT h.host_id) as host_count,
    COUNT(l.listing_id) as total_listings,
    ROUND(AVG(l.rating), 2) as avg_listing_rating,
    ROUND(AVG(l.price), 2) as avg_price,
    ROUND(AVG(h.response_rate), 2) as avg_response_rate,
    AVG(l.number_of_reviews) as avg_reviews
FROM hosts h
LEFT JOIN listings l ON h.host_id = l.host_id
GROUP BY h.is_superhost
ORDER BY h.is_superhost DESC;
"""

# Query 5: Listings with best value (high rating, low price)
BEST_VALUE_QUERY = """
SELECT 
    l.name,
    l.price,
    l.rating,
    l.guests,
    l.bedrooms,
    (l.rating / NULLIF(l.price, 0)) as value_score,
    COUNT(r.review_id) as review_count
FROM listings l
LEFT JOIN listing_reviews r ON l.listing_id = r.listing_id
WHERE l.rating >= 4.5 AND l.price IS NOT NULL AND l.price > 0
GROUP BY l.listing_id, l.name, l.price, l.rating, l.guests, l.bedrooms
ORDER BY value_score DESC
LIMIT 15;
"""

# Query 6: Review sentiment over time
REVIEWS_OVER_TIME_QUERY = """
SELECT 
    DATE_TRUNC('month', r.review_date) as month,
    COUNT(*) as review_count,
    ROUND(AVG(r.rating), 2) as avg_rating,
    COUNT(DISTINCT r.listing_id) as listings_reviewed
FROM listing_reviews r
WHERE r.review_date IS NOT NULL
GROUP BY DATE_TRUNC('month', r.review_date)
ORDER BY month DESC;
"""

# Query 7: Category ratings breakdown
CATEGORY_RATINGS_QUERY = """
SELECT 
    cr.category_name,
    ROUND(AVG(cr.rating_value), 2) as avg_rating,
    ROUND(MIN(cr.rating_value), 2) as min_rating,
    ROUND(MAX(cr.rating_value), 2) as max_rating,
    COUNT(DISTINCT cr.listing_id) as listing_count
FROM listing_category_ratings cr
GROUP BY cr.category_name
ORDER BY avg_rating DESC;
"""

# Query 8: Listings by location with amenity counts
LOCATION_AMENITY_QUERY = """
SELECT 
    l.location,
    COUNT(DISTINCT l.listing_id) as listing_count,
    ROUND(AVG(l.price), 2) as avg_price,
    ROUND(AVG(l.rating), 2) as avg_rating,
    COUNT(DISTINCT la.amenity_id) as unique_amenities
FROM listings l
LEFT JOIN listing_amenities la ON l.listing_id = la.listing_id
GROUP BY l.location
ORDER BY listing_count DESC;
"""

# Query 9: Hosts with multiple properties
MULTI_PROPERTY_HOSTS_QUERY = """
SELECT 
    h.name,
    h.rating as host_rating,
    h.is_superhost,
    COUNT(l.listing_id) as property_count,
    ROUND(AVG(l.rating), 2) as avg_property_rating,
    ROUND(AVG(l.price), 2) as avg_price,
    SUM(l.number_of_reviews) as total_reviews
FROM hosts h
JOIN listings l ON h.host_id = l.host_id
GROUP BY h.host_id, h.name, h.rating, h.is_superhost
HAVING COUNT(l.listing_id) > 1
ORDER BY property_count DESC, avg_property_rating DESC;
"""

# Query 10: Correlation between amenities and ratings
AMENITY_RATING_CORRELATION_QUERY = """
SELECT 
    ag.group_name,
    COUNT(DISTINCT la.listing_id) as listings_with_amenity,
    ROUND(AVG(l.rating), 2) as avg_rating_with_amenity,
    (SELECT ROUND(AVG(rating), 2) FROM listings WHERE rating IS NOT NULL) as overall_avg_rating,
    ROUND(AVG(l.rating) - (SELECT AVG(rating) FROM listings WHERE rating IS NOT NULL), 2) as rating_difference
FROM amenity_groups ag
JOIN amenities a ON ag.group_id = a.group_id
JOIN listing_amenities la ON a.amenity_id = la.amenity_id
JOIN listings l ON la.listing_id = l.listing_id
WHERE l.rating IS NOT NULL
GROUP BY ag.group_name
HAVING COUNT(DISTINCT la.listing_id) >= 5
ORDER BY rating_difference DESC;
"""


def execute_sample_queries(connection):
    """
    Execute all sample queries and display results.
    
    Parameters
    ----------
    connection : psycopg2.connection
        Active database connection
    
    Example
    -------
    >>> import psycopg2
    >>> conn = psycopg2.connect(
    ...     host='localhost',
    ...     database='airbnb_db',
    ...     user='postgres',
    ...     password='password'
    ... )
    >>> execute_sample_queries(conn)
    """
    import psycopg2.extras
    
    queries = {
        'Top Rated Listings': TOP_RATED_QUERY,
        'Price by Bedrooms': PRICE_BY_BEDROOMS_QUERY,
        'Popular Amenities': POPULAR_AMENITIES_QUERY,
        'Superhost Comparison': SUPERHOST_COMPARISON_QUERY,
        'Best Value Listings': BEST_VALUE_QUERY,
        'Reviews Over Time': REVIEWS_OVER_TIME_QUERY,
        'Category Ratings': CATEGORY_RATINGS_QUERY,
        'Location Analysis': LOCATION_AMENITY_QUERY,
        'Multi-Property Hosts': MULTI_PROPERTY_HOSTS_QUERY,
        'Amenity-Rating Correlation': AMENITY_RATING_CORRELATION_QUERY
    }
    
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    for query_name, query in queries.items():
        print(f"\n{'='*80}")
        print(f"Query: {query_name}")
        print(f"{'='*80}")
        
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            
            if results:
                # Print column headers
                headers = results[0].keys()
                print(" | ".join(headers))
                print("-" * 80)
                
                # Print rows
                for row in results[:10]:  # Limit to first 10 rows
                    print(" | ".join(str(row[col]) for col in headers))
                
                if len(results) > 10:
                    print(f"\n... ({len(results) - 10} more rows)")
            else:
                print("No results found.")
                
        except Exception as e:
            print(f"Error executing query: {e}")
    
    cursor.close()


if __name__ == '__main__':
    import psycopg2
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'airbnb_db',
        'user': 'postgres',
        'password': 'your_password',  # UPDATE THIS
        'port': 5432
    }
    
    # Connect and run queries
    try:
        conn = psycopg2.connect(**db_config)
        execute_sample_queries(conn)
        conn.close()
    except Exception as e:
        print(f"Failed to connect to database: {e}")
