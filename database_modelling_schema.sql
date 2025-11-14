-- ============================================================================
-- Airbnb Listings Dimensional Database Schema (Star Schema)
-- ============================================================================
-- Purpose: Dimensional model optimized for competitor analysis and price optimization
-- Architecture: Star schema with fact tables, dimensions, and bridge tables
-- Use Case: 
--   1. Identify top 25 competitors for any listing based on similarity scores
--   2. Calculate optimal pricing based on competitor analysis
--   3. Enable multi-dimensional business intelligence queries
-- ============================================================================

-- ============================================================================
-- DROP EXISTING OBJECTS (in reverse dependency order)
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS view_top_competitors CASCADE;
DROP VIEW IF EXISTS view_price_recommendations CASCADE;
DROP VIEW IF EXISTS view_listing_summary CASCADE;

DROP TABLE IF EXISTS fact_competitor_pricing_analysis CASCADE;
DROP TABLE IF EXISTS bridge_listing_competitors CASCADE;
DROP TABLE IF EXISTS fact_listing_amenities_summary CASCADE;
DROP TABLE IF EXISTS fact_listing_metrics CASCADE;

DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_category_ratings CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_property CASCADE;
DROP TABLE IF EXISTS dim_host CASCADE;

DROP FUNCTION IF EXISTS calculate_distance_km(DECIMAL, DECIMAL, DECIMAL, DECIMAL) CASCADE;

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

-- Calculate distance between two coordinates using Haversine formula
-- Returns distance in kilometers
CREATE OR REPLACE FUNCTION calculate_distance_km(
    lat1 DECIMAL, 
    lon1 DECIMAL, 
    lat2 DECIMAL, 
    lon2 DECIMAL
)
RETURNS DECIMAL AS $$
DECLARE
    earth_radius_km DECIMAL := 6371.0;
    dlat DECIMAL;
    dlon DECIMAL;
    a DECIMAL;
    c DECIMAL;
BEGIN
    dlat := RADIANS(lat2 - lat1);
    dlon := RADIANS(lon2 - lon1);
    
    a := SIN(dlat/2) * SIN(dlat/2) + 
         COS(RADIANS(lat1)) * COS(RADIANS(lat2)) * 
         SIN(dlon/2) * SIN(dlon/2);
    
    c := 2 * ATAN2(SQRT(a), SQRT(1-a));
    
    RETURN earth_radius_km * c;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_host: Host quality and reputation metrics
-- ----------------------------------------------------------------------------
CREATE TABLE dim_host (
    host_key SERIAL PRIMARY KEY,
    host_id TEXT UNIQUE NOT NULL,
    host_name TEXT,
    host_rating DECIMAL(3, 2),
    host_number_of_reviews INTEGER DEFAULT 0,
    host_response_rate INTEGER,
    host_response_time TEXT,
    host_years_hosting INTEGER,
    languages TEXT,
    my_work TEXT,
    image_url TEXT,
    profile_url TEXT,
    is_superhost BOOLEAN DEFAULT FALSE,
    
    -- Calculated columns (populated during ETL)
    host_tier TEXT,  -- 'Elite', 'Premium', 'Standard'
    experience_level TEXT,  -- 'Expert', 'Experienced', 'New'
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_host IS 'Host dimension: quality and reputation metrics for property managers';
COMMENT ON COLUMN dim_host.host_tier IS 'CALCULATED: Elite (superhost + rating>4.8), Premium (rating>4.5), Standard';
COMMENT ON COLUMN dim_host.experience_level IS 'CALCULATED: Expert (years>5), Experienced (years>2), New';

-- ----------------------------------------------------------------------------
-- dim_property: Physical property characteristics
-- ----------------------------------------------------------------------------
CREATE TABLE dim_property (
    property_key SERIAL PRIMARY KEY,
    property_id TEXT UNIQUE NOT NULL,
    name TEXT,
    listing_name TEXT,
    listing_title TEXT,
    category TEXT,
    url TEXT,
    description TEXT,
    guests_capacity INTEGER,
    bedrooms INTEGER,
    beds INTEGER,
    baths INTEGER,
    pets_allowed BOOLEAN DEFAULT FALSE,
    is_guest_favorite BOOLEAN DEFAULT FALSE,
    
    -- Calculated columns (populated during ETL)
    property_size_tier TEXT,  -- 'Studio', 'Small', 'Medium', 'Large'
    guest_per_bedroom_ratio DECIMAL(4, 2),  -- guests/bedrooms
    bath_to_bedroom_ratio DECIMAL(3, 2),  -- baths/bedrooms (luxury indicator)
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_property IS 'Property dimension: physical characteristics and capacity';
COMMENT ON COLUMN dim_property.property_size_tier IS 'CALCULATED: Studio (bed=0), Small (bed=1), Medium (bed=2-3), Large (bed>=4)';
COMMENT ON COLUMN dim_property.guest_per_bedroom_ratio IS 'CALCULATED: Space efficiency metric';
COMMENT ON COLUMN dim_property.bath_to_bedroom_ratio IS 'CALCULATED: Luxury level indicator';

-- ----------------------------------------------------------------------------
-- dim_location: Geographic positioning and neighborhood clustering
-- ----------------------------------------------------------------------------
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    city TEXT,
    province TEXT,
    country TEXT,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    neighborhood TEXT,
    
    -- Calculated columns (populated during ETL)
    location_cluster_id INTEGER,  -- K-means cluster (calculated externally)
    distance_to_downtown_km DECIMAL(5, 2),  -- Distance to Calgary downtown
    location_tier TEXT,  -- 'Urban Core', 'Downtown Adjacent', 'Neighborhood', 'Suburban'
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique location combinations
    UNIQUE(latitude, longitude)
);

COMMENT ON TABLE dim_location IS 'Location dimension: geographic positioning and clustering';
COMMENT ON COLUMN dim_location.location_cluster_id IS 'CALCULATED: K-means cluster ID (computed in Python/R during ETL)';
COMMENT ON COLUMN dim_location.distance_to_downtown_km IS 'CALCULATED: Distance to Calgary downtown (51.0447, -114.0719)';
COMMENT ON COLUMN dim_location.location_tier IS 'CALCULATED: Urban Core (<1km), Downtown Adjacent (1-3km), Neighborhood (3-7km), Suburban (>7km)';

-- ----------------------------------------------------------------------------
-- dim_category_ratings: Guest experience quality metrics
-- ----------------------------------------------------------------------------
CREATE TABLE dim_category_ratings (
    rating_key SERIAL PRIMARY KEY,
    cleanliness_rating DECIMAL(3, 2),
    accuracy_rating DECIMAL(3, 2),
    checkin_rating DECIMAL(3, 2),
    communication_rating DECIMAL(3, 2),
    location_rating DECIMAL(3, 2),
    value_rating DECIMAL(3, 2),
    
    -- Calculated columns (populated during ETL)
    overall_quality_score DECIMAL(3, 2),  -- Weighted average
    quality_tier TEXT,  -- 'Exceptional', 'Excellent', 'Good', 'Fair'
    value_index DECIMAL(4, 2),  -- value_rating / overall_quality_score
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_category_ratings IS 'Category ratings dimension: detailed guest experience metrics';
COMMENT ON COLUMN dim_category_ratings.overall_quality_score IS 'CALCULATED: Weighted avg (cleanliness×0.25 + accuracy×0.15 + checkin×0.10 + communication×0.15 + location×0.15 + value×0.20)';
COMMENT ON COLUMN dim_category_ratings.quality_tier IS 'CALCULATED: Exceptional (>4.8), Excellent (>4.5), Good (>4.0), Fair';
COMMENT ON COLUMN dim_category_ratings.value_index IS 'CALCULATED: Identifies good deals (value_rating / overall_quality_score)';

-- ----------------------------------------------------------------------------
-- dim_date: Time intelligence and seasonality
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    season TEXT  -- Calgary-specific seasons
);

COMMENT ON TABLE dim_date IS 'Date dimension: time intelligence for temporal analysis';
COMMENT ON COLUMN dim_date.season IS 'Calgary seasons: Stampede (Jul), Summer Peak (Jun-Aug), Winter (Dec-Feb), Spring/Fall';

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- fact_listing_metrics: Central fact table (one row per listing per snapshot)
-- ----------------------------------------------------------------------------
CREATE TABLE fact_listing_metrics (
    listing_key SERIAL PRIMARY KEY,
    property_id TEXT NOT NULL,  -- Business key
    
    -- Foreign keys to dimensions
    host_key INTEGER REFERENCES dim_host(host_key),
    property_key INTEGER REFERENCES dim_property(property_key),
    location_key INTEGER REFERENCES dim_location(location_key),
    rating_key INTEGER REFERENCES dim_category_ratings(rating_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Base measures
    price_per_night DECIMAL(10, 2),
    currency TEXT,
    listing_rating DECIMAL(3, 2),
    number_of_reviews INTEGER DEFAULT 0,
    is_available BOOLEAN DEFAULT TRUE,
    
    -- Calculated measures (populated during ETL)
    price_per_guest DECIMAL(10, 2),  -- price_per_night / guests
    price_per_bedroom DECIMAL(10, 2),  -- price_per_night / bedrooms
    price_per_bed DECIMAL(10, 2),  -- price_per_night / beds
    review_velocity DECIMAL(6, 2),  -- number_of_reviews / days_since_created
    competitiveness_score DECIMAL(5, 2),  -- Composite score (0-100)
    value_score DECIMAL(5, 2),  -- Quality vs price metric (0-100)
    popularity_index DECIMAL(6, 2),  -- (reviews × rating) / segment_average
    
    -- Metadata
    data_scraped_at TIMESTAMP,
    snapshot_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE fact_listing_metrics IS 'Central fact table: listing performance metrics with dimensional context';
COMMENT ON COLUMN fact_listing_metrics.price_per_guest IS 'CALCULATED: price_per_night / guests_capacity';
COMMENT ON COLUMN fact_listing_metrics.price_per_bedroom IS 'CALCULATED: price_per_night / bedrooms';
COMMENT ON COLUMN fact_listing_metrics.price_per_bed IS 'CALCULATED: price_per_night / beds';
COMMENT ON COLUMN fact_listing_metrics.review_velocity IS 'CALCULATED: Reviews per day since listing created';
COMMENT ON COLUMN fact_listing_metrics.competitiveness_score IS 'CALCULATED: Composite (rating, reviews, host_quality, amenities) 0-100';
COMMENT ON COLUMN fact_listing_metrics.value_score IS 'CALCULATED: Quality metrics / price (normalized 0-100)';
COMMENT ON COLUMN fact_listing_metrics.popularity_index IS 'CALCULATED: (number_of_reviews × rating) / segment_average';

-- ----------------------------------------------------------------------------
-- fact_listing_amenities_summary: Aggregate amenity metrics per listing
-- ----------------------------------------------------------------------------
CREATE TABLE fact_listing_amenities_summary (
    amenity_summary_key SERIAL PRIMARY KEY,
    listing_key INTEGER REFERENCES fact_listing_metrics(listing_key) ON DELETE CASCADE,
    
    -- Amenity counts
    total_amenities_count INTEGER DEFAULT 0,
    essential_amenities_count INTEGER DEFAULT 0,  -- Wifi, kitchen, parking, AC, heating
    luxury_amenities_count INTEGER DEFAULT 0,  -- Pool, gym, hot tub, EV charger
    safety_amenities_count INTEGER DEFAULT 0,  -- Smoke/CO detectors, first aid
    
    -- Calculated measures (populated during ETL)
    amenity_score INTEGER,  -- Weighted sum (essential×2 + luxury×3 + safety×1)
    amenity_tier TEXT,  -- 'Luxury', 'Premium', 'Standard', 'Basic'
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(listing_key)
);

COMMENT ON TABLE fact_listing_amenities_summary IS 'Aggregate fact: quantified amenity metrics for comparison';
COMMENT ON COLUMN fact_listing_amenities_summary.amenity_score IS 'CALCULATED: Weighted sum (essential×2 + luxury×3 + safety×1)';
COMMENT ON COLUMN fact_listing_amenities_summary.amenity_tier IS 'CALCULATED: Luxury (score>50), Premium (>30), Standard (>15), Basic';

-- ----------------------------------------------------------------------------
-- fact_competitor_pricing_analysis: Aggregated competitor pricing metrics
-- ----------------------------------------------------------------------------
CREATE TABLE fact_competitor_pricing_analysis (
    pricing_analysis_key SERIAL PRIMARY KEY,
    listing_key INTEGER REFERENCES fact_listing_metrics(listing_key) ON DELETE CASCADE,
    analysis_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Competitor statistics
    competitor_count INTEGER DEFAULT 25,
    avg_competitor_price DECIMAL(10, 2),
    min_competitor_price DECIMAL(10, 2),
    max_competitor_price DECIMAL(10, 2),
    median_competitor_price DECIMAL(10, 2),
    percentile_25_price DECIMAL(10, 2),
    percentile_75_price DECIMAL(10, 2),
    
    -- Calculated measures (populated during ETL)
    weighted_avg_price DECIMAL(10, 2),  -- Weighted by similarity scores
    price_premium_discount DECIMAL(5, 2),  -- % difference from weighted avg
    recommended_price_lower DECIMAL(10, 2),  -- Lower bound recommendation
    recommended_price_upper DECIMAL(10, 2),  -- Upper bound recommendation
    recommended_optimal_price DECIMAL(10, 2),  -- Optimal price recommendation
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(listing_key, analysis_date_key)
);

COMMENT ON TABLE fact_competitor_pricing_analysis IS 'Aggregate fact: competitor pricing statistics and recommendations';
COMMENT ON COLUMN fact_competitor_pricing_analysis.weighted_avg_price IS 'CALCULATED: Average weighted by similarity scores';
COMMENT ON COLUMN fact_competitor_pricing_analysis.price_premium_discount IS 'CALCULATED: (current_price - weighted_avg) / weighted_avg × 100';
COMMENT ON COLUMN fact_competitor_pricing_analysis.recommended_optimal_price IS 'CALCULATED: Weighted_avg × quality_adjustment_factor';

-- ============================================================================
-- BRIDGE TABLE (Many-to-Many Relationships)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- bridge_listing_competitors: Pre-computed top 25 competitors per listing
-- ----------------------------------------------------------------------------
CREATE TABLE bridge_listing_competitors (
    bridge_key SERIAL PRIMARY KEY,
    listing_key INTEGER REFERENCES fact_listing_metrics(listing_key) ON DELETE CASCADE,
    competitor_listing_key INTEGER REFERENCES fact_listing_metrics(listing_key) ON DELETE CASCADE,
    
    -- Ranking and scoring
    similarity_rank INTEGER NOT NULL CHECK (similarity_rank BETWEEN 1 AND 25),
    overall_similarity_score DECIMAL(5, 2) NOT NULL,  -- 0-100 scale
    
    -- Component similarity scores (populated during ETL)
    location_similarity DECIMAL(5, 2),  -- Based on distance & cluster
    property_similarity DECIMAL(5, 2),  -- Bedroom/bed/bath match
    quality_similarity DECIMAL(5, 2),  -- Rating alignment
    amenity_similarity DECIMAL(5, 2),  -- Jaccard coefficient
    price_similarity DECIMAL(5, 2),  -- Price range overlap
    
    -- Weighting for calculations
    weight DECIMAL(4, 3),  -- Sum of weights = 1.0 per listing
    
    -- Status flags
    is_active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique competitor rankings per listing
    UNIQUE(listing_key, competitor_listing_key),
    UNIQUE(listing_key, similarity_rank),
    
    -- Prevent self-referencing
    CHECK (listing_key != competitor_listing_key)
);

COMMENT ON TABLE bridge_listing_competitors IS 'Bridge table: many-to-many competitor relationships with similarity scores';
COMMENT ON COLUMN bridge_listing_competitors.overall_similarity_score IS 'CALCULATED: Composite (location×0.35 + property×0.25 + quality×0.20 + amenity×0.10 + price×0.10)';
COMMENT ON COLUMN bridge_listing_competitors.location_similarity IS 'CALCULATED: Based on geographic distance and cluster membership';
COMMENT ON COLUMN bridge_listing_competitors.property_similarity IS 'CALCULATED: Bedroom/bed/bath/guest capacity match';
COMMENT ON COLUMN bridge_listing_competitors.quality_similarity IS 'CALCULATED: Rating and quality tier alignment';
COMMENT ON COLUMN bridge_listing_competitors.amenity_similarity IS 'CALCULATED: Jaccard index of shared amenities';
COMMENT ON COLUMN bridge_listing_competitors.price_similarity IS 'CALCULATED: Price range overlap metric';

-- ============================================================================
-- INDEXES FOR QUERY PERFORMANCE
-- ============================================================================

-- dim_host indexes
CREATE INDEX idx_dim_host_host_id ON dim_host(host_id);
CREATE INDEX idx_dim_host_rating ON dim_host(host_rating);
CREATE INDEX idx_dim_host_tier ON dim_host(host_tier);
CREATE INDEX idx_dim_host_superhost ON dim_host(is_superhost);

-- dim_property indexes
CREATE INDEX idx_dim_property_property_id ON dim_property(property_id);
CREATE INDEX idx_dim_property_category ON dim_property(category);
CREATE INDEX idx_dim_property_size_tier ON dim_property(property_size_tier);
CREATE INDEX idx_dim_property_bedrooms ON dim_property(bedrooms);
CREATE INDEX idx_dim_property_guests ON dim_property(guests_capacity);
CREATE INDEX idx_dim_property_favorite ON dim_property(is_guest_favorite);

-- dim_location indexes
CREATE INDEX idx_dim_location_city ON dim_location(city);
CREATE INDEX idx_dim_location_cluster ON dim_location(location_cluster_id);
CREATE INDEX idx_dim_location_coords ON dim_location(latitude, longitude);
CREATE INDEX idx_dim_location_tier ON dim_location(location_tier);

-- dim_category_ratings indexes
CREATE INDEX idx_dim_category_quality_score ON dim_category_ratings(overall_quality_score);
CREATE INDEX idx_dim_category_quality_tier ON dim_category_ratings(quality_tier);
CREATE INDEX idx_dim_category_value_index ON dim_category_ratings(value_index);

-- dim_date indexes
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);
CREATE INDEX idx_dim_date_season ON dim_date(season);

-- fact_listing_metrics indexes
CREATE INDEX idx_fact_listing_property_id ON fact_listing_metrics(property_id);
CREATE INDEX idx_fact_listing_host_key ON fact_listing_metrics(host_key);
CREATE INDEX idx_fact_listing_property_key ON fact_listing_metrics(property_key);
CREATE INDEX idx_fact_listing_location_key ON fact_listing_metrics(location_key);
CREATE INDEX idx_fact_listing_rating_key ON fact_listing_metrics(rating_key);
CREATE INDEX idx_fact_listing_date_key ON fact_listing_metrics(date_key);
CREATE INDEX idx_fact_listing_price ON fact_listing_metrics(price_per_night);
CREATE INDEX idx_fact_listing_rating ON fact_listing_metrics(listing_rating);
CREATE INDEX idx_fact_listing_snapshot ON fact_listing_metrics(snapshot_date);
CREATE INDEX idx_fact_listing_competitiveness ON fact_listing_metrics(competitiveness_score);

-- bridge_listing_competitors indexes
CREATE INDEX idx_bridge_listing_key ON bridge_listing_competitors(listing_key);
CREATE INDEX idx_bridge_competitor_key ON bridge_listing_competitors(competitor_listing_key);
CREATE INDEX idx_bridge_rank ON bridge_listing_competitors(listing_key, similarity_rank);
CREATE INDEX idx_bridge_similarity_score ON bridge_listing_competitors(overall_similarity_score);
CREATE INDEX idx_bridge_active ON bridge_listing_competitors(is_active);

-- fact_competitor_pricing_analysis indexes
CREATE INDEX idx_pricing_listing_key ON fact_competitor_pricing_analysis(listing_key);
CREATE INDEX idx_pricing_date_key ON fact_competitor_pricing_analysis(analysis_date_key);
CREATE INDEX idx_pricing_optimal ON fact_competitor_pricing_analysis(recommended_optimal_price);

-- fact_listing_amenities_summary indexes
CREATE INDEX idx_amenity_summary_listing_key ON fact_listing_amenities_summary(listing_key);
CREATE INDEX idx_amenity_summary_tier ON fact_listing_amenities_summary(amenity_tier);
CREATE INDEX idx_amenity_summary_score ON fact_listing_amenities_summary(amenity_score);

-- ============================================================================
-- HELPER VIEWS FOR COMMON QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- view_listing_summary: Denormalized view combining fact and dimensions
-- ----------------------------------------------------------------------------
CREATE VIEW view_listing_summary AS
SELECT 
    f.listing_key,
    f.property_id,
    
    -- Property details
    p.name,
    p.listing_name,
    p.listing_title,
    p.category,
    p.url,
    p.description,
    p.guests_capacity,
    p.bedrooms,
    p.beds,
    p.baths,
    p.pets_allowed,
    p.is_guest_favorite,
    p.property_size_tier,
    
    -- Location details
    l.city,
    l.province,
    l.latitude,
    l.longitude,
    l.location_cluster_id,
    l.location_tier,
    l.distance_to_downtown_km,
    
    -- Host details
    h.host_id,
    h.host_name,
    h.host_rating,
    h.host_response_rate,
    h.host_response_time,
    h.languages,
    h.is_superhost,
    h.host_tier,
    h.experience_level,
    
    -- Rating details
    r.cleanliness_rating,
    r.accuracy_rating,
    r.location_rating,
    r.value_rating,
    r.overall_quality_score,
    r.quality_tier,
    
    -- Pricing and metrics
    f.price_per_night,
    f.currency,
    f.listing_rating,
    f.number_of_reviews,
    f.price_per_guest,
    f.price_per_bedroom,
    f.competitiveness_score,
    f.value_score,
    f.popularity_index,
    
    -- Amenities
    a.total_amenities_count,
    a.amenity_tier,
    a.amenity_score,
    
    -- Metadata
    f.data_scraped_at,
    f.snapshot_date,
    f.is_available
    
FROM fact_listing_metrics f
LEFT JOIN dim_property p ON f.property_key = p.property_key
LEFT JOIN dim_location l ON f.location_key = l.location_key
LEFT JOIN dim_host h ON f.host_key = h.host_key
LEFT JOIN dim_category_ratings r ON f.rating_key = r.rating_key
LEFT JOIN fact_listing_amenities_summary a ON f.listing_key = a.listing_key;

COMMENT ON VIEW view_listing_summary IS 'Denormalized view: complete listing profile for easy querying';

-- ----------------------------------------------------------------------------
-- view_top_competitors (Materialized): Pre-filtered top 25 competitors
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW view_top_competitors AS
SELECT 
    b.listing_key,
    b.competitor_listing_key,
    b.similarity_rank,
    b.overall_similarity_score,
    b.location_similarity,
    b.property_similarity,
    b.quality_similarity,
    b.amenity_similarity,
    b.price_similarity,
    b.weight,
    
    -- Source listing details
    f1.property_id as source_property_id,
    f1.price_per_night as source_price,
    f1.listing_rating as source_rating,
    
    -- Competitor listing details
    f2.property_id as competitor_property_id,
    f2.price_per_night as competitor_price,
    f2.listing_rating as competitor_rating,
    
    -- Location comparison
    l1.location_cluster_id as source_cluster,
    l2.location_cluster_id as competitor_cluster,
    calculate_distance_km(l1.latitude, l1.longitude, l2.latitude, l2.longitude) as distance_km
    
FROM bridge_listing_competitors b
JOIN fact_listing_metrics f1 ON b.listing_key = f1.listing_key
JOIN fact_listing_metrics f2 ON b.competitor_listing_key = f2.listing_key
JOIN dim_location l1 ON f1.location_key = l1.location_key
JOIN dim_location l2 ON f2.location_key = l2.location_key
WHERE b.is_active = TRUE
  AND b.similarity_rank <= 25
ORDER BY b.listing_key, b.similarity_rank;

CREATE INDEX idx_mv_top_competitors_listing ON view_top_competitors(listing_key);
CREATE INDEX idx_mv_top_competitors_rank ON view_top_competitors(listing_key, similarity_rank);

COMMENT ON MATERIALIZED VIEW view_top_competitors IS 'Materialized view: pre-computed top 25 competitors with comparison metrics';

-- ----------------------------------------------------------------------------
-- view_price_recommendations: Pricing analysis with context
-- ----------------------------------------------------------------------------
CREATE VIEW view_price_recommendations AS
SELECT 
    f.listing_key,
    f.property_id,
    prop.listing_name,
    
    -- Current pricing
    f.price_per_night as current_price,
    f.listing_rating,
    f.number_of_reviews,
    
    -- Location context
    l.city,
    l.location_tier,
    
    -- Property context
    prop.bedrooms,
    prop.guests_capacity,
    prop.property_size_tier,
    
    -- Competitor pricing analysis
    pricing.competitor_count,
    pricing.avg_competitor_price,
    pricing.median_competitor_price,
    pricing.weighted_avg_price,
    pricing.percentile_25_price,
    pricing.percentile_75_price,
    
    -- Recommendations
    pricing.recommended_optimal_price,
    pricing.recommended_price_lower,
    pricing.recommended_price_upper,
    pricing.price_premium_discount,
    
    -- Price difference analysis
    (f.price_per_night - pricing.recommended_optimal_price) as price_difference,
    CASE 
        WHEN f.price_per_night > pricing.recommended_price_upper THEN 'OVERPRICED'
        WHEN f.price_per_night < pricing.recommended_price_lower THEN 'UNDERPRICED'
        ELSE 'OPTIMAL'
    END as pricing_status,
    
    -- Analysis date
    d.full_date as analysis_date
    
FROM fact_listing_metrics f
JOIN dim_property prop ON f.property_key = prop.property_key
JOIN dim_location l ON f.location_key = l.location_key
LEFT JOIN fact_competitor_pricing_analysis pricing ON f.listing_key = pricing.listing_key
LEFT JOIN dim_date d ON pricing.analysis_date_key = d.date_key;

COMMENT ON VIEW view_price_recommendations IS 'Price recommendations view: combines current pricing with competitive analysis';

-- ============================================================================
-- SAMPLE HELPER FUNCTION: Populate dim_date table
-- ============================================================================

CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS INTEGER AS $$
DECLARE
    curr_date DATE := start_date;
    row_count INTEGER := 0;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO dim_date (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            week_of_year,
            day_of_week,
            day_name,
            is_weekend,
            season
        ) VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INTEGER,
            curr_date,
            EXTRACT(YEAR FROM curr_date)::INTEGER,
            EXTRACT(QUARTER FROM curr_date)::INTEGER,
            EXTRACT(MONTH FROM curr_date)::INTEGER,
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(WEEK FROM curr_date)::INTEGER,
            EXTRACT(DOW FROM curr_date)::INTEGER,
            TO_CHAR(curr_date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM curr_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            CASE 
                WHEN EXTRACT(MONTH FROM curr_date) = 7 THEN 'Stampede'
                WHEN EXTRACT(MONTH FROM curr_date) IN (6, 8) THEN 'Summer Peak'
                WHEN EXTRACT(MONTH FROM curr_date) IN (12, 1, 2) THEN 'Winter'
                ELSE 'Spring/Fall'
            END
        )
        ON CONFLICT (date_key) DO NOTHING;
        
        curr_date := curr_date + INTERVAL '1 day';
        row_count := row_count + 1;
    END LOOP;
    
    RETURN row_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION populate_dim_date IS 'Utility function to populate date dimension for a given date range';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Example 1: Populate date dimension for 2024-2026
-- SELECT populate_dim_date('2024-01-01'::DATE, '2026-12-31'::DATE);

-- Example 2: Get top 25 competitors for a specific listing
-- SELECT * FROM view_top_competitors WHERE listing_key = 123 ORDER BY similarity_rank;

-- Example 3: Get price recommendations for all listings
-- SELECT * FROM view_price_recommendations WHERE pricing_status != 'OPTIMAL';

-- Example 4: Calculate distance between two locations
-- SELECT calculate_distance_km(51.0447, -114.0719, 51.0362, -114.0876) as distance_km;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
