-- ============================================================================
-- Airbnb Listings Normalized Database Schema
-- ============================================================================
-- This schema normalizes Airbnb listing data into a relational structure
-- following 3NF (Third Normal Form) principles
-- ============================================================================

-- Drop existing tables (in reverse order of dependencies)
DROP TABLE IF EXISTS listing_amenities CASCADE;
DROP TABLE IF EXISTS listing_highlights CASCADE;
DROP TABLE IF EXISTS listing_arrangement_details CASCADE;
DROP TABLE IF EXISTS listing_category_ratings CASCADE;
DROP TABLE IF EXISTS listing_house_rules CASCADE;
DROP TABLE IF EXISTS listing_reviews CASCADE;
DROP TABLE IF EXISTS listing_location_details CASCADE;
DROP TABLE IF EXISTS listing_description_sections CASCADE;
DROP TABLE IF EXISTS listing_cancellation_policies CASCADE;
DROP TABLE IF EXISTS amenities CASCADE;
DROP TABLE IF EXISTS amenity_groups CASCADE;
DROP TABLE IF EXISTS listings CASCADE;
DROP TABLE IF EXISTS hosts CASCADE;

-- ============================================================================
-- CORE ENTITIES
-- ============================================================================

-- Hosts table: Stores unique host information
-- Using TEXT for all string fields to handle variable-length data without truncation
CREATE TABLE hosts (
    host_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    image_url TEXT,
    profile_url TEXT,
    rating DECIMAL(3, 2),
    number_of_reviews INTEGER DEFAULT 0,
    response_rate INTEGER,
    response_time TEXT,
    years_hosting INTEGER,
    languages TEXT,
    my_work TEXT,
    is_superhost BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Listings table: Main property listing information
-- Using TEXT for all string fields to handle variable-length data without truncation
CREATE TABLE listings (
    listing_id SERIAL PRIMARY KEY,
    property_id TEXT UNIQUE,
    host_id TEXT REFERENCES hosts(host_id),
    name TEXT NOT NULL,
    listing_title TEXT,
    listing_name TEXT,
    url TEXT,
    category TEXT,
    description TEXT,
    city TEXT,
    province TEXT,
    country TEXT,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    price_per_night DECIMAL(10, 2),
    currency TEXT DEFAULT 'CAD',
    rating DECIMAL(3, 2),
    number_of_reviews INTEGER DEFAULT 0,
    guests INTEGER,
    bedrooms INTEGER,
    beds INTEGER,
    baths INTEGER,
    pets_allowed BOOLEAN DEFAULT FALSE,
    availability BOOLEAN DEFAULT TRUE,
    is_guest_favorite BOOLEAN DEFAULT FALSE,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- LOOKUP TABLES
-- ============================================================================

-- Amenity groups: Categories of amenities
-- Using TEXT for group_name to handle variable-length category names
CREATE TABLE amenity_groups (
    group_id SERIAL PRIMARY KEY,
    group_name TEXT UNIQUE NOT NULL
);

-- Amenities: Individual amenities with their system codes
-- Using TEXT for amenity_code and amenity_name to handle long descriptions
-- (e.g., security device details can exceed 300 characters)
CREATE TABLE amenities (
    amenity_id SERIAL PRIMARY KEY,
    amenity_code TEXT,
    amenity_name TEXT NOT NULL,
    group_id INTEGER REFERENCES amenity_groups(group_id),
    UNIQUE(amenity_code, amenity_name, group_id)
);

-- ============================================================================
-- RELATIONSHIP/JUNCTION TABLES
-- ============================================================================

-- Listing amenities: Links listings to their amenities
CREATE TABLE listing_amenities (
    listing_amenity_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    amenity_id INTEGER REFERENCES amenities(amenity_id),
    UNIQUE(listing_id, amenity_id)
);

-- Listing highlights: Special features of listings
-- Using TEXT for all string fields
CREATE TABLE listing_highlights (
    highlight_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    highlight_name TEXT NOT NULL,
    highlight_value TEXT
);

-- Listing arrangement details: Room and bed arrangements
-- Using TEXT for all string fields
CREATE TABLE listing_arrangement_details (
    arrangement_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    room_name TEXT NOT NULL,
    arrangement_value TEXT NOT NULL
);

-- Listing category ratings: Detailed ratings by category
-- Using TEXT for category_name
CREATE TABLE listing_category_ratings (
    category_rating_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    category_name TEXT NOT NULL,
    rating_value DECIMAL(3, 2) NOT NULL,
    UNIQUE(listing_id, category_name)
);

-- Listing house rules: Rules for each listing
-- Using TEXT for rule_text to handle variable-length rules
CREATE TABLE listing_house_rules (
    rule_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    rule_text TEXT NOT NULL
);

-- Listing reviews: Guest reviews with detailed information
-- Using TEXT for all string fields
CREATE TABLE listing_reviews (
    review_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    guest_name TEXT,
    guest_time_on_airbnb TEXT,
    review_text TEXT NOT NULL,
    review_date TIMESTAMP,
    rating INTEGER,
    stayed_for TEXT,
    host_response TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Listing location details: Detailed location descriptions
-- Using TEXT for all string fields
CREATE TABLE listing_location_details (
    location_detail_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    detail_title TEXT,
    detail_value TEXT NOT NULL
);

-- Listing description sections: Structured description by sections
-- Using TEXT for all string fields
CREATE TABLE listing_description_sections (
    section_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    section_title TEXT,
    section_value TEXT NOT NULL,
    section_order INTEGER
);

-- Listing cancellation policies: Cancellation policy details
-- Using TEXT for policy_name
CREATE TABLE listing_cancellation_policies (
    policy_id SERIAL PRIMARY KEY,
    listing_id INTEGER REFERENCES listings(listing_id) ON DELETE CASCADE,
    policy_name TEXT NOT NULL,
    policy_date DATE
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Hosts indexes
CREATE INDEX idx_hosts_rating ON hosts(rating);
CREATE INDEX idx_hosts_superhost ON hosts(is_superhost);

-- Listings indexes
CREATE INDEX idx_listings_host ON listings(host_id);
CREATE INDEX idx_listings_city ON listings(city);
CREATE INDEX idx_listings_province ON listings(province);
CREATE INDEX idx_listings_country ON listings(country);
CREATE INDEX idx_listings_price ON listings(price_per_night);
CREATE INDEX idx_listings_rating ON listings(rating);
CREATE INDEX idx_listings_guests ON listings(guests);
CREATE INDEX idx_listings_availability ON listings(availability);
CREATE INDEX idx_listings_coords ON listings(latitude, longitude);
CREATE INDEX idx_listings_property_id ON listings(property_id);

-- Amenities indexes
CREATE INDEX idx_amenities_group ON amenities(group_id);
CREATE INDEX idx_listing_amenities_listing ON listing_amenities(listing_id);

-- Reviews indexes
CREATE INDEX idx_reviews_listing ON listing_reviews(listing_id);
CREATE INDEX idx_reviews_date ON listing_reviews(review_date);
CREATE INDEX idx_reviews_rating ON listing_reviews(rating);

-- Category ratings indexes
CREATE INDEX idx_category_ratings_listing ON listing_category_ratings(listing_id);

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE hosts IS 'Stores unique host/property manager information';
COMMENT ON TABLE listings IS 'Main table for Airbnb property listings';
COMMENT ON TABLE amenity_groups IS 'Categories for grouping amenities';
COMMENT ON TABLE amenities IS 'Master list of all available amenities';
COMMENT ON TABLE listing_amenities IS 'Junction table linking listings to amenities';
COMMENT ON TABLE listing_highlights IS 'Special features and highlights of listings';
COMMENT ON TABLE listing_arrangement_details IS 'Room and bed arrangement details';
COMMENT ON TABLE listing_category_ratings IS 'Detailed ratings by category (cleanliness, location, etc.)';
COMMENT ON TABLE listing_house_rules IS 'House rules for each listing';
COMMENT ON TABLE listing_reviews IS 'Guest reviews with detailed metadata';
COMMENT ON TABLE listing_location_details IS 'Detailed location descriptions and highlights';
COMMENT ON TABLE listing_description_sections IS 'Structured description broken into sections';
COMMENT ON TABLE listing_cancellation_policies IS 'Cancellation policy information';
