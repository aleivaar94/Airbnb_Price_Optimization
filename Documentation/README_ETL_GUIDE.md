# ETL Guide: Normalized to Dimensional Database

## Overview

This guide explains how to transform your normalized Airbnb database into a dimensional star schema for competitor analysis and price optimization.

## Prerequisites

- PostgreSQL 12+ installed
- Python 3.8+ with the following packages:
  ```bash
  pip install psycopg2-binary python-dotenv scikit-learn numpy
  ```
- Existing normalized database with data (`airbnb_db`)

## Step-by-Step Setup

### 1. Create the Dimensional Database

```bash
# Connect to PostgreSQL
psql -U postgres

# Create new database for dimensional model
CREATE DATABASE airbnb_dimensional;

# Exit psql
\q
```

### 2. Run the Dimensional Schema

```bash
# Apply the dimensional schema
psql -U postgres -d airbnb_dimensional -f database_modelling_schema.sql
```

This creates:
- 5 dimension tables
- 1 central fact table
- 2 aggregate fact tables
- 1 bridge table for competitors
- 3 helper views
- Utility functions for distance calculations

### 3. Populate Date Dimension

```bash
psql -U postgres -d airbnb_dimensional

-- Populate dates for 2024-2026
SELECT populate_dim_date('2024-01-01'::DATE, '2026-12-31'::DATE);

\q
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# PostgreSQL Connection Settings (Shared)
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_secure_password

# Database Names (Independent)
SOURCE_DB_NAME=airbnb_db              # Normalized database (3NF)
TARGET_DB_NAME=airbnb_dimensional     # Dimensional database (star schema)

# Schema Files (Independent)
NORMALIZED_SCHEMA_FILE=database_normalized_schema.sql
DIMENSIONAL_SCHEMA_FILE=database_modelling_schema.sql

# Data Source
JSON_FILE=Resources/airbnb_beltline_calgary_listings_100.json
```

**Important**: These variables allow both normalized and dimensional databases to coexist independently. You can recreate either database without affecting the other.

### 5. Run the ETL Script

```bash
python etl_normalized_to_dimensional.py
```

## What the ETL Does

### Phase 1: Load Dimensions

**dim_host** - Extracts hosts and calculates:
- `host_tier`: Elite/Premium/Standard
- `experience_level`: Expert/Experienced/New

**dim_property** - Extracts properties and calculates:
- `property_size_tier`: Studio/Small/Medium/Large
- `guest_per_bedroom_ratio`: Space efficiency
- `bath_to_bedroom_ratio`: Luxury indicator

**dim_location** - Performs K-means clustering and calculates:
- `location_cluster_id`: Geographic cluster (1-10)
- `distance_to_downtown_km`: Distance to Calgary downtown
- `location_tier`: Urban Core/Downtown Adjacent/Neighborhood/Suburban

**dim_category_ratings** - Aggregates ratings and calculates:
- `overall_quality_score`: Weighted average of all ratings
- `quality_tier`: Exceptional/Excellent/Good/Fair
- `value_index`: Value metric (value_rating / overall_quality)

### Phase 2: Load Central Fact

**fact_listing_metrics** - Loads all listings and calculates:
- `price_per_guest`: price / guests
- `price_per_bedroom`: price / bedrooms
- `price_per_bed`: price / beds
- `review_velocity`: reviews per day
- `competitiveness_score`: Composite quality metric (0-100)
- `value_score`: Quality vs price metric (0-100)
- `popularity_index`: Normalized popularity measure

### Phase 3: Load Aggregate Facts

**fact_listing_amenities_summary** - Counts and classifies amenities:
- `essential_amenities_count`: Wifi, kitchen, parking, AC, heating
- `luxury_amenities_count`: Pool, gym, hot tub, EV charger
- `safety_amenities_count`: Smoke alarm, CO detector, first aid
- `amenity_score`: Weighted sum (essential×2 + luxury×3 + safety×1)
- `amenity_tier`: Luxury/Premium/Standard/Basic

### Phase 4: Competitor Analysis (Most Complex)

**bridge_listing_competitors** - For each listing, calculates similarity with all others:

**Similarity Components:**
1. **Location Similarity (35% weight)**
   - Same cluster: +50 points
   - Distance factor: 100 × e^(-distance/2)

2. **Property Similarity (25% weight)**
   - Bedroom match: +40 points
   - Guest capacity within ±2: +30 points
   - Bed/bath similarity: +30 points

3. **Quality Similarity (20% weight)**
   - Based on rating alignment

4. **Amenity Similarity (10% weight)**
   - Amenity score comparison

5. **Price Similarity (10% weight)**
   - Price range overlap

**Output**: Top 25 competitors per listing with:
- Similarity rank (1-25)
- Overall similarity score (0-100)
- Component similarity scores
- Weight for pricing calculations

### Phase 5: Pricing Analysis

**fact_competitor_pricing_analysis** - Aggregates competitor prices:
- Statistical measures (avg, median, min, max, percentiles)
- Weighted average (by similarity scores)
- Price recommendations:
  - `recommended_price_lower`: 25th percentile × 0.95
  - `recommended_price_upper`: 75th percentile × 1.05
  - `recommended_optimal_price`: Weighted avg × quality adjustment

**Quality Adjustment**: ±15% based on rating vs competitor average

### Phase 6: Finalize

- Refreshes materialized view `view_top_competitors`

## Verifying the ETL

### Check Row Counts

```sql
-- Connect to dimensional database
psql -U postgres -d airbnb_dimensional

-- Check all tables
SELECT 'dim_host' as table_name, COUNT(*) FROM dim_host
UNION ALL
SELECT 'dim_property', COUNT(*) FROM dim_property
UNION ALL
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_category_ratings', COUNT(*) FROM dim_category_ratings
UNION ALL
SELECT 'fact_listing_metrics', COUNT(*) FROM fact_listing_metrics
UNION ALL
SELECT 'fact_listing_amenities_summary', COUNT(*) FROM fact_listing_amenities_summary
UNION ALL
SELECT 'bridge_listing_competitors', COUNT(*) FROM bridge_listing_competitors
UNION ALL
SELECT 'fact_competitor_pricing_analysis', COUNT(*) FROM fact_competitor_pricing_analysis;
```

### Query Top Competitors for a Listing

```sql
-- Get top 10 competitors for listing_key = 1
SELECT 
    competitor_property_id,
    similarity_rank,
    overall_similarity_score,
    location_similarity,
    property_similarity,
    competitor_price,
    distance_km
FROM view_top_competitors
WHERE listing_key = 1
ORDER BY similarity_rank
LIMIT 10;
```

### Get Price Recommendations

```sql
-- View all price recommendations
SELECT 
    listing_key,
    property_id,
    current_price,
    recommended_optimal_price,
    pricing_status,
    price_difference
FROM view_price_recommendations
ORDER BY ABS(price_difference) DESC
LIMIT 10;
```

### Check Underpriced Properties

```sql
SELECT 
    property_id,
    current_price,
    recommended_optimal_price,
    (recommended_optimal_price - current_price) as potential_revenue_increase
FROM view_price_recommendations
WHERE pricing_status = 'UNDERPRICED'
ORDER BY potential_revenue_increase DESC;
```

## Performance Considerations

### For 100 Listings (Your Current Dataset)
- **ETL Runtime**: ~2-5 minutes
- **Competitor Calculations**: 100 × 99 = 9,900 comparisons
- **Bridge Table Rows**: 100 × 25 = 2,500 rows

### For 1,000 Listings
- **ETL Runtime**: ~15-30 minutes
- **Competitor Calculations**: 1,000 × 999 = 999,000 comparisons
- **Bridge Table Rows**: 1,000 × 25 = 25,000 rows

### For 10,000 Listings
- **ETL Runtime**: ~3-5 hours
- **Competitor Calculations**: 10,000 × 9,999 = ~100M comparisons
- **Bridge Table Rows**: 10,000 × 25 = 250,000 rows

**Optimization for Large Datasets:**
- Pre-filter by location cluster before similarity calculations
- Use parallel processing for competitor calculations
- Batch process in chunks of 1,000 listings

## Troubleshooting

### Issue: "Module not found: sklearn"
```bash
pip install scikit-learn
```

### Issue: "Connection refused"
- Verify PostgreSQL is running
- Check `.env` file has correct credentials
- Ensure both databases exist

### Issue: "Division by zero"
- Some listings may have 0 bedrooms, guests, etc.
- The script handles these with None values
- Check source data quality

### Issue: "No competitors found"
- Verify bridge table has data: `SELECT COUNT(*) FROM bridge_listing_competitors;`
- Check if similarity calculations completed
- Review logs for errors during Phase 4

### Issue: ETL is too slow
For testing with large datasets, you can:
1. Comment out competitor analysis (Phase 4-5) for initial testing
2. Reduce to top 10 competitors instead of 25
3. Add WHERE clause to limit listings processed

## Re-running the ETL

The ETL script uses `ON CONFLICT` clauses, so you can safely re-run it:

```bash
# Re-run full ETL
python etl_normalized_to_dimensional.py

# Or manually refresh just the pricing analysis
psql -U postgres -d airbnb_dimensional -c "
DELETE FROM fact_competitor_pricing_analysis;
"
# Then run ETL
```

## Database Architecture

```
┌─────────────────────┐         ┌──────────────────────┐
│  airbnb_db          │         │  airbnb_dimensional  │
│  (Normalized 3NF)   │ ──ETL──>│  (Star Schema)       │
│                     │         │                      │
│  • 13 tables        │         │  • 9 tables          │
│  • Optimized for    │         │  • Optimized for     │
│    data integrity   │         │    analytics         │
│  • Write-heavy      │         │  • Read-heavy        │
└─────────────────────┘         └──────────────────────┘
```

**Keep Both Databases:**
- `airbnb_db` (SOURCE_DB_NAME): For transactional operations, data collection
- `airbnb_dimensional` (TARGET_DB_NAME): For analytics, reporting, ML models

**Independent Configuration:**
- Each database has its own schema file variable
- Both can be recreated independently without conflicts
- Separate ETL scripts reference their respective databases

**Refresh Strategy:**
- Run ETL daily/weekly to sync changes
- Or trigger on data updates in source

## Next Steps

After successful ETL:

1. **Build ML Models** for price optimization
2. **Create Dashboards** using views
3. **Implement API** to serve recommendations
4. **Schedule ETL** using cron/airflow for automated refreshes

## Files Created

- `database_modelling_schema.sql` - Dimensional schema DDL
- `etl_normalized_to_dimensional.py` - ETL transformation script
- `Documentation/README_DIMENSIONAL_MODEL.md` - Detailed documentation

## Questions?

Common queries are in `Documentation/README_DIMENSIONAL_MODEL.md`
