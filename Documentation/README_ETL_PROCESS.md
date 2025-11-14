# ETL Process: Normalized to Dimensional Database

## Overview

The `etl_normalized_to_dimensional.py` script transforms Airbnb data from a normalized (3NF) schema to a dimensional star schema optimized for competitor analysis and price optimization.

## Architecture

### Source: Normalized Database (3NF)
- **Tables**: `hosts`, `listings`, `amenities`, `listing_amenities`, `listing_category_ratings`
- **Purpose**: Data integrity, minimal redundancy, transactional operations

### Target: Dimensional Database (Star Schema)
- **Dimensions**: `dim_host`, `dim_property`, `dim_location`, `dim_category_ratings`, `dim_date`
- **Facts**: `fact_listing_metrics`, `fact_listing_amenities_summary`, `fact_competitor_pricing_analysis`
- **Bridge**: `bridge_listing_competitors`
- **Purpose**: Fast analytics, competitor analysis, price optimization

## ETL Pipeline Phases

### Phase 1: Load Dimensions
Extracts and transforms dimension data with calculated attributes:

**1. dim_host**
- Extracts: Host profile data
- Calculates:
  - `host_tier`: Elite (superhost + rating>4.8), Premium (rating>4.5), Standard
  - `experience_level`: Expert (>5 years), Experienced (2-5 years), New (≤2 years)

**2. dim_property**
- Extracts: Property characteristics
- Calculates:
  - `property_size_tier`: Studio, Small (1 bed), Medium (2-3 beds), Large (4+ beds)
  - `guest_per_bedroom_ratio`: Space efficiency metric
  - `bath_to_bedroom_ratio`: Luxury indicator

**3. dim_location**
- Extracts: Unique geographic coordinates
- Performs: K-means clustering (up to 10 clusters)
- Calculates:
  - `distance_to_downtown_km`: Haversine distance to Calgary downtown
  - `location_tier`: Urban Core (<1km), Downtown Adjacent (1-3km), Neighborhood (3-7km), Suburban (>7km)

**4. dim_category_ratings**
- Extracts: Pivoted rating categories (cleanliness, accuracy, check-in, communication, location, value)
- Calculates:
  - `overall_quality_score`: Weighted average (cleanliness×0.25 + accuracy×0.15 + checkin×0.10 + communication×0.15 + location×0.15 + value×0.20)
  - `quality_tier`: Exceptional (>4.8), Excellent (>4.5), Good (>4.0), Fair
  - `value_index`: value_rating / overall_quality_score

### Phase 2: Load Central Fact
**fact_listing_metrics**
- Joins: All dimensions with source listings
- Calculates:
  - `price_per_guest`, `price_per_bedroom`, `price_per_bed`: Price efficiency metrics
  - `review_velocity`: Reviews per day since listing creation
  - `competitiveness_score`: Composite score (0-100) based on rating, reviews, host quality
  - `value_score`: Quality metrics / price (0-100)
  - `popularity_index`: (reviews × rating) / segment average

### Phase 3: Load Aggregate Facts
**fact_listing_amenities_summary**
- Extracts: All amenities per listing
- Classifies amenities:
  - **Essential**: Wifi, Kitchen, Parking, AC, Heating, Washer, Dryer, Workspace
  - **Luxury**: Pool, Hot tub, Gym, EV charger, Sauna, BBQ, Outdoor furniture
  - **Safety**: Smoke alarm, CO alarm, First aid, Fire extinguisher, Security cameras
- Calculates:
  - `amenity_score`: Weighted sum (essential×2 + luxury×3 + safety×1)
  - `amenity_tier`: Luxury (>50), Premium (>30), Standard (>15), Basic

### Phase 4: Competitor Analysis
**bridge_listing_competitors**
- Algorithm: Multi-dimensional similarity scoring
- Components:
  - **Location Similarity (35%)**: Geographic distance + cluster membership
  - **Property Similarity (25%)**: Bedroom/bed/bath/guest capacity match
  - **Quality Similarity (20%)**: Rating alignment
  - **Amenity Similarity (10%)**: Jaccard index of shared amenities
  - **Price Similarity (10%)**: Price range overlap
- Output: Top 25 competitors per listing with normalized weights

### Phase 5: Pricing Analysis
**fact_competitor_pricing_analysis**
- Aggregates: Competitor prices from bridge table
- Calculates:
  - Statistical measures: avg, median, min, max, percentiles (25th, 75th)
  - `weighted_avg_price`: Average weighted by similarity scores
  - `price_premium_discount`: % difference from weighted average
  - `recommended_price_lower`: 25th percentile × 0.95
  - `recommended_price_upper`: 75th percentile × 1.05
  - `recommended_optimal_price`: weighted_avg × quality_adjustment_factor (±15% based on rating)

### Phase 6: Finalization
- Refreshes materialized view: `view_top_competitors`
- Commits all transactions
- Logs completion statistics

## Usage

### Prerequisites
1. Normalized database (`airbnb_db`) must be populated
2. Dimensional database (`airbnb_dimensional`) must have schema created
3. Environment variables configured in `.env`:
   ```
   DB_HOST=localhost
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_PORT=5432
   SOURCE_DB_NAME=airbnb_db
   TARGET_DB_NAME=airbnb_dimensional
   ```

### Running the ETL

**Option 1: Command Line**
```bash
python etl_normalized_to_dimensional.py
```

**Option 2: From Jupyter Notebook**
Run the cells in `create_airbnb_database_dimensional.ipynb` after schema creation.

**Option 3: Programmatically**
```python
from etl_normalized_to_dimensional import DimensionalETL

source_config = {
    'host': 'localhost',
    'database': 'airbnb_db',
    'user': 'postgres',
    'password': 'your_password',
    'port': 5432
}

target_config = {
    'host': 'localhost',
    'database': 'airbnb_dimensional',
    'user': 'postgres',
    'password': 'your_password',
    'port': 5432
}

etl = DimensionalETL(source_config, target_config)
etl.run_full_etl()
```

## Performance Considerations

### Optimization Strategies
1. **Batch Loading**: Uses `psycopg2.extras.execute_values()` for bulk inserts
2. **Dimension Caching**: Builds in-memory caches for foreign key lookups
3. **K-means Clustering**: Limited to 10 clusters for efficiency
4. **Competitor Limiting**: Only top 25 competitors per listing to manage size
5. **Materialized Views**: Pre-computed joins for faster queries

### Expected Runtime
- Small dataset (<100 listings): 1-2 minutes
- Medium dataset (100-1000 listings): 5-10 minutes
- Large dataset (1000+ listings): 15-30 minutes

*Note: Competitor similarity calculation is O(n²), scales quadratically*

## Data Quality Checks

The ETL includes built-in validation:
- ✓ Handles NULL values gracefully
- ✓ Prevents division by zero errors
- ✓ Validates coordinate data before clustering
- ✓ Skips listings missing critical dimensions
- ✓ Logs warning messages for data issues
- ✓ Rolls back on errors to maintain consistency

## Output Verification

After ETL completion, verify data loaded correctly:

```sql
-- Check row counts
SELECT 
    'dim_host' as table_name, COUNT(*) as rows FROM dim_host
UNION ALL
SELECT 'dim_property', COUNT(*) FROM dim_property
UNION ALL
SELECT 'fact_listing_metrics', COUNT(*) FROM fact_listing_metrics
UNION ALL
SELECT 'bridge_listing_competitors', COUNT(*) FROM bridge_listing_competitors;

-- Sample competitor analysis
SELECT * FROM view_top_competitors LIMIT 10;

-- Sample price recommendations
SELECT * FROM view_price_recommendations 
WHERE pricing_status != 'OPTIMAL' 
LIMIT 10;
```

## Troubleshooting

### Common Issues

**Issue**: Connection refused
- **Solution**: Verify PostgreSQL is running and credentials in `.env` are correct

**Issue**: Source database empty
- **Solution**: Run normalized database ETL first (`etl_airbnb_normalized_postgres.py`)

**Issue**: Schema not found errors
- **Solution**: Ensure dimensional schema was created (`create_airbnb_database_dimensional.ipynb`)

**Issue**: Out of memory during clustering
- **Solution**: Reduce `n_clusters` in `load_dim_location()` method

**Issue**: Very slow competitor calculation
- **Solution**: Reduce dataset size or modify competitor limit from 25 to fewer

## Logging

The ETL provides detailed logging:
- INFO: Progress updates for each phase
- WARNING: Data quality issues (skipped records, NULL values)
- ERROR: Critical failures with rollback

Logs include:
- Extraction row counts
- Transformation calculations
- Load statistics
- Timing information
- Error messages with context

## Maintenance

### Incremental Updates
For incremental loads:
1. Modify dimension loads to use `ON CONFLICT ... DO UPDATE`
2. Add timestamp filtering in source queries
3. Update only changed fact records
4. Refresh materialized views

### Data Refresh Strategy
- **Full Refresh**: Run complete ETL (recommended weekly)
- **Competitor Updates**: Re-run phases 4-5 only (recommended daily)
- **Price Analysis Only**: Run phase 5 only (recommended multiple times daily)

## Related Documentation
- [Database Schema](README_DATABASE.md)
- [Dimensional Model](README_DIMENSIONAL_MODEL.md)
- [Normalization Process](NORMALIZATION_SUMMARY.md)
