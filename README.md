# Environment Setup:
- Need to install PostgreSQL 18.0


# Process

Got a first dump of 100 listings (sd_mhscq4j82hwgc7ayqf.json) for Beltline, Calgary. Then compared the keys in the json dump to minimize the file size (and price of the API call) by only requesting necessary fields. 

Then I leveraged AI (claude) with these keys variables that could be used for price optimization.

How to make the normalization schema for a SQL database?

Use CLaude to identify entities from the json dump of 25 listings (pretty_airbnb_data.json) and to propose a data normalization schema for a SQL database. First analyse the data and decide the dimension and fact tables. (conversation: Normalizing Airbanb listings into relational database: https://claude.ai/chat/bbeb5847-5f36-4932-8b31-a36ad40f855f)


When normalizing, creating dimensional model, pricing optimization and algorithm to identify competitors need to identify the business processes.

From the data, the key business processes are:

Pricing Analysis - Understanding price factors
Booking Performance - Availability and reviews
Property Features - Amenities impact on pricing/ratings
Host Performance - Superhost status, response rates
Guest Satisfaction - Reviews and ratings



# Prompt: Normalizing Airbnb listings into relational database (took screenshot)

You are a data engineer. The data in the json contains 100 web scraped airbnb lisitngs from the Beltline, Calgary, Canada neighborhood.  First analyse the data to understand its structure, identifying the entities and relationships, then normalize into a PostgreSQL relational database. Think step by step.

- Need to add to this prompt the specific of VARCHAR to text.
Listings table (normalized)
    split location into city, province, country. Need to update schema
    Remove:
    total_price

    Rename
    price to price_per_night

listings_reviews table (normalized)
    Remove:
    guest_location


Amenities had problems with VARCHAR length (truncated to 255 characters in ETL script) so we used TEXT instead because PostgreSQL treats them the same way with no performance difference. Updated all VARCHAR types to TEXT throughout the entire normalization schema for consistency and to eliminate any potential truncation issues.

Next comes identifying relationships by looking at how entities connect. The key question is cardinality: how many of entity A relate to entity B? When we saw that one host could have multiple listings but each listing has only one host, that's a many-to-one relationship. The analysis showed you have 25 listings from 24 hosts, confirming this pattern. For amenities, when we found 138 unique amenity types appearing 1,253 times across listings—a 9x reuse factor—that proved amenities are shared resources, creating a many-to-many relationship that requires a junction table.


# Prompt: Dimensional modelling (took screenshot)
You are a data engineer. he data in the json contains 100 web scraped airbnb lisitngs from the Beltline, Calgary, Canada neighborhood.  First analyse the data to understand its structure. The data in the json is normalized based on the data schema in #file:database_normalized_schema.sql and the #file:etl_airbnb_normalized_postgres.py creates the pipeline to create the relational database in postgressql.
Propose a dimensional model with fact and dimension tables. Think step by step.
#sequentialthinking 


# Prompt: Creating the Airbnb Listings Fetch Script (Took Screenshot)
Create a new script named "airbnb_listings_fetch" based on the the brightdata.py. Modify it to include the request in the #file:airbnb_location.ipynb. This file fetches airbnb listings based on location using brightdata's API scraper. The input parameters must be location, for example "Beltline, Calgary" and limit_per_input, for example, 100.

Load the API key from the env file.
The snapshot is ready between 10-15 minutes so include the time and number of retries to accomodate this total time.

# Prompt: Modified the Airbnb Listings Fetch Script to include fetch by URL (Took Screenshot)
Include a function in #file:airbnb_listings_fetch.py that includes the code below. The code below extracts airbnb listint data by url, instead of by location. The function must have url as input parameter. Keep the custom output fields that are commented out.

Rename the functions so that its clear which functions fetch listings by location and by url.
Change the max retries and wait time for location function to a maximum of 30 minutes. 
Change the max retries and wait tiem for url function to a maximum of 5 minutes

url = "https://api.brightdata.com/datasets/v3/trigger"
headers = {
        "Authorization": f"Bearer {brightdata_api_key}",
        "Content-Type": "application/json",
}
params = {
        "dataset_id": "gd_ld7ll037kqy322v05",
}

data = {
    "input": [
        {"url": "https://www.airbnb.ca/rooms/1300059188064308611", "country": "CA"}
    ],
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

response = requests.post(url, headers=headers, params=params, json=data)
print(response.json())


# Prompt: Creating dimensional model tables in Postgres (Took Screenshot)
#sequentialthinking 
You are a data engineer and a data scientist. The end objective is to create a competitor algorithm that selects the top 25 competitors based on a particular listing. Based on the top 25 competitors an optimal price for the particular listing needs to be calculated.

The task is to propose a relational dimensional database model with dimension and fact tables from the normalized database that includes aggregations or calculated columns that can be used for the end objective.

Based on research, these are important variables when a user selects a property (for competitor algorithm)

hosts table

rating
number_of_reviews
response_rate
years_hosting
is_superhost

listings table

price_per_night
rating
number_of_reviews
guests
bedrooms
beds
baths
pets_allowed
is_guest_favorite

listing_category_ratings table

cleanliness
accuracy
check-in
communication
location
value

think step by step

# Notes

## Understanding "Reduce Redundancy"

Now let me explain what "reduce redundancy" means, because it's central to understanding why normalization matters. Redundancy means storing the same piece of information in multiple places. Let me illustrate with your Airbnb data.

Imagine you didn't normalize and instead kept everything in one giant properties table. Each property row would contain the host's name, host's rating, and host's response rate. Now picture that the same host has five different properties. Without normalization, you'd store that host's name five times, their rating five times, and their response rate five times. That's redundancy - the same information repeated unnecessarily.

Why is this a problem? Let's say the host's rating changes from four-point-eight to four-point-nine. In the redundant structure, you'd need to find all five property records for that host and update each one. Miss one, and now you have inconsistent data - some properties show the old rating and others show the new rating. This is called an update anomaly. You also have an insertion anomaly - what if you want to add a new host to your system before they have any properties listed? In the redundant structure, you can't store host information without a property. And there's a deletion anomaly - if a host removes all their properties, you lose all information about that host.

Normalization eliminates redundancy by storing the host information once in a hosts table. Each property just has a host_id that points to that single host record. Now when the host's rating changes, you update it in exactly one place, and all five properties automatically reflect the correct rating through the foreign key relationship. You can add hosts without properties, and delete properties without losing host information. The data is consistent, maintainable, and efficient.

Let me show you this with a concrete example from your data:
```
WITHOUT NORMALIZATION (Redundant):
properties_flat table:
property_id | name          | price | host_name | host_rating | host_response_rate
PROP_001    | Calgary Apt   | 84.10 | Geoff     | 4.8         | 100%
PROP_002    | Downtown Unit | 92.00 | Geoff     | 4.8         | 100%
PROP_003    | Cozy Studio   | 78.50 | Geoff     | 4.8         | 100%

Problem: Geoff's information stored THREE times. If his rating changes, 
you must update THREE rows. Forget one and you have inconsistent data.

WITH NORMALIZATION (No Redundancy):
hosts table:
host_id  | host_name | host_rating | host_response_rate
HOST_001 | Geoff     | 4.8         | 100%

properties table:
property_id | name          | price | host_id
PROP_001    | Calgary Apt   | 84.10 | HOST_001
PROP_002    | Downtown Unit | 92.00 | HOST_001
PROP_003    | Cozy Studio   | 78.50 | HOST_001

Solution: Geoff's information stored ONCE. Update it once, and all three
properties automatically reflect the change through the foreign key.



## Claude Deep research on factors affecting booking success(https://claude.ai/chat/3c50a57b-83ef-484b-b98c-afeeb46d5f0a)

Below three supposedly more important than price or location:
- Cleanliness
- Host communication
- Host responsiveness
- Review ratings
- Amenities (competitors analysis)
- Wifi is a must so not having it can be a penalty
- is Superhost status


"Negative reviews carry twice the impact of positive reviews on pricing decisions." (https://www.emerald.com/ijchm/article/31/12/4520/126996/Standing-out-from-the-crowd-an-exploration-of)

"Amenities do matter". This is an article by Airbnb. People prefer functionality and comfort over connectivity. (https://news.airbnb.com/amenities-do-matter-airbnb-reveals-which-amenities-guests-search-for-most/)\

After the deep research I fed the a sample of 25 listings to Claude and asked what variables would be important for developing a competitor matching algorithm and a price optimization algorithm.

---

# Implementation Guide

## Project Architecture

This project implements a complete data pipeline from raw web-scraped data to ML-ready dimensional model:

```
JSON Data → Normalized DB (3NF) → Dimensional DB (Star Schema) → ML Models
   100       13 tables (airbnb_db)    9 tables (airbnb_dimensional)    Price Optimization
listings                                                                Competitor Analysis
```

## Quick Start

### 1. Initial Setup (One-Time)

```bash
# Install dependencies
pip install psycopg2-binary python-dotenv scikit-learn numpy

# Configure .env file with your PostgreSQL credentials
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=your_password
SOURCE_DB_NAME=airbnb_db
TARGET_DB_NAME=airbnb_dimensional
```

### 2. Load Initial Data

```bash
# Creates normalized database and loads 100 listings
python etl_airbnb_normalized_postgres.py
```

### 3. Create Dimensional Model

**Option A - Automated (Recommended):**
```bash
python setup_dimensional_db.py
```

**Option B - Manual:**
```bash
# Create dimensional database
psql -U postgres -c "CREATE DATABASE airbnb_dimensional;"

# Apply schema
psql -U postgres -d airbnb_dimensional -f database_modelling_schema.sql

# Populate date dimension
psql -U postgres -d airbnb_dimensional -c "SELECT populate_dim_date('2024-01-01'::DATE, '2026-12-31'::DATE);"

# Run ETL transformation
python etl_normalized_to_dimensional.py
```

## Key Features

### 1. Competitor Matching Algorithm

Identifies top 25 most similar listings using **multi-dimensional similarity**:

- **Location (35%)**: Geographic distance + cluster membership
- **Property (25%)**: Bedrooms, beds, baths, capacity
- **Quality (20%)**: Rating alignment
- **Amenity (10%)**: Shared amenities
- **Price (10%)**: Price range overlap

**Query example:**
```sql
-- Get competitors for listing_key = 1
SELECT * FROM view_top_competitors 
WHERE listing_key = 1 
ORDER BY similarity_rank;
```

### 2. Dynamic Price Optimization

Analyzes competitor pricing and provides:
- Weighted average price (by similarity)
- Statistical benchmarks (median, percentiles)
- Quality-adjusted recommendations
- Pricing status (UNDERPRICED/OPTIMAL/OVERPRICED)

**Query example:**
```sql
-- Get price recommendations
SELECT 
    property_id,
    current_price,
    recommended_optimal_price,
    pricing_status
FROM view_price_recommendations
WHERE pricing_status = 'UNDERPRICED';
```

### 3. Pre-Computed Analytics

**Calculated Metrics:**
- `competitiveness_score` - Overall listing quality (0-100)
- `value_score` - Quality vs price ratio
- `amenity_score` - Quantified amenity value
- `host_tier` - Elite/Premium/Standard classification
- `property_size_tier` - Studio/Small/Medium/Large
- `location_tier` - Urban Core to Suburban

## File Structure

| File | Purpose |
|------|---------|
| `database_normalized_schema.sql` | 3NF schema definition (13 tables) |
| `database_modelling_schema.sql` | Star schema definition (9 tables) |
| `etl_airbnb_normalized_postgres.py` | Loads JSON → Normalized DB |
| `etl_normalized_to_dimensional.py` | Transforms Normalized → Dimensional |
| `setup_dimensional_db.py` | Automated setup script |
| `airbnb_listings_fetch.py` | BrightData API scraper |
| `Documentation/README_ETL_GUIDE.md` | Complete setup guide |
| `Documentation/README_DIMENSIONAL_MODEL.md` | Model documentation |

## Database Schemas

### Normalized Database (airbnb_db)

**Purpose**: Source of truth, data integrity, transactional operations

**Tables (13):**
- Core: `hosts`, `listings`
- Lookup: `amenity_groups`, `amenities`
- Relationships: `listing_amenities`, `listing_category_ratings`, `listing_reviews`, `listing_highlights`, `listing_arrangement_details`, `listing_house_rules`, `listing_location_details`, `listing_description_sections`, `listing_cancellation_policies`

### Dimensional Database (airbnb_dimensional)

**Purpose**: Analytics, competitor analysis, price optimization, ML models

**Dimensions (5):**
- `dim_host` - Host quality and experience
- `dim_property` - Property characteristics
- `dim_location` - Geographic clustering
- `dim_category_ratings` - Quality metrics
- `dim_date` - Time intelligence

**Facts (3):**
- `fact_listing_metrics` - Central fact with performance metrics
- `fact_listing_amenities_summary` - Aggregated amenity data
- `fact_competitor_pricing_analysis` - Price recommendations

**Bridge (1):**
- `bridge_listing_competitors` - Top 25 competitors per listing

## Usage Examples

### Find Underpriced Listings

```sql
SELECT 
    property_id,
    listing_name,
    current_price,
    recommended_optimal_price,
    (recommended_optimal_price - current_price) AS revenue_opportunity
FROM view_price_recommendations
WHERE pricing_status = 'UNDERPRICED'
ORDER BY revenue_opportunity DESC
LIMIT 10;
```

### Analyze by Host Tier

```sql
SELECT 
    h.host_tier,
    COUNT(*) AS listing_count,
    AVG(f.price_per_night) AS avg_price,
    AVG(f.listing_rating) AS avg_rating,
    AVG(f.competitiveness_score) AS avg_competitiveness
FROM fact_listing_metrics f
JOIN dim_host h ON f.host_key = h.host_key
GROUP BY h.host_tier
ORDER BY avg_competitiveness DESC;
```

### Compare Properties by Location Cluster

```sql
SELECT 
    l.location_cluster_id,
    l.location_tier,
    COUNT(*) AS properties,
    AVG(f.price_per_night) AS avg_price,
    AVG(f.listing_rating) AS avg_rating
FROM fact_listing_metrics f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.location_cluster_id, l.location_tier
ORDER BY avg_price DESC;
```

## Performance & Scalability

| Dataset | ETL Time | Comparisons | Storage |
|---------|----------|-------------|---------|
| 100 listings | 2-5 min | 9,900 | ~5 MB |
| 1K listings | 15-30 min | 999K | ~50 MB |
| 10K listings | 3-5 hours | 100M | ~500 MB |

## Documentation

- **[README_ETL_GUIDE.md](Documentation/README_ETL_GUIDE.md)** - Complete setup instructions
- **[README_DIMENSIONAL_MODEL.md](Documentation/README_DIMENSIONAL_MODEL.md)** - Schema details and formulas
- **[README_DATABASE.md](Documentation/README_DATABASE.md)** - Normalized database guide
- **[NORMALIZATION_SUMMARY.md](Documentation/NORMALIZATION_SUMMARY.md)** - Database design process

## Technology Stack

- **Database**: PostgreSQL 12+
- **ETL**: Python 3.8+ with psycopg2, numpy, scikit-learn
- **Clustering**: K-means algorithm for geographic grouping
- **Distance**: Haversine formula for geo calculations
- **Data Source**: BrightData API (web scraping)

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests if applicable
4. Submit a pull request

## License

MIT License - See LICENSE file for details

## Contact

- GitHub: [@aleivaar94](https://github.com/aleivaar94)
- Project: [Airbnb_Price_Optimization](https://github.com/aleivaar94/Airbnb_Price_Optimization)