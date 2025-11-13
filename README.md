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