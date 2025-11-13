"""Quick script to find amenities with names longer than 255 characters."""

import json

# Load the JSON data
with open('Resources/airbnb_beltline_calgary_listings_100.json', 'r', encoding='utf-8') as f:
    listings = json.load(f)

print("Checking for long amenity names and codes...")
print("=" * 80)

long_found = False
for idx, listing in enumerate(listings):
    amenities_data = listing.get('amenities', [])
    
    for amenity_group in amenities_data:
        group_name = amenity_group.get('group_name', '')
        
        for item in amenity_group.get('items', []):
            amenity_name = item.get('name', '')
            amenity_code = item.get('value', '')
            
            if len(amenity_name) > 255:
                print(f"\n🔴 Listing {idx + 1}: Long amenity_name found!")
                print(f"   Group: {group_name}")
                print(f"   Length: {len(amenity_name)}")
                print(f"   First 100 chars: {amenity_name[:100]}...")
                long_found = True
                
            if amenity_code and len(amenity_code) > 255:
                print(f"\n🔴 Listing {idx + 1}: Long amenity_code found!")
                print(f"   Group: {group_name}")
                print(f"   Length: {len(amenity_code)}")
                print(f"   First 100 chars: {amenity_code[:100]}...")
                long_found = True

if not long_found:
    print("✅ No amenities with names/codes longer than 255 characters found!")
else:
    print("\n" + "=" * 80)
    print("Fix applied: These will be automatically truncated to 255 characters.")
