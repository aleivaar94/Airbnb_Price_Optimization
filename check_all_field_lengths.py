"""Comprehensive script to find all fields exceeding database limits."""

import json

# Load the JSON data
with open('Resources/airbnb_beltline_calgary_listings_100.json', 'r', encoding='utf-8') as f:
    listings = json.load(f)

print("Checking all text fields against database schema limits...")
print("=" * 80)

# Field limits from database_normalized_schema.sql
field_limits = {
    'host_id': 50,
    'name': 255,  # host name
    'listing_name': 500,
    'listing_title': 500,
    'location': 255,
    'category': 100,
    'currency': 10,
    'amenity_code': 255,
    'amenity_name': 255,
    'amenity_group_name': 100,
    'highlight_name': 255,
    'room_name': 100,
    'arrangement_value': 255,
    'category_name': 100,
    'rule_text': 500,
    'guest_name': 255,
    'guest_location': 255,
    'guest_time_on_airbnb': 100,
    'stayed_for': 100,
    'detail_title': 255,
    'section_title': 255,
    'policy_name': 255,
}

violations = []

for idx, listing in enumerate(listings):
    listing_num = idx + 1
    
    # Check host details
    host = listing.get('host_details', {})
    if host.get('host_id') and len(str(host['host_id'])) > field_limits['host_id']:
        violations.append((listing_num, 'host_id', len(str(host['host_id'])), field_limits['host_id']))
    if host.get('name') and len(host['name']) > field_limits['name']:
        violations.append((listing_num, 'host.name', len(host['name']), field_limits['name']))
    
    # Check listing fields
    if listing.get('listing_name') and len(listing['listing_name']) > field_limits['listing_name']:
        violations.append((listing_num, 'listing_name', len(listing['listing_name']), field_limits['listing_name']))
    if listing.get('listing_title') and len(listing['listing_title']) > field_limits['listing_title']:
        violations.append((listing_num, 'listing_title', len(listing['listing_title']), field_limits['listing_title']))
    if listing.get('location') and len(listing['location']) > field_limits['location']:
        violations.append((listing_num, 'location', len(listing['location']), field_limits['location']))
    if listing.get('category') and len(listing['category']) > field_limits['category']:
        violations.append((listing_num, 'category', len(listing['category']), field_limits['category']))
    
    # Check amenities
    for amenity_group in listing.get('amenities', []):
        group_name = amenity_group.get('group_name', '')
        if len(group_name) > field_limits['amenity_group_name']:
            violations.append((listing_num, 'amenity_group_name', len(group_name), field_limits['amenity_group_name']))
        
        for item in amenity_group.get('items', []):
            if item.get('name') and len(item['name']) > field_limits['amenity_name']:
                violations.append((listing_num, 'amenity_name', len(item['name']), field_limits['amenity_name']))
            if item.get('value') and len(item['value']) > field_limits['amenity_code']:
                violations.append((listing_num, 'amenity_code', len(item['value']), field_limits['amenity_code']))
    
    # Check highlights
    for highlight in listing.get('highlights', []):
        if highlight.get('name') and len(highlight['name']) > field_limits['highlight_name']:
            violations.append((listing_num, 'highlight_name', len(highlight['name']), field_limits['highlight_name']))
    
    # Check arrangements
    for arr in listing.get('arrangement_details', []):
        if arr.get('room') and len(arr['room']) > field_limits['room_name']:
            violations.append((listing_num, 'room_name', len(arr['room']), field_limits['room_name']))
        if arr.get('arrangement') and len(arr['arrangement']) > field_limits['arrangement_value']:
            violations.append((listing_num, 'arrangement_value', len(arr['arrangement']), field_limits['arrangement_value']))
    
    # Check house rules
    for rule in listing.get('house_rules', []):
        if isinstance(rule, str) and len(rule) > field_limits['rule_text']:
            violations.append((listing_num, 'rule_text', len(rule), field_limits['rule_text']))
    
    # Check reviews
    for review in listing.get('reviews_details', []):
        if review.get('guest_name') and len(review['guest_name']) > field_limits['guest_name']:
            violations.append((listing_num, 'guest_name', len(review['guest_name']), field_limits['guest_name']))
        if review.get('guest_location') and len(review['guest_location']) > field_limits['guest_location']:
            violations.append((listing_num, 'guest_location', len(review['guest_location']), field_limits['guest_location']))
        if review.get('time_on_airbnb') and len(review['time_on_airbnb']) > field_limits['guest_time_on_airbnb']:
            violations.append((listing_num, 'guest_time_on_airbnb', len(review['time_on_airbnb']), field_limits['guest_time_on_airbnb']))
        if review.get('stayed_for') and len(review['stayed_for']) > field_limits['stayed_for']:
            violations.append((listing_num, 'stayed_for', len(review['stayed_for']), field_limits['stayed_for']))
    
    # Check location details
    for detail in listing.get('location_details', []):
        if detail.get('title') and len(detail['title']) > field_limits['detail_title']:
            violations.append((listing_num, 'detail_title', len(detail['title']), field_limits['detail_title']))
    
    # Check description sections
    for section in listing.get('description_by_sections', []):
        if section.get('title') and len(section['title']) > field_limits['section_title']:
            violations.append((listing_num, 'section_title', len(section['title']), field_limits['section_title']))
    
    # Check cancellation policies
    for policy in listing.get('cancellation_policy', []):
        if policy.get('policy') and len(policy['policy']) > field_limits['policy_name']:
            violations.append((listing_num, 'policy_name', len(policy['policy']), field_limits['policy_name']))

print(f"\n📊 Found {len(violations)} field length violations:\n")

if violations:
    for listing_num, field_name, actual_len, max_len in violations:
        print(f"🔴 Listing {listing_num}: {field_name}")
        print(f"   Actual: {actual_len} chars | Max: {max_len} chars | Excess: {actual_len - max_len}")
        print()
else:
    print("✅ No field length violations found!")

print("=" * 80)
print(f"\n💡 Solution: All violations need truncation in etl_airbnb_normalized_postgres.py")
