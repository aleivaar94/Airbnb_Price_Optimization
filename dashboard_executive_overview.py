"""
Airbnb Competitive Intelligence Dashboard - Executive Overview
==============================================================

Interactive Streamlit dashboard for Airbnb property competitive analysis
and price optimization. Displays top 25 competitors, pricing recommendations,
and strategic insights.

Dashboard Structure
-------------------
- Filters: Property ID, Listing Title, URL
- Hero Metrics: 4 KPI cards
- Tab 1: Overview (property summary)
- Tab 2: Competitors (table + visualizations)
- Tab 3: Pricing (metrics + charts)
- Tab 4: Recommendations (strategic insights)

Usage
-----
    streamlit run dashboard_executive_overview.py

Environment Variables
--------------------
DB_HOST, DB_PORT, TARGET_DB_NAME, DB_USER, DB_PASSWORD
(see dashboard_db_utils.py for details)

Author: Data Science Team
Date: 2025-01-14
"""

import streamlit as st
import pandas as pd
import numpy as np
import dashboard_db_utils as db_utils
import dashboard_visualizations as viz

# Page configuration
st.set_page_config(
    page_title="RankBreeze Competitive Intelligence",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .logo-container {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-bottom: 1rem;
    }
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF5A5F;
        text-align: center;
        margin-bottom: 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding-left: 24px;
        padding-right: 24px;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Logo and Title
col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    st.image("assets/images/RankBreeze-Logo-Purple.png", width=500)

st.markdown('<h1 class="main-header">Competitive Intelligence Dashboard</h1>', 
            unsafe_allow_html=True)

# ============================================================================
# SECTION 1: DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_connection():
    """Get cached database connection."""
    return db_utils.create_connection()

conn = get_connection()

if not conn:
    st.error("❌ Failed to connect to database. Please check your environment variables.")
    st.stop()

# ============================================================================
# SECTION 2: FILTERS
# ============================================================================

st.markdown("### 🔍 Select Property")

# Load property list
properties_df = db_utils.get_property_list(conn)

if properties_df.empty:
    st.error("No properties found in database. Please run the ETL process first.")
    st.stop()

# Create filters in three columns
col1, col2, col3 = st.columns(3)

with col1:
    # Property ID filter
    property_ids = properties_df['property_id'].tolist()
    selected_property_id = st.selectbox(
        "Property ID",
        options=property_ids,
        format_func=lambda x: f"ID: {x}",
        help="Select property by unique identifier"
    )

# Update other filters based on selected property_id
selected_property = properties_df[properties_df['property_id'] == selected_property_id].iloc[0]

with col2:
    # Listing Title (display only, synced with property_id)
    st.text_input(
        "Listing Title",
        value=selected_property['listing_title'] if pd.notna(selected_property['listing_title']) else "N/A",
        disabled=True,
        help="Listing title from selected property"
    )

with col3:
    # URL (display only, synced with property_id)
    url_display = selected_property['url'] if pd.notna(selected_property['url']) else "N/A"
    if url_display != "N/A":
        url_display = url_display.split('/')[-1]  # Show only listing ID part
    st.text_input(
        "Property URL",
        value=url_display,
        disabled=True,
        help="Airbnb listing URL"
    )

st.divider()

# ============================================================================
# SECTION 3: LOAD DATA FOR SELECTED PROPERTY
# ============================================================================

# Load property data
property_df = db_utils.get_property_overview(conn, selected_property_id)

if property_df.empty:
    st.error(f"Property ID {selected_property_id} not found in database.")
    st.stop()

property_data = property_df.iloc[0].to_dict()

# Load competitors
competitors_df = db_utils.get_top_competitors(conn, selected_property_id)

# Load pricing analysis
pricing_df = db_utils.get_pricing_analysis(conn, selected_property_id)

# ============================================================================
# SECTION 4: HERO METRICS
# ============================================================================

st.markdown("### 📊 Key Performance Indicators")

col1, col2, col3, col4 = st.columns(4)

with col1:
    price_delta = None
    if not pricing_df.empty and pd.notna(pricing_df['price_difference'].iloc[0]):
        price_delta = f"${pricing_df['price_difference'].iloc[0]:.0f} vs optimal"
    
    st.metric(
        label="💰 Price per Night",
        value=f"${property_data['price_per_night']:.0f}",
        delta=price_delta,
        delta_color="inverse" if price_delta and pricing_df['price_difference'].iloc[0] > 0 else "normal"
    )

with col2:
    st.metric(
        label="⭐ Overall Rating",
        value=f"{property_data['listing_rating']:.2f}",
        delta=property_data['quality_tier']
    )

with col3:
    comp_score = property_data['competitiveness_score']
    comp_percentile = int((comp_score / 100) * 100)
    st.metric(
        label="🎯 Competitiveness",
        value=f"{comp_score:.0f}/100",
        delta=f"{comp_percentile}th percentile"
    )

with col4:
    if not pricing_df.empty and pd.notna(pricing_df['pricing_status'].iloc[0]):
        pricing_status = pricing_df['pricing_status'].iloc[0]
        status_emoji = {"OPTIMAL": "✓", "OVERPRICED": "⚠️", "UNDERPRICED": "💡"}
        price_premium = pricing_df['price_premium_discount'].iloc[0]
        
        st.metric(
            label="📊 Pricing Status",
            value=f"{status_emoji.get(pricing_status, '?')} {pricing_status}",
            delta=f"{abs(price_premium):.1f}% vs market"
        )
    else:
        st.metric(
            label="📊 Pricing Status",
            value="N/A",
            delta="No data"
        )

st.divider()

# ============================================================================
# SECTION 5: TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Overview", 
    "🏆 Competitors", 
    "💰 Pricing", 
    "🎯 Recommendations"
])

# ============================================================================
# TAB 1: OVERVIEW
# ============================================================================

with tab1:
    st.header("📸 Property Overview")
    
    # Property details in two columns
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader(f"{property_data['listing_title']}")
        st.write(f"**Category:** {property_data['category']}")
        st.write(f"**Location:** {property_data['city']}, {property_data['province']} ({property_data['location_tier']})")
        st.write(f"**Distance to Downtown:** {property_data['distance_to_downtown_km']:.2f} km")
        
        # Property specs in columns
        spec_col1, spec_col2, spec_col3, spec_col4 = st.columns(4)
        spec_col1.metric("🛏️ Bedrooms", int(property_data['bedrooms']) if pd.notna(property_data['bedrooms']) else 0)
        spec_col2.metric("🛋️ Beds", int(property_data['beds']) if pd.notna(property_data['beds']) else 0)
        spec_col3.metric("🚿 Bathrooms", f"{property_data['baths']:.1f}" if pd.notna(property_data['baths']) else "0")
        spec_col4.metric("👥 Capacity", int(property_data['guests_capacity']) if pd.notna(property_data['guests_capacity']) else 0)
    
    with col2:
        st.subheader("Host Information")
        st.write(f"**Name:** {property_data['host_name']}")
        st.write(f"**Rating:** {property_data['host_rating']:.2f} ⭐")
        st.write(f"**Tier:** {property_data['host_tier']}")
        if property_data['is_superhost']:
            st.success("✅ Superhost")
        
        st.subheader("Amenities")
        st.write(f"**Total:** {int(property_data['total_amenities_count'])}")
        st.write(f"**Tier:** {property_data['amenity_tier']}")
        st.write(f"**Score:** {int(property_data['amenity_score'])}")
    
    st.divider()
    
    # Ratings breakdown
    st.subheader("⭐ Ratings Breakdown")
    rating_col1, rating_col2, rating_col3, rating_col4, rating_col5, rating_col6 = st.columns(6)
    
    rating_col1.metric("Overall", f"{property_data['listing_rating']:.2f}")
    rating_col2.metric("Cleanliness", f"{property_data['cleanliness_rating']:.2f}")
    rating_col3.metric("Accuracy", f"{property_data['accuracy_rating']:.2f}")
    rating_col4.metric("Location", f"{property_data['location_rating']:.2f}")
    rating_col5.metric("Value", f"{property_data['value_rating']:.2f}")
    rating_col6.metric("Reviews", int(property_data['number_of_reviews']))

# ============================================================================
# TAB 2: COMPETITORS
# ============================================================================

with tab2:
    st.header("🏆 Competitive Analysis")
    
    if competitors_df.empty:
        st.warning("No competitor data available for this property.")
    else:
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Competitors Analyzed", len(competitors_df))
        col2.metric("Avg Similarity", f"{competitors_df['overall_similarity_score'].mean():.1f}%")
        col3.metric("Avg Distance", f"{competitors_df['distance_km'].mean():.2f} km")
        
        st.divider()
        
        # Top 25 Competitors Table
        st.subheader("📊 Top 25 Most Similar Competitors")
        
        st.dataframe(
            competitors_df,
            column_config={
                "similarity_rank": st.column_config.NumberColumn("Rank", width="small"),
                "competitor_property_id": st.column_config.TextColumn("Property ID", width="medium"),
                "competitor_listing_title": st.column_config.TextColumn("Listing Title", width="large"),
                "competitor_name": st.column_config.TextColumn("Listing Name", width="large"),
                "overall_similarity_score": st.column_config.ProgressColumn(
                    "Similarity",
                    min_value=0,
                    max_value=100,
                    format="%d%%",
                    width="medium"
                ),
                "competitor_price": st.column_config.NumberColumn(
                    "Price",
                    format="$%.0f",
                    width="small"
                ),
                "price_diff_pct": st.column_config.NumberColumn(
                    "Price Diff %",
                    format="%.1f%%",
                    help="Percentage difference from your price"
                ),
                "distance_km": st.column_config.NumberColumn(
                    "Distance (km)",
                    format="%.2f km"
                ),
                "competitor_rating": st.column_config.NumberColumn(
                    "Rating",
                    format="%.2f ⭐"
                ),
                "competitor_bedrooms": "Beds"
            },
            hide_index=True,
            width='stretch',
            height=400
        )
        
        st.divider()
        
        # Visualizations
        st.subheader("📈 Competitive Insights")
        
        # Row 1: Price Distribution & Similarity Components
        col1, col2 = st.columns([3, 2], gap="large")
        
        with col1:
            optimal_price = None
            lower_bound = None
            upper_bound = None
            
            if not pricing_df.empty:
                optimal_price = pricing_df['recommended_optimal_price'].iloc[0] if pd.notna(pricing_df['recommended_optimal_price'].iloc[0]) else None
                lower_bound = pricing_df['recommended_price_lower'].iloc[0] if pd.notna(pricing_df['recommended_price_lower'].iloc[0]) else None
                upper_bound = pricing_df['recommended_price_upper'].iloc[0] if pd.notna(pricing_df['recommended_price_upper'].iloc[0]) else None
            
            fig_price_dist = viz.create_price_distribution_histogram(
                competitors_df['competitor_price'],
                property_data['price_per_night'],
                optimal_price,
                lower_bound,
                upper_bound
            )
            st.plotly_chart(fig_price_dist, width='stretch')
        
        with col2:
            fig_similarity = viz.create_similarity_bar_chart(competitors_df)
            st.plotly_chart(fig_similarity, width='stretch')
        
        # Add vertical spacing
        st.markdown("<div style='margin-top: 2rem;'></div>", unsafe_allow_html=True)
        
        st.divider()
        
        # Competitive Positioning Radar
        # Calculate property scores (normalized to 0-100)
        your_scores = [
            competitors_df['location_similarity'].mean(),  # Use avg similarity as proxy
            80,  # Property match (simplified)
            (property_data['overall_quality_score'] / 5.0) * 100,
            (property_data['amenity_score'] / 100) * 100,
            100 - abs(pricing_df['price_premium_discount'].iloc[0]) if not pricing_df.empty else 80
        ]
        
        # Competitor average scores
        comp_scores = [
            competitors_df['location_similarity'].mean(),
            competitors_df['property_similarity'].mean(),
            competitors_df['quality_similarity'].mean(),
            competitors_df['amenity_similarity'].mean(),
            competitors_df['price_similarity'].mean()
        ]
        
        fig_radar = viz.create_radar_chart(your_scores, comp_scores)
        st.plotly_chart(fig_radar, width='stretch')

# ============================================================================
# TAB 3: PRICING
# ============================================================================

with tab3:
    st.header("💰 Pricing Intelligence")
    
    if pricing_df.empty or pd.isna(pricing_df['recommended_optimal_price'].iloc[0]):
        st.warning("Pricing analysis not available for this property.")
    else:
        pricing_data = pricing_df.iloc[0].to_dict()
        
        # Pricing Status Banner
        pricing_status = pricing_data['pricing_status']
        if pricing_status == 'OPTIMAL':
            st.success("✅ Your pricing is OPTIMAL - well positioned in the market!")
        elif pricing_status == 'OVERPRICED':
            st.error(f"⚠️ Your listing is OVERPRICED by {pricing_data['price_premium_discount']:.1f}%")
        else:
            st.info(f"💡 Your listing is UNDERPRICED - opportunity to increase revenue by {abs(pricing_data['price_premium_discount']):.1f}%")
        
        st.divider()
        
        # Pricing Metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("Current Pricing")
            st.metric("Your Price", f"${property_data['price_per_night']:.0f}")
            st.metric("Price per Guest", f"${property_data['price_per_guest']:.0f}")
            st.metric("Price per Bedroom", f"${property_data['price_per_bedroom']:.0f}")
        
        with col2:
            st.subheader("Market Benchmarks")
            st.metric("Market Average", f"${pricing_data['avg_competitor_price']:.0f}")
            st.metric("Market Median", f"${pricing_data['median_competitor_price']:.0f}")
            st.metric("Weighted Avg", f"${pricing_data['weighted_avg_price']:.0f}")
        
        with col3:
            st.subheader("Recommendations")
            st.metric(
                "Optimal Price",
                f"${pricing_data['recommended_optimal_price']:.0f}",
                delta=f"${pricing_data['price_difference']:.0f}"
            )
            st.metric("Lower Bound", f"${pricing_data['recommended_price_lower']:.0f}")
            st.metric("Upper Bound", f"${pricing_data['recommended_price_upper']:.0f}")
        
        st.divider()
        
        # Visualizations
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("**Competitiveness Score Gauge**")
            fig_gauge = viz.create_gauge_chart(property_data['competitiveness_score'])
            st.plotly_chart(fig_gauge, width='stretch')
        
        with col2:
            st.markdown("**Price vs Rating Positioning**")
            if not competitors_df.empty:
                fig_scatter = viz.create_price_rating_scatter(
                    property_data,
                    competitors_df,
                    pricing_data['recommended_optimal_price'] if not pricing_df.empty else None
                )
                st.plotly_chart(fig_scatter, width='stretch')
        
        # Geographic Map
        if not competitors_df.empty:
            st.divider()
            st.markdown("**Geographic Distribution of Competitors**")
            
            # Add lat/lon to competitors if available
            # Note: This requires coordinate data in competitors query
            if 'latitude' in property_data and 'longitude' in property_data:
                fig_map = viz.create_competitor_map(property_data, competitors_df)
                if fig_map.data:  # Check if map was created successfully
                    st.plotly_chart(fig_map, width='stretch')
                else:
                    st.info("Geographic data not available for map visualization")

# ============================================================================
# TAB 4: RECOMMENDATIONS
# ============================================================================

with tab4:
    st.header("🎯 Strategic Action Plan")
    
    # Two-column layout for Strengths vs Improvements
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💪 Your Competitive Strengths")
        
        strengths = []
        if property_data['is_guest_favorite']:
            strengths.append("✅ Guest Favorite status")
        if property_data['is_superhost']:
            strengths.append("⭐ Superhost badge")
        if property_data['overall_quality_score'] > 4.7:
            strengths.append(f"🏆 Exceptional quality ({property_data['overall_quality_score']:.2f}/5.0)")
        if property_data['location_tier'] in ['Urban Core', 'Downtown Adjacent']:
            strengths.append(f"📍 Prime location ({property_data['location_tier']})")
        if property_data['amenity_tier'] in ['Luxury', 'Premium']:
            strengths.append(f"✨ {property_data['amenity_tier']} amenities")
        
        if strengths:
            for strength in strengths:
                st.success(strength)
        else:
            st.info("Build your competitive advantages by improving ratings and amenities")
    
    with col2:
        st.subheader("🔧 Areas for Improvement")
        
        improvements = []
        if property_data['number_of_reviews'] < 10:
            improvements.append(("📝", "Build review base", "Incentivize guests to leave reviews"))
        if property_data['cleanliness_rating'] < 4.8:
            improvements.append(("🧹", "Enhance cleanliness", "Critical rating factor"))
        if property_data['value_rating'] < 4.5:
            improvements.append(("💰", "Improve value perception", "Consider price or amenities"))
        if property_data['amenity_tier'] == 'Basic':
            improvements.append(("➕", "Add amenities", "WiFi, kitchen, workspace"))
        
        if improvements:
            for emoji, title, description in improvements:
                st.warning(f"{emoji} **{title}**: {description}")
        else:
            st.success("Well-positioned! Focus on maintaining quality")
    
    st.divider()
    
    # Detailed Action Plan
    with st.expander("📋 Detailed Action Plan", expanded=True):
        st.subheader("Immediate Actions (Next 7 Days)")
        
        if not pricing_df.empty:
            pricing_data = pricing_df.iloc[0].to_dict()
            pricing_status = pricing_data['pricing_status']
            
            if pricing_status == 'OVERPRICED':
                st.markdown(f"""
                1. **Reduce Price** ⚠️
                   - Current: ${property_data['price_per_night']:.0f}
                   - Recommended: ${pricing_data['recommended_optimal_price']:.0f}
                   - Reduction: ${abs(pricing_data['price_difference']):.0f} per night
                   - Expected impact: Increase bookings by ~{abs(pricing_data['price_premium_discount']):.0f}%
                """)
            elif pricing_status == 'UNDERPRICED':
                monthly_gain = abs(pricing_data['price_difference']) * 30
                yearly_gain = monthly_gain * 12
                st.markdown(f"""
                1. **Increase Price** 💡
                   - Current: ${property_data['price_per_night']:.0f}
                   - Recommended: ${pricing_data['recommended_optimal_price']:.0f}
                   - Increase: ${abs(pricing_data['price_difference']):.0f} per night
                   - Potential revenue gain:
                     - Monthly: ${monthly_gain:.0f} (assuming 30 bookings)
                     - Yearly: ${yearly_gain:.0f}
                """)
            else:
                st.success("✅ Maintain current pricing - it's optimal!")
        
        st.subheader("Short-term Actions (Next 30 Days)")
        st.markdown("""
        2. **Monitor Competition** - Track top 5 competitors weekly
        3. **Enhance Listing** - Update photos, description, amenities list
        4. **Guest Experience** - Focus on cleanliness and communication
        """)
        
        st.subheader("Long-term Strategy (Next 90 Days)")
        st.markdown("""
        5. **Amenity Upgrades** - Invest in high-impact amenities
        6. **Seasonal Pricing** - Adjust for Stampede and winter seasons
        7. **Marketing** - Optimize listing title and keywords
        """)

# ============================================================================
# FOOTER
# ============================================================================

st.divider()
st.caption("Dashboard powered by Streamlit | Data from Airbnb Dimensional Database")
st.caption("For support, contact: alejandro@rankbreeze.com")