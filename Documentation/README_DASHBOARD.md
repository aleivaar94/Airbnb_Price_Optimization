# Airbnb Competitive Intelligence Dashboard

Executive overview dashboard for analyzing Airbnb property competitiveness and pricing optimization.

## Features

### 🎯 Core Components
- **3 Property Filters**: Select by Property ID, Listing Title, or URL
- **4 KPI Metrics**: Price, Rating, Competitiveness Score, Pricing Status
- **Top 25 Competitors Table**: Full competitive analysis with similarity rankings
- **7 Interactive Visualizations**: Powered by Plotly

### 📊 Visualizations

#### Required (from notebook)
1. **Price Distribution Histogram** - Shows where your price sits in the competitive landscape
2. **Similarity Components Bar Chart** - Breaks down the 5-factor similarity algorithm

#### Additional
3. **Competitive Positioning Radar Chart** - Multi-dimensional comparison vs top 5 competitors
4. **Competitiveness Score Gauge** - Visual indicator of market position (0-100)
5. **Price vs Rating Scatter Plot** - Market positioning analysis
6. **Feature Comparison Heatmap** - Side-by-side comparison of key metrics
7. **Geographic Competitor Map** - Spatial distribution of competition

### 🎨 Dashboard Tabs

**Tab 1: Overview** - Property summary with host info, amenities, and ratings breakdown

**Tab 2: Competitors** - Top 25 table with all visualizations and competitive insights

**Tab 3: Pricing** - Market benchmarks, recommendations, and positioning charts

**Tab 4: Recommendations** - Strategic action plan with strengths and improvements

## Installation

### Prerequisites
- Python 3.8+
- PostgreSQL database with dimensional schema
- Environment variables configured

### Dependencies

The dashboard requires these packages (add to `pyproject.toml` or install directly):

```bash
uv add streamlit plotly pandas numpy psycopg2-binary python-dotenv
```

Or using pip:
```bash
pip install streamlit plotly pandas numpy psycopg2-binary python-dotenv
```

## Configuration

Create a `.env` file in the project root with the following variables:

```env
# Database Connection
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password_here

# Database Name (Dimensional Schema)
TARGET_DB_NAME=airbnb_dimensional

# Optional: Source Database (for reference)
SOURCE_DB_NAME=airbnb_db
```

## Usage

### Run Dashboard

```bash
streamlit run dashboard_executive_overview.py
```

The dashboard will open in your default browser at `http://localhost:8501`

### Module Structure

```
├── dashboard_executive_overview.py   # Main Streamlit app
├── dashboard_db_utils.py             # Database utilities
├── dashboard_visualizations.py       # Plotly chart functions
└── .env                             # Environment variables
```

## Database Requirements

The dashboard queries the following views/tables from the dimensional database:

### Views
- `view_listing_summary` - Denormalized property details
- `view_top_competitors` - Pre-computed competitor relationships
- `view_price_recommendations` - Pricing analysis and recommendations

### Tables (via views)
- `fact_listing_metrics` - Central fact table
- `dim_property` - Property characteristics
- `dim_location` - Geographic data
- `dim_host` - Host information
- `dim_category_ratings` - Quality metrics
- `fact_listing_amenities_summary` - Amenity aggregates
- `bridge_listing_competitors` - Competitor relationships

## Architecture

### Data Flow
```
PostgreSQL (Dimensional DB)
          ↓
  dashboard_db_utils.py
    (Cached queries)
          ↓
dashboard_executive_overview.py
    (Streamlit UI)
          ↓
dashboard_visualizations.py
    (Plotly charts)
```

### Caching Strategy
- Database queries cached for 5 minutes (`@st.cache_data(ttl=300)`)
- Connection cached for session (`@st.cache_resource`)
- Improves performance for repeated queries

## Features by Tab

### Tab 1: Overview
- Property details (beds, baths, capacity)
- Host information and reputation
- Amenity summary
- Ratings breakdown (6 categories)

### Tab 2: Competitors
- Interactive table with 25 competitors
- Similarity ranking (1-25)
- Price comparison and distance
- 4 visualizations:
  - Price distribution histogram
  - Similarity components bar chart
  - Competitive positioning radar
  - Feature comparison heatmap

### Tab 3: Pricing
- Current pricing metrics
- Market benchmarks (avg, median, weighted)
- Price recommendations (optimal, lower, upper)
- 3 visualizations:
  - Competitiveness gauge
  - Price vs rating scatter
  - Geographic competitor map

### Tab 4: Recommendations
- Competitive strengths (checkmarks)
- Areas for improvement (warnings)
- Detailed action plan:
  - Immediate actions (7 days)
  - Short-term actions (30 days)
  - Long-term strategy (90 days)

## Similarity Algorithm

The dashboard uses a 5-factor similarity algorithm to identify top 25 competitors:

| Factor | Weight | Components |
|--------|--------|------------|
| **Location** | 35% | Geographic distance + cluster membership |
| **Property** | 25% | Bedrooms, beds, baths, guest capacity |
| **Quality** | 20% | Rating alignment |
| **Amenity** | 10% | Shared amenities (Jaccard coefficient) |
| **Price** | 10% | Price range overlap |

**Overall Similarity Score** = Weighted sum of all factors (0-100)

## Pricing Optimization

### Algorithm
1. Calculate weighted average competitor price (using similarity scores)
2. Apply quality adjustment factor (±15% based on rating differential)
3. Determine price bounds (25th and 75th percentiles ± 5%)
4. Generate optimal price recommendation

### Pricing Status
- **OPTIMAL**: Price within recommended range
- **OVERPRICED**: Price above upper bound
- **UNDERPRICED**: Price below lower bound

## Troubleshooting

### Common Issues

**1. Database Connection Failed**
- Check `.env` file exists and contains correct credentials
- Verify PostgreSQL is running
- Confirm dimensional database exists and ETL has been run

**2. No Properties Found**
- Run ETL process to populate database
- Check `TARGET_DB_NAME` points to correct database

**3. No Competitor Data**
- Verify competitor analysis step in ETL completed
- Check `bridge_listing_competitors` table has data
- Refresh materialized view: `REFRESH MATERIALIZED VIEW view_top_competitors`

**4. Pricing Analysis Missing**
- Ensure `fact_competitor_pricing_analysis` is populated
- Check ETL process completed all steps

**5. Visualization Errors**
- Verify Plotly is installed: `pip show plotly`
- Check data types in returned DataFrames
- Ensure numeric columns don't contain NaN values

### Performance Optimization

If dashboard is slow:
- Increase cache TTL: Change `ttl=300` to `ttl=600` in `@st.cache_data`
- Limit competitor table rows: Modify query to return fewer columns
- Pre-compute more metrics in database views

## Data Sources

All data comes from the dimensional database created by the ETL process:
- **Source**: Normalized Airbnb database (`airbnb_db`)
- **ETL Script**: `etl_normalized_to_dimensional.py`
- **Target**: Dimensional database (`airbnb_dimensional`)

## Customization

### Adding New Visualizations

1. Create function in `dashboard_visualizations.py`:
```python
def create_new_chart(data: pd.DataFrame) -> go.Figure:
    """Create new chart."""
    fig = px.scatter(data, x='col1', y='col2')
    return fig
```

2. Import and use in dashboard:
```python
fig = viz.create_new_chart(competitors_df)
st.plotly_chart(fig, use_container_width=True)
```

### Modifying Filters

To add a fourth filter (e.g., by city):

1. Update filter section in `dashboard_executive_overview.py`
2. Modify `get_property_list()` to include city column
3. Add new `st.selectbox()` for city selection

### Changing Color Schemes

Plotly color scales can be changed in visualization functions:
- Histogram: `marker_color='skyblue'`
- Scatter: `color_continuous_scale='Viridis'`
- Heatmap: `colorscale='RdYlGn'`

Available scales: 'Viridis', 'Plasma', 'Inferno', 'Magma', 'Cividis', 'RdYlGn', 'RdBu', etc.

## Best Practices

### For Non-Technical Users
- Use filters at top to select different properties
- Look for color coding: 🟢 Green = good, 🟡 Yellow = caution, 🔴 Red = warning
- Hover over charts for detailed information
- Expand action plan for specific recommendations
- Focus on "Pricing Status" metric for quick assessment

### For Analysts
- Export data: Add `st.download_button()` to download CSVs
- Bookmark properties: Use URL parameters to save selections
- Compare multiple properties: Open dashboard in multiple tabs
- Schedule reports: Use Streamlit's deployment features for automation

## Support

For issues or questions:
- Check error messages in dashboard (red boxes)
- Review database connection status
- Consult ETL documentation for data pipeline issues
- Contact: data-team@example.com

## License

Internal use only - Airbnb Competitive Intelligence Project

## Version History

**v1.0.0** (2025-01-14)
- Initial release
- 7 interactive visualizations
- 4 tabbed sections
- Full competitor analysis
- Pricing recommendations

---

**Built with:** Streamlit, Plotly, PostgreSQL, Python 3.8+
