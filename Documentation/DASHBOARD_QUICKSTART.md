# Dashboard Quick Start Guide

## ✅ Pre-Flight Checklist

Before running the dashboard, ensure:

1. **Database is running**: PostgreSQL server is active
2. **ETL completed**: Dimensional database populated with data
3. **Environment variables set**: `.env` file configured
4. **Dependencies installed**: All packages available

## 🚀 Launch Dashboard

### Option 1: Command Line

```bash
streamlit run dashboard_executive_overview.py
```

### Option 2: Windows Command Prompt

```cmd
streamlit run dashboard_executive_overview.py
```

### Option 3: VS Code Terminal

```bash
# Activate UV environment (if using UV)
streamlit run dashboard_executive_overview.py
```

The dashboard will automatically open in your browser at:
```
http://localhost:8501
```

## 📋 First Time Setup

### 1. Verify Database Connection

The dashboard will show an error if it can't connect to the database. Check:

```bash
# Test database connection
python -c "import dashboard_db_utils as db; conn = db.create_connection(); print('Connected!' if conn else 'Failed')"
```

### 2. Check Environment Variables

Ensure `.env` file exists with:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
TARGET_DB_NAME=airbnb_dimensional
```

### 3. Verify Data Exists

```sql
-- Connect to PostgreSQL and check:
SELECT COUNT(*) FROM dim_property;  -- Should return > 0
SELECT COUNT(*) FROM bridge_listing_competitors;  -- Should return > 0
```

## 🎯 Using the Dashboard

### Step 1: Select Property
- Use any of the 3 filters at the top
- Property ID dropdown shows all available properties
- Other fields auto-populate based on selection

### Step 2: Review KPI Metrics
- 4 key metrics displayed at the top
- Color-coded for quick assessment
- Delta values show comparison to benchmarks

### Step 3: Navigate Tabs

**Tab 1: Overview** 📋
- View property details
- Check host information
- Review ratings breakdown

**Tab 2: Competitors** 🏆
- See top 25 similar listings
- Analyze similarity scores
- Review 4 visualizations

**Tab 3: Pricing** 💰
- Compare to market benchmarks
- View price recommendations
- Analyze competitiveness

**Tab 4: Recommendations** 🎯
- Read strategic insights
- Review strengths and improvements
- Follow action plan

## 🔧 Troubleshooting

### Dashboard Won't Start

**Error**: `ModuleNotFoundError: No module named 'streamlit'`

**Solution**:
```bash
uv add streamlit
# or
pip install streamlit
```

### Database Connection Failed

**Error**: "Database connection failed: FATAL: password authentication failed"

**Solution**:
1. Check `.env` file has correct password
2. Verify PostgreSQL is running
3. Test connection manually

### No Properties Found

**Error**: "No properties found in database"

**Solution**:
1. Run ETL process to populate database
2. Verify `TARGET_DB_NAME` is correct
3. Check `dim_property` table has data

### Visualization Errors

**Error**: "KeyError: 'column_name'"

**Solution**:
1. Ensure ETL completed successfully
2. Check all views exist in database
3. Refresh materialized views

## 🎨 Dashboard Features

### Interactive Elements

**Filters** (Top of page)
- Real-time property selection
- Auto-sync across filters

**Metrics Cards**
- Color-coded status
- Delta indicators
- Hover for details

**Data Table** (Competitors tab)
- Sortable columns
- Progress bars for similarity
- Full competitor details

**Charts** (All tabs)
- Hover for details
- Zoom and pan
- Download as image
- Interactive legends

### Keyboard Shortcuts

- `r` - Rerun app
- `c` - Clear cache
- `?` - Show keyboard shortcuts

## 📊 Sample Workflow

### Scenario: Optimize Pricing for Your Listing

1. **Select your property** using Property ID filter
2. **Check Pricing Status** in KPI metrics (top right)
3. **Navigate to Pricing tab** (Tab 3)
4. **Review recommendations**:
   - Optimal price
   - Price range
   - Market position
5. **Check Competitors tab** (Tab 2) to see:
   - Who are your competitors
   - Their pricing strategies
   - Your competitive position
6. **Review Recommendations tab** (Tab 4) for:
   - Specific action items
   - Timeline for implementation
   - Expected impact

### Scenario: Understand Competitive Position

1. **Select your property**
2. **Navigate to Competitors tab** (Tab 2)
3. **Analyze visualizations**:
   - Price Distribution: Where do you stand?
   - Similarity Components: What makes listings similar?
   - Radar Chart: What are your strengths?
   - Heatmap: How do you compare feature-by-feature?
4. **Review top 25 competitors** in the table
5. **Note similarities** and **differences**

## 💡 Tips for Non-Technical Users

### Understanding the Metrics

**Competitiveness Score** (0-100)
- 70-100: Excellent (Green zone)
- 40-70: Good (Yellow zone)
- 0-40: Needs improvement (Red zone)

**Pricing Status**
- ✓ OPTIMAL: Your price is just right
- ⚠️ OVERPRICED: Consider reducing
- 💡 UNDERPRICED: Opportunity to increase

**Similarity Score** (0-100)
- Higher = More similar
- Rank 1 = Most similar competitor
- Used to calculate pricing recommendations

### Reading the Charts

**Price Distribution Histogram**
- Shows how many competitors at each price point
- Your price marked with red line
- Recommended price marked with green line
- Green shaded area = recommended range

**Similarity Bar Chart**
- Shows what makes listings similar
- Higher bars = stronger match
- Percentages show algorithm weights

**Radar Chart**
- Pentagon shape comparing you to competitors
- Larger area = better performance
- Blue = Your listing
- Orange = Competitor average

**Gauge Chart**
- Speedometer showing competitiveness
- Needle position = your score
- Color zones indicate performance level

## 🔄 Refreshing Data

Dashboard caches data for 5 minutes to improve performance.

To force refresh:
1. Press `r` to rerun
2. Or restart the dashboard

## 📱 Mobile View

The dashboard is responsive but best viewed on:
- Desktop (1920x1080 recommended)
- Tablet (1024x768 minimum)
- Large mobile screens (landscape mode)

## 🆘 Getting Help

If you encounter issues:

1. **Check error messages** in red boxes
2. **Review browser console** (F12)
3. **Check terminal output** where dashboard is running
4. **Refer to main documentation**: `Documentation/README_DASHBOARD.md`

## 🎓 Next Steps

After getting comfortable with the dashboard:

1. **Explore different properties** - Compare multiple listings
2. **Track changes over time** - Monitor competitor pricing
3. **Test pricing changes** - Use recommendations to adjust prices
4. **Analyze patterns** - Look for seasonal trends
5. **Share insights** - Export data for reports

---

**Ready?** Run the dashboard:

```bash
streamlit run dashboard_executive_overview.py
```

Your dashboard will open automatically! 🎉
