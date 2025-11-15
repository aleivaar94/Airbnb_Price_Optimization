"""
Visualization Functions for Airbnb Competitor Analysis Dashboard
================================================================

This module provides Plotly visualization functions for the Streamlit dashboard,
creating interactive charts for competitor analysis and price optimization.

Functions
---------
create_price_distribution_histogram : Price distribution with markers
create_similarity_bar_chart : Similarity components breakdown
create_radar_chart : Competitive positioning radar chart
create_gauge_chart : Competitiveness score gauge
create_price_rating_scatter : Price vs rating scatter plot
create_competitor_heatmap : Feature comparison heatmap
create_competitor_map : Geographic distribution map

Dependencies
------------
plotly.express : High-level plotting interface
plotly.graph_objects : Low-level plotting interface
pandas : Data manipulation
numpy : Numerical operations

Example
-------
>>> import dashboard_visualizations as viz
>>> import pandas as pd
>>> 
>>> # Create price distribution chart
>>> fig = viz.create_price_distribution_histogram(
...     competitor_prices=df['price'],
...     current_price=150.0,
...     optimal_price=145.0
... )
>>> fig.show()
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from typing import Optional, List, Union


def create_price_distribution_histogram(
    competitor_prices: pd.Series,
    current_price: float,
    optimal_price: Optional[float] = None,
    lower_bound: Optional[float] = None,
    upper_bound: Optional[float] = None
) -> go.Figure:
    """
    Create price distribution histogram with reference markers.
    
    Generates a histogram showing the distribution of competitor prices
    with vertical lines marking the current price, optimal price, and
    recommended price range.
    
    Parameters
    ----------
    competitor_prices : pd.Series
        Series of competitor prices
    current_price : float
        Current listing price
    optimal_price : float, optional
        Recommended optimal price
    lower_bound : float, optional
        Lower bound of recommended price range
    upper_bound : float, optional
        Upper bound of recommended price range
    
    Returns
    -------
    plotly.graph_objects.Figure
        Interactive histogram figure
    
    Example
    -------
    >>> prices = pd.Series([100, 120, 130, 140, 150, 160, 170])
    >>> fig = create_price_distribution_histogram(
    ...     competitor_prices=prices,
    ...     current_price=150.0,
    ...     optimal_price=145.0,
    ...     lower_bound=130.0,
    ...     upper_bound=160.0
    ... )
    >>> fig.show()
    
    Notes
    -----
    - Uses 15 bins by default for histogram
    - Current price marked with red dashed line
    - Optimal price marked with green dashed line
    - Recommended range shown as shaded green area
    """
    fig = go.Figure()
    
    # Create histogram
    fig.add_trace(go.Histogram(
        x=competitor_prices,
        nbinsx=15,
        name='Competitors',
        marker_color='skyblue',
        opacity=0.7,
        hovertemplate='Price: $%{x:.0f}<br>Count: %{y}<extra></extra>'
    ))
    
    # Add current price line
    fig.add_vline(
        x=current_price,
        line_dash="dash",
        line_color="red",
        line_width=2,
        annotation_text=f"Your Price: ${current_price:.0f}",
        annotation_position="top"
    )
    
    # Add optimal price line if provided
    if optimal_price is not None:
        fig.add_vline(
            x=optimal_price,
            line_dash="dash",
            line_color="green",
            line_width=2,
            annotation_text=f"Recommended: ${optimal_price:.0f}",
            annotation_position="top"
        )
    
    # Add recommended range shading if bounds provided
    if lower_bound is not None and upper_bound is not None:
        fig.add_vrect(
            x0=lower_bound,
            x1=upper_bound,
            fillcolor="green",
            opacity=0.2,
            layer="below",
            line_width=0,
            annotation_text="Recommended Range",
            annotation_position="top left"
        )
    
    fig.update_layout(
        title="Competitor Price Distribution Analysis",
        xaxis_title="Price per Night (CAD)",
        yaxis_title="Number of Competitors",
        showlegend=False,
        height=400,
        hovermode='x unified',
        template='plotly_white'
    )
    
    return fig


def create_similarity_bar_chart(competitors_df: pd.DataFrame) -> go.Figure:
    """
    Create bar chart showing average similarity scores by component.
    
    Displays the breakdown of the similarity algorithm showing how the
    property compares to competitors across 5 dimensions.
    
    Parameters
    ----------
    competitors_df : pd.DataFrame
        DataFrame with similarity columns (location_similarity,
        property_similarity, quality_similarity, amenity_similarity,
        price_similarity)
    
    Returns
    -------
    plotly.graph_objects.Figure
        Bar chart figure
    
    Example
    -------
    >>> df = pd.DataFrame({
    ...     'location_similarity': [80, 85, 90],
    ...     'property_similarity': [70, 75, 80],
    ...     'quality_similarity': [85, 90, 95],
    ...     'amenity_similarity': [60, 65, 70],
    ...     'price_similarity': [75, 80, 85]
    ... })
    >>> fig = create_similarity_bar_chart(df)
    >>> fig.show()
    
    Notes
    -----
    Similarity algorithm weights:
    - Location: 35% (geographic distance + cluster)
    - Property: 25% (bedrooms, beds, baths, capacity)
    - Quality: 20% (ratings alignment)
    - Amenity: 10% (shared amenities)
    - Price: 10% (price range overlap)
    """
    components = {
        'Location\n(35%)': competitors_df['location_similarity'].mean(),
        'Property\n(25%)': competitors_df['property_similarity'].mean(),
        'Quality\n(20%)': competitors_df['quality_similarity'].mean(),
        'Amenity\n(10%)': competitors_df['amenity_similarity'].mean(),
        'Price\n(10%)': competitors_df['price_similarity'].mean()
    }
    
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8']
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=list(components.keys()),
        y=list(components.values()),
        marker_color=colors,
        text=[f"{v:.1f}" for v in components.values()],
        textposition='outside',
        textfont=dict(size=14, color='white'),
        hovertemplate='<b>%{x}</b><br>Score: %{y:.1f}<extra></extra>'
    ))
    
    fig.update_layout(
        title="Similarity Components (Average Across Top 25)",
        yaxis_title="Average Similarity Score",
        xaxis_title="Component",
        yaxis_range=[0, 110],
        showlegend=False,
        height=400,
        hovermode='x',
        template='plotly_white'
    )
    
    fig.update_yaxes(gridcolor='lightgray', gridwidth=0.5)
    
    return fig


def create_radar_chart(
    property_scores: List[float],
    competitor_avg_scores: List[float],
    categories: Optional[List[str]] = None
) -> go.Figure:
    """
    Create radar chart comparing property to competitor average.
    
    Shows competitive positioning across multiple dimensions in a
    circular/spider chart format.
    
    Parameters
    ----------
    property_scores : list of float
        Scores for the property (5 values, 0-100 scale)
    competitor_avg_scores : list of float
        Average scores for top competitors (5 values, 0-100 scale)
    categories : list of str, optional
        Category labels (default: ['Location', 'Property', 'Quality', 
        'Amenity', 'Price'])
    
    Returns
    -------
    plotly.graph_objects.Figure
        Radar chart figure
    
    Example
    -------
    >>> your_scores = [85, 80, 90, 70, 75]
    >>> comp_scores = [75, 78, 85, 65, 72]
    >>> fig = create_radar_chart(your_scores, comp_scores)
    >>> fig.show()
    
    Notes
    -----
    All scores should be normalized to 0-100 scale for consistency.
    The chart uses 'toself' fill to create shaded areas.
    """
    if categories is None:
        categories = ['Location', 'Property', 'Quality', 'Amenity', 'Price']
    
    fig = go.Figure()
    
    # Add property trace
    fig.add_trace(go.Scatterpolar(
        r=property_scores,
        theta=categories,
        fill='toself',
        name='Your Listing',
        line_color='#1f77b4',
        fillcolor='rgba(31, 119, 180, 0.3)'
    ))
    
    # Add competitor average trace
    fig.add_trace(go.Scatterpolar(
        r=competitor_avg_scores,
        theta=categories,
        fill='toself',
        name='Top 5 Competitors Avg',
        line_color='#ff7f0e',
        fillcolor='rgba(255, 127, 14, 0.2)'
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 110],
                tickmode='linear',
                tick0=0,
                dtick=20,
                tickfont=dict(color='black'),
                gridcolor='lightgray'
            ),
            angularaxis=dict(
                tickfont=dict(color='white', size=12)
            )
        ),
        showlegend=True,
        title="Competitive Position Across 5 Dimensions",
        height=500,
        template='plotly_white',
        font=dict(color='black'),
        # margin=dict(l=100, r=100, t=100, b=100)
    )
    
    return fig


def create_gauge_chart(score: float, title: str = "Competitiveness Score") -> go.Figure:
    """
    Create gauge chart for competitiveness score.
    
    Displays a single metric (0-100) in a gauge/speedometer format
    with color-coded zones.
    
    Parameters
    ----------
    score : float
        Competitiveness score (0-100)
    title : str, optional
        Chart title (default: 'Competitiveness Score')
    
    Returns
    -------
    plotly.graph_objects.Figure
        Gauge chart figure
    
    Example
    -------
    >>> fig = create_gauge_chart(78.5)
    >>> fig.show()
    
    Notes
    -----
    Score zones:
    - 0-40: Red (Needs Improvement)
    - 40-70: Yellow (Good)
    - 70-100: Green (Excellent)
    """
    # Determine status based on score
    if score >= 70:
        bar_color = "green"
        status = "Excellent"
    elif score >= 40:
        bar_color = "orange"
        status = "Good"
    else:
        bar_color = "red"
        status = "Needs Improvement"
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        title={'text': title, 'font': {'size': 20}},
        delta={'reference': 70, 'suffix': ' vs target'},
        number={'suffix': f"/100"},
        gauge={
            'axis': {'range': [0, 100], 'tickwidth': 1},
            'bar': {'color': bar_color, 'thickness': 0.75},
            'steps': [
                {'range': [0, 40], 'color': "#ffcccc"},
                {'range': [40, 70], 'color': "#ffffcc"},
                {'range': [70, 100], 'color': "#ccffcc"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 4},
                'thickness': 0.75,
                'value': 70
            }
        }
    ))
    
    fig.add_annotation(
        text=f"<b>{status}</b>",
        x=0.5,
        y=0.15,
        showarrow=False,
        font=dict(size=16)
    )
    
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=60, b=20),
        template='plotly_white'
    )
    
    return fig


def create_price_rating_scatter(
    property_data: dict,
    competitors_df: pd.DataFrame,
    optimal_price: Optional[float] = None
) -> go.Figure:
    """
    Create scatter plot of price vs rating with similarity coloring.
    
    Shows market positioning by plotting price against rating,
    with point size representing number of reviews and color
    representing similarity score.
    
    Parameters
    ----------
    property_data : dict
        Dictionary with keys: listing_rating, price_per_night,
        number_of_reviews, listing_name
    competitors_df : pd.DataFrame
        Competitor data with columns: competitor_rating, competitor_price,
        competitor_reviews, competitor_name, overall_similarity_score
    optimal_price : float, optional
        Recommended optimal price (draws horizontal line)
    
    Returns
    -------
    plotly.graph_objects.Figure
        Scatter plot figure
    
    Example
    -------
    >>> property = {
    ...     'listing_rating': 4.8,
    ...     'price_per_night': 150.0,
    ...     'number_of_reviews': 50,
    ...     'listing_name': 'My Property'
    ... }
    >>> fig = create_price_rating_scatter(property, competitors_df, 145.0)
    >>> fig.show()
    
    Notes
    -----
    - Your property shown as large red star
    - Competitors colored by similarity score (Viridis scale)
    - Point size proportional to number of reviews
    """
    # Create competitor scatter
    fig = px.scatter(
        competitors_df,
        x='competitor_rating',
        y='competitor_price',
        size='competitor_reviews',
        color='overall_similarity_score',
        hover_name='competitor_name',
        hover_data={
            'competitor_rating': ':.2f',
            'competitor_price': ':$,.0f',
            'overall_similarity_score': ':.1f',
            'competitor_reviews': ':,d'
        },
        color_continuous_scale='Viridis',
        labels={
            'competitor_rating': 'Rating',
            'competitor_price': 'Price per Night (CAD)',
            'overall_similarity_score': 'Similarity Score',
            'competitor_reviews': 'Reviews'
        },
        title='Price vs Rating: Market Positioning'
    )
    
    # Add your property as a star marker
    fig.add_trace(go.Scatter(
        x=[property_data['listing_rating']],
        y=[property_data['price_per_night']],
        mode='markers',
        marker=dict(
            size=25,
            symbol='star',
            color='red',
            line=dict(color='black', width=2)
        ),
        name='Your Listing',
        hovertemplate=f"<b>{property_data['listing_name']}</b><br>" +
                     f"Rating: {property_data['listing_rating']:.2f}<br>" +
                     f"Price: ${property_data['price_per_night']:.0f}<br>" +
                     f"Reviews: {property_data['number_of_reviews']:,d}<extra></extra>"
    ))
    
    # Add optimal price line if provided
    if optimal_price is not None:
        fig.add_hline(
            y=optimal_price,
            line_dash="dash",
            line_color="green",
            opacity=0.5,
            annotation_text=f"Optimal: ${optimal_price:.0f}",
            annotation_position="right"
        )
    
    fig.update_layout(
        height=500,
        hovermode='closest',
        showlegend=True,
        template='plotly_white'
    )
    
    return fig


def create_competitor_heatmap(
    property_data: dict,
    competitors_df: pd.DataFrame,
    top_n: int = 10
) -> go.Figure:
    """
    Create heatmap comparing features across top N competitors.
    
    Shows side-by-side comparison of key metrics in a color-coded matrix,
    with your property highlighted at the top.
    
    Parameters
    ----------
    property_data : dict
        Dictionary with keys: price_per_night, listing_rating, bedrooms,
        amenity_score
    competitors_df : pd.DataFrame
        Competitor data (will use top N rows)
    top_n : int, optional
        Number of competitors to include (default: 10)
    
    Returns
    -------
    plotly.graph_objects.Figure
        Heatmap figure
    
    Example
    -------
    >>> property = {
    ...     'price_per_night': 150.0,
    ...     'listing_rating': 4.8,
    ...     'bedrooms': 2,
    ...     'amenity_score': 45
    ... }
    >>> fig = create_competitor_heatmap(property, competitors_df, top_n=10)
    >>> fig.show()
    
    Notes
    -----
    - Features are normalized to 0-1 scale for comparison
    - Original values shown in heatmap cells
    - Color scale: Red (low) to Yellow to Green (high)
    - Your listing appears in the first row
    """
    # Select top N competitors
    top_competitors = competitors_df.head(top_n)
    
    # Prepare data matrix
    data_matrix = []
    labels = ['Your Listing']
    feature_labels = ['Price', 'Rating', 'Bedrooms', 'Amenity Score']
    
    # Add your property
    your_row = [
        property_data.get('price_per_night', 0),
        property_data.get('listing_rating', 0),
        property_data.get('bedrooms', 0),
        property_data.get('amenity_score', 0)
    ]
    data_matrix.append(your_row)
    
    # Add competitors
    for _, competitor in top_competitors.iterrows():
        labels.append(f"#{int(competitor['similarity_rank'])}")
        competitor_row = [
            competitor.get('competitor_price', 0),
            competitor.get('competitor_rating', 0),
            competitor.get('competitor_bedrooms', 0),
            competitor.get('amenity_score', 0)
        ]
        data_matrix.append(competitor_row)
    
    # Convert to numpy array
    data_array = np.array(data_matrix)
    
    # Normalize columns to 0-1 scale for color mapping
    normalized_data = np.zeros_like(data_array)
    for i in range(data_array.shape[1]):
        col_min = data_array[:, i].min()
        col_max = data_array[:, i].max()
        if col_max > col_min:
            normalized_data[:, i] = (data_array[:, i] - col_min) / (col_max - col_min)
        else:
            normalized_data[:, i] = 0.5
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=normalized_data,
        x=feature_labels,
        y=labels,
        colorscale='RdYlGn',
        text=data_array,
        texttemplate='%{text:.1f}',
        textfont={"size": 10},
        hovertemplate='<b>%{y}</b><br>%{x}: %{text:.1f}<extra></extra>',
        colorbar=dict(title="Normalized<br>Value")
    ))
    
    fig.update_layout(
        title=f"Feature Comparison Matrix (Top {top_n})",
        height=450,
        xaxis_title="Features",
        yaxis_title="Listings",
        template='plotly_white'
    )
    
    # Add annotation to highlight your listing
    fig.add_annotation(
        text="⭐",
        x=-0.15,
        y=0,
        xref="x domain",
        yref="y",
        showarrow=False,
        font=dict(size=16, color='red')
    )
    
    return fig


def create_competitor_map(
    property_data: dict,
    competitors_df: pd.DataFrame,
    map_style: str = "open-street-map"
) -> go.Figure:
    """
    Create map showing geographic distribution of competitors.
    
    Plots your property and competitors on an interactive map,
    with color coding by similarity and size by price.
    
    Parameters
    ----------
    property_data : dict
        Dictionary with keys: latitude, longitude, listing_name,
        price_per_night
    competitors_df : pd.DataFrame
        Competitor data with columns: latitude, longitude, competitor_name,
        competitor_price, overall_similarity_score
    map_style : str, optional
        Mapbox style (default: 'open-street-map' - no token required)
        Options: 'open-street-map', 'carto-positron', 'carto-darkmatter'
    
    Returns
    -------
    plotly.graph_objects.Figure
        Map figure
    
    Example
    -------
    >>> property = {
    ...     'latitude': 51.0447,
    ...     'longitude': -114.0719,
    ...     'listing_name': 'My Property',
    ...     'price_per_night': 150.0
    ... }
    >>> fig = create_competitor_map(property, competitors_df)
    >>> fig.show()
    
    Notes
    -----
    - Uses Maplibre for rendering (no Mapbox token needed)
    - Your property shown with similarity score of 100
    - Competitors colored by similarity (Viridis scale)
    - Point size proportional to price
    - Map automatically centers and zooms to show all points
    """
    # Prepare competitor data
    map_df = competitors_df.copy()
    
    # Ensure required columns exist - return empty figure if no coordinates
    if 'latitude' not in map_df.columns and 'competitor_latitude' not in map_df.columns:
        # Return empty figure with message
        fig = go.Figure()
        fig.add_annotation(
            text="Geographic coordinates not available for map visualization",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="gray")
        )
        fig.update_layout(height=500)
        return fig
    
    # Add your property
    your_location = pd.DataFrame([{
        'latitude': property_data.get('latitude'),
        'longitude': property_data.get('longitude'),
        'name': property_data.get('listing_name', 'Your Listing'),
        'price': property_data.get('price_per_night', 0),
        'similarity': 100,
        'type': 'Your Listing'
    }])
    
    # Prepare competitor dataframe
    competitor_map_df = pd.DataFrame({
        'latitude': map_df.get('latitude', map_df.get('competitor_latitude')),
        'longitude': map_df.get('longitude', map_df.get('competitor_longitude')),
        'name': map_df.get('competitor_name', 'Competitor'),
        'price': map_df.get('competitor_price', 0),
        'similarity': map_df.get('overall_similarity_score', 0),
        'type': 'Competitor'
    })
    
    # Combine dataframes
    map_data = pd.concat([your_location, competitor_map_df], ignore_index=True)
    
    # Create scatter map
    fig = px.scatter_map(
        map_data,
        lat='latitude',
        lon='longitude',
        size='price',
        color='similarity',
        hover_name='name',
        hover_data={
            'price': ':$,.0f',
            'similarity': ':.1f',
            'latitude': False,
            'longitude': False,
            'type': True
        },
        color_continuous_scale='Viridis',
        size_max=20,
        zoom=12,
        height=500,
        title="Geographic Distribution of Competitors"
    )
    
    fig.update_layout(
        map_style=map_style,
        margin={"r": 0, "t": 30, "l": 0, "b": 0},
        showlegend=False,
        template='plotly_white'
    )
    
    return fig
