import os
import sys
from datetime import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import statsmodels.api as sm

# --- CONFIGURATION ---
DATA_FILE_PATH = "data/bls_data.csv"
st.set_page_config(layout="wide", page_title="US Labor & Economic Dashboard (BLS Data)")

@st.cache_data
def load_data(path):
    """Loads the processed data from CSV, caches it for fast dashboard loading."""
    try:
        df = pd.read_csv(path, parse_dates=['Date'])
        # Set date column is the index
        df.set_index('Date', inplace=True)
        return df
    except FileNotFoundError:
        st.error(f"Data file not found at {path}. Please run the data collection script first.")
        # Log this error explicitly too
        with open('/content/app_debug_logs.txt', 'a') as f:
            f.write(f'[DEBUG APP] FileNotFoundError for {path} at {datetime.now()}\n')
        return pd.DataFrame()

# Load the data
df = load_data(DATA_FILE_PATH)

if not df.empty:

    # --- SIDEBAR FILTERS (UPDATED) ---
    with st.sidebar:
        st.header("Dashboard Controls")

        # 1. Date Range Slider (KEPT)
        st.subheader("Select Date Range")
        full_min_datetime = df.index.min().to_pydatetime()
        full_max_datetime = df.index.max().to_pydatetime()

        start_date_slider, end_date_slider = st.slider(
            'Filter data between:',
            min_value=full_min_datetime,
            max_value=full_max_datetime,
            value=(full_min_datetime, full_max_datetime),
            format="YYYY-MM-DD"
        )

        start_timestamp = pd.to_datetime(start_date_slider)
        end_timestamp = pd.to_datetime(end_date_slider)
        df_filtered = df.loc[start_timestamp:end_timestamp]

        st.markdown("---")

        # 2. Last Date Updated (NEW)
        st.subheader("Data Status")
        last_date = df.index[-1].strftime('%B %Y')
        st.info(f" **Last Date Updated:** {last_date}")

        st.markdown("---")

        # 3. Quick Links / Table of Contents (NEW)
        # Note: These are not true links but aid navigation on longer pages.
        st.subheader("Quick Navigation")
        st.markdown(
            """
            * [Labor Market Conditions](#labor-market-conditions)
            * [Productivity and Hours](#productivity-and-hours)
            * [Inflation and Trade](#inflation-and-trade)
            * [Statistical Analysis](#statistical-analysis-unemployment-vs-employment)
            """
        )

    #  MAIN DASHBOARD HEADER
    st.title("US Labor & Economic Indicators Dashboard")
    st.markdown("""
        This interactive dashboard showcases key labor market, productivity, inflation, and trade
        trends using data collected from the US Bureau of Labor Statistics (BLS) Public API.

    """)
    st.markdown("---")


    # EMPLOYMENT & LABOR PANEL
    # Added the anchor tag format (using header ID) for the Quick Navigation link
    st.header(" Labor Market Conditions", anchor="labor-market-conditions")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Unemployment Rate (SA)")
        fig_unemp = px.line(
            df_filtered,
            y='Unemployment_Rate_SA',
            title='Unemployment Rate Over Time',
            labels={'Unemployment_Rate_SA': 'Rate (%)'},
            height=400
        )
        st.plotly_chart(fig_unemp, use_container_width=True)

    with col2:
        st.subheader("Total Nonfarm Employment (SA)")
        fig_nonfarm = px.line(
            df_filtered,
            y='Total_Nonfarm_Employment_SA',
            title='Total Nonfarm Employment',
            labels={'Total_Nonfarm_Employment_SA': 'Employment (Thousands)'},
            height=400
        )
        st.plotly_chart(fig_nonfarm, use_container_width=True)

    st.markdown("---")


    #  PRODUCTIVITY PANEL
    # Added the anchor tag format (using header ID) for the Quick Navigation link
    st.header(" Productivity and Hours", anchor="productivity-and-hours")
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Output Per Hour - Non-farm Business")

        OUTPUT_COLUMN = 'Output_Per_Hour_NF'
        if OUTPUT_COLUMN in df_filtered.columns:

            fig_output = px.line(
                df_filtered.dropna(subset=[OUTPUT_COLUMN]), # Drop missing rows
                x=df_filtered.dropna(subset=[OUTPUT_COLUMN]).index,
                y=OUTPUT_COLUMN,
                title='Output Per Hour Index (Quarterly)',
                height=400,
                labels={'Output_Per_Hour_NF': 'Index Value'}
            )

            fig_output.update_traces(
                mode='lines+markers',
                line=dict(width=3, color='firebrick'),
                marker=dict(size=8, symbol='circle', line=dict(width=1, color='DarkSlateGrey')),
                connectgaps=False
            )

            fig_output.update_layout(
                xaxis_title='Date',
                yaxis_title='Index Value'
            )

            st.plotly_chart(fig_output, use_container_width=True)
            st.info(" **Note:** This series is released **quarterly**, resulting in only four points per year.")

        else:
            st.warning(f"Data series '{OUTPUT_COLUMN}' not found in the data.")

    with col4:
        st.subheader("Total Private Average Weekly Hours")
        fig_hours = px.line(
            df_filtered,
            y='Avg_Weekly_Hours_Private_SA',
            title='Average Weekly Hours',
            height=400
        )
        st.plotly_chart(fig_hours, use_container_width=True)

    st.markdown("---")


    # INFLATION & TRADE PANELS
    # Added the anchor tag format (using header ID) for the Quick Navigation link
    st.header(" Inflation and Trade", anchor="inflation-and-trade")

    st.subheader("CPI-U Less Food and Energy (Unadjusted)")

    CPI_COLUMN = 'CPI_U_Ex_Food_Energy_U'

    if CPI_COLUMN in df_filtered.columns:
        # Calculate Year-over-Year (YoY) percentage change
        df_filtered_cpi = df_filtered.copy()
        df_filtered_cpi['YoY_Change'] = df_filtered_cpi[CPI_COLUMN].pct_change(periods=12) * 100

        # Create the bar chart for CPI
        fig_cpi = px.bar(
            df_filtered_cpi.dropna(subset=['YoY_Change']).reset_index(), # Drop initial from pct_change
            x='Date',
            y='YoY_Change',
            title='Inflation: CPI-U Less Food and Energy (YoY % Change)',
            labels={'YoY_Change': 'Year-over-Year Change (%)'},
            height=400,
            color='YoY_Change',
            color_continuous_scale=px.colors.diverging.RdYlGn_r
        )

        fig_cpi.add_hline(y=0, line_dash="solid", line_color="black")

        st.plotly_chart(fig_cpi, use_container_width=True)

    else:
        st.warning(f"Data series '{CPI_COLUMN}' not found in the CSV. Cannot display inflation chart.")


    st.subheader("Imports vs. Exports (All Commodities)")

    df_trade = df_filtered.copy()
    df_trade['Trade_Balance'] = df_trade['Exports_All_Commodities_U'] - df_trade['Imports_All_Commodities_U']

    # Combine the data for Plotly (Exports and Imports on the same axis)
    df_trade_melt = df_trade[['Exports_All_Commodities_U', 'Imports_All_Commodities_U']].reset_index().melt(
        id_vars='Date',
        var_name='Series',
        value_name='Value'
    )

    fig_trade = px.line(
        df_trade_melt,
        x='Date',
        y='Value',
        color='Series',
        title='Trade: Imports and Exports for All Commodities',
        labels={'Value': 'Value ($ Billions?)'},
        height=400
    )

    st.plotly_chart(fig_trade, use_container_width=True)


    st.markdown("---")

    # STATISTICAL ANALYSIS PANEL (OLS)
    # Added the anchor tag format (using header ID) for the Quick Navigation link
    st.header(" Statistical Analysis: Unemployment vs. Employment", anchor="statistical-analysis-unemployment-vs-employment")
    st.markdown(
        "**Objective:** Analyze the relationship between the **Unemployment Rate** "
        "and **Total Nonfarm Employment** using Ordinary Least Squares (OLS)."
    )

    # Prepare data for OLS
    ols_df = df_filtered[['Unemployment_Rate_SA', 'Total_Nonfarm_Employment_SA']].dropna()

    if len(ols_df) > 5:
        try:
            # Define Variables
            Y = ols_df['Unemployment_Rate_SA']
            X = ols_df['Total_Nonfarm_Employment_SA']
            X = sm.add_constant(X) # Add the intercept term

            # Run OLS Regression
            model = sm.OLS(Y, X)
            results = model.fit()

            # Display key results
            col_ols1, col_ols2 = st.columns(2)
            with col_ols1:
                st.info(f"R-squared: **{results.rsquared:.4f}**")
                st.info(f"P-value (Nonfarm Employment): **{results.pvalues[1]:.4f}**")
                st.info(f"Coefficient (Employment): **{results.params[1]:.4e}**") # Scientific notation for small coef

            with col_ols2:
                # Create a scatter plot with the OLS line
                fig_ols = px.scatter(
                    ols_df,
                    x='Total_Nonfarm_Employment_SA',
                    y='Unemployment_Rate_SA',
                    title='OLS Regression: Unemployment vs. Nonfarm Employment',
                    trendline="ols", # Automatically adds the OLS line
                    height=500
                )
                st.plotly_chart(fig_ols, use_container_width=True)

        except Exception as e:
            st.error(f"Could not perform OLS regression with the selected data: {e}")
    else:

        st.warning("Not enough data points selected to perform OLS regression. Please widen the date range.")
