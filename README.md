#  Rundown of ETL-project (Car_sales)
    Dataset Preparation: We sourced the car_info dataset from Kaggle and uploaded it to Databricks for analysis.
    Data Loading: The dataset was loaded into a Spark DataFrame from its Delta Lake location.
    Data Cleaning: We removed duplicates based on VIN, handled null values, and standardized column types for consistency.
    Data Transformation: Calculated car_age dynamically, filtered records based on conditions, and created derived metrics like price differences and average prices.
    Delta Table Creation: Cleaned and transformed data was saved as Delta tables for structured querying and analysis.
    SQL Queries: Crafted 20 SQL queries to gain insights, such as top-selling models, price trends, and high-value sellers.
    Power BI Integration: Exported the transformed data to Power BI for visualization and reporting.
    Dashboards Creation: Built four dashboards to analyze sales trends, pricing comparisons, customer preferences, and seller performance.
    DAX Calculations: Used DAX formulas for advanced metrics like profit margins, yearly trends, and conditional formatting.
    Interactive Features: Added slicers for filtering by year, month, and quarter to enable dynamic analysis.
    Created a price comparison chart with a trendline to analyze correlations between estimated market value and selling price.
    Visualized sales over time and analyzed sales distribution by car type and seller.
    Data Export: Successfully exported insights and models while managing notebook size constraints.
    Collaboration: Linked the Databricks workspace to GitHub for version control and backup.
    Completion: Delivered a fully functional data pipeline and interactive reports for actionable insights.

This project successfully combined ETL processes, advanced SQL, and visual storytelling to provide a comprehensive car sales analysis. 🚗📊
