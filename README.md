# Cricket Data Analysis
This project aims to use open-source CSV files of ball-by-ball cricket data from the Indian Premier League between 2008 and 2025 and create a data warehouse to develop SQL views that can be used for data analysis.
The end goal is to have a dashboard on either Tableau or PowerBI to visualize the data extracted from the data warehouse. I also want this project to open complex analysis that looks beyond basic analytics and focuses on match situations to create complex analytics.

## Data Engineering approach
I am using the medallion approach to create my data warehouse. The idea is to separate the ETL process into separate stages:
- Bronze Layer: Focus on extracting the data only
- Silver Layer: Focus on transforming and enriching the data
- Gold Layer: Prepare the data to load into the data warehouse and use for analytics

Below is a data flow diagram showcasing how I envision my layers to operate:

### Source Files
I am using the CSV files from this dataset in Kaggle: https://www.kaggle.com/datasets/dgsports/ipl-ball-by-ball-2008-to-2022, which has updated its CSVs for data upto 2025.

This contains 2 CSV files:
- deliveries_updated_ipl_upto_2025.csv: Ball-by-ball information on IPL matches till 2025
- matches_updated_ipl_upto_2025.csv: General match summaries on IPL matches till 2025

### Bronze Layer
This layer will focus on just extracting the CSV information and loading it into Microsoft SQL Server. The schemas will and data content will remain unchanged.

### Silver Layer
This layer will focus on inspecting the data quality of the source file and resolving any data quality issues. We will also enrich the data and prepare the information to be used in the Gold Layer. This is why in the silver layer, I have chosen to split our two tables into the key tables we need for business insights. This allows us to complete complex calculations in this layer, improving the performance of the Gold layer.

The data quality tests will be a stored procedure, which will execute the test plan and ensure that the data has no quality issues in the gold layer.

### Gold Layer
This layer will focus on converting the tables in Silver layers into views. I am using views since this provides flexibility in adding and removing fields without having to change the schema of a table. This also provides future compatibility, allowing me to change the schemas in the silver layer without impacting the end user who will be interacting with the information in the gold layer.
