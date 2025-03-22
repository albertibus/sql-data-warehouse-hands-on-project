# Data Warehouse Project

## Project Overview 📖 
This project involves:
1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.

## Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using PostgresSQL to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.


## Data Architecture 🏗️ 
The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

## License 🛡️ 

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## About Me 🙋‍♂️
I'm Alberto, a passionate software developer with an insatiable thirst for knowledge and a knack for discovering innovative solutions. 
What truly sets me apart is my unyielding desire to learn and explore. I am constantly seeking opportunities to broaden my horizons, be it through attending industry conferences, participating in online courses, or engaging in collaborative projects with talented professionals. The ever-changing landscape of technology inspires me, and I eagerly embrace it with open arms. 
Feel free to reach out to me to discuss potential collaborations, career opportunities, or simply to connect and share ideas. Together, let's embark on a journey of continuous learning and discovery! 

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/alberto-pillado-garcía-01a9821a7/)

 

