# Data Swan Inc: Medicaid and Medicare evaluation of sponsorship and ratings
Correlation One - DS4A Data Engineering
Learning Group 23

## Introduction
Our team chose this topic because we are interested in understanding how COVID impacted trends across the healthcare industry and whether this shift has any impact on the quality of care delivered by physicians.

## Data Sources
The following are the data sources that we have procured  to build our data warehouse and dashboard:
- 2020 and 2021 Transaction/Payments cleaned data for:
    - Research Payments (csv file)
    - Ownership Payments (csv file)
    - General Payments (csv file)
- Physician Reviews Data coming mainly from: RateMD (MongoDB access provided by TA)
- Physician Profiles - additional information regarding the provider of care (csv file)
- MIPs score file - Merit-Based Incentive Payment System (MIPS) Final Scores (csv file)

## Data Model
DB Diagram [link] (https://dbdiagram.io/d/64a9720002bd1c4a5eb8d9c4)
The fact tables in the schema are:
- fact_ratings: This table contains information about ratings given by customers to doctors.
- fact_ownership_payment: This table captures ownership payment details. 
- fact_research_payment: This table stores information about research payments.
- fact_general_payment: This table represents general payments made.
- fact_mips: This table contains MIPs (Merit-based Incentive Payment System) scores.

These tables follow a star schema design, where the fact tables (e.g., fact_ratings, fact_ownership_payment etc.) contain the primary business metrics and are connected to the dimension tables (e.g., dim_physician_recipient, dim_op_manufacturer_gpo, etc.) through foreign key references. The dimension tables provide descriptive attributes and context to analyze the facts. This design facilitates efficient querying and reporting on the data.

## ETL Pipeline

#### Data Extraction
Based on the data source, we utilized different methods for data extraction:
- Airflow DAGs orchestration (for Research and Ownership payments data, as well as Physician profile data)
- Python script execution (for larger data files such as General Payments data, as well as Ratings Data extracted from MongoDB)

Because the data sources we are ingesting are historical data points, we will not need a task schedule for orchestration. Rather, we can trigger an initialization task and all of the downstream tasks will run based on the dependencies/completion of tasks upstream

#### Data Transform
All the three payment related files and the MIPS score file were extracted into separate Python files and converted into Pandas data frames for further analysis and transformations in order to convert them to fact and dimension tables for loading onto the database.
- format
- consistency
- relevance
- data augmentation
Through the above-mentioned process, we created fact and dimension tables as proposed in our data model, extracted these as csv files and loaded them onto S3 as transformed files.

#### Data Load Process
Based on the data source, we utilized different methods for data load process:
- Airflow DAGs orchestration (for Research and Ownership payments data, as well as Physician profile data, and ratings)
- Python script execution (for General Payments data only)
Because the data sources we are loading are historical data points, we will not need a task schedule for orchestration since we are not expecting changes in the data. The process results in fact and dimensional tables created in PostgeSQL database hosted through AWS.

#### Conclusion and Future Work
What went well?
- We learned about different AWS services (S3, EC2, Glue)  in order to carry out our project.
- Different toolkits and methodologies (airflow, star/snowflake schema types, dimensional modeling, MongoDB)
- We were able to identify which types of data to procure for this project.

What would you do differently next?
- AWS credits and permission issues in order to more evenly distribute the workload, getting logistics sorted out earlier
- Challenges with accessing Ratings data which slowed our initial progress on obtaining the right data points
- Explore other ways of optimizing data storage, or enhance the ETL process:
    - Some of our scripts are not incorporated into the ETL as DAGs because of file size/memory issues
    - Optimize storage with different data file types for example parquet vs. csv
