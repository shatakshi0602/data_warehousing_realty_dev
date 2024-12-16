Realty_Dev

Architecture Document for Data Warehouse Implementation

Introduction The purpose of this document is to outline the architecture of the Realty Dev Data Warehouse (DW). This document provides the flow of implementation, key decisions, and an overview of the data warehouse components.

System Overview The data warehouse solution is designed to support business decision-making processes. The system will support incremental and full data loads, business logic implementation for KPI derivation which can be used by the analytical team.

Architectural Goals Goals: Provide a scalable and high-performance data warehouse. Ensure data integrity, quality, and consistency. Support both incremental and full data loads. Enable self-service reporting and analytics for business users.

High-Level Architecture Diagram

Data Extraction Bucket Structure: Data will be extracted from OLTP . A structured bucket system (on AWS S3) will be used to organize incoming raw data into appropriate folders based on source type, date /date_time and frequency. Mechanism: Snowflake will be used for scheduling and managing the extraction of data from various sources. ETL tools may also be utilized for complex data pipelines.

Data Loading Staging Area: Data will be loaded into a Snowflake staging area before transformation and modeling. Loading Strategies: Incremental Load: Mechanisms such as SCD2 and SCD1 loads will be used to only load data changes since the last run. Full Load: Full loads will be performed for data sources where delta mechanisms are not applicable or for reinitialization or failure of ETL. ETL Tools: Use of tools like Snowflake’s native capabilities and custom-built Python-based ETL scripts.

Data Modeling and Design Data Modeling Approach: Dimensional modeling using star schema to represent facts and dimensions, ensuring efficient querying and reporting. Entities and Relationships: Fact Tables: Store quantitative data for business processes. Dimension Tables: Hold descriptive attributes about the facts (e.g., time, product, location).
