# Youtube_Project

YouTube Data Collection and Storage with SQL, MongoDB, and Streamlit

Introduction

This project, YouTube Data Collection and Storage, aims to create a platform enabling users to access and analyze data from various YouTube channels. Utilizing SQL, MongoDB, and Streamlit, it facilitates easy data retrieval, storage, and querying of YouTube channel and video data.

Project Overview

The YouTube Data Collection and Storage project comprises the following components:

Streamlit Application: A user-friendly interface built with the Streamlit library, allowing users to interact with the application and conduct data retrieval and analysis tasks.

YouTube API Integration: Integration with the YouTube API to fetch channel and video data based on provided channel IDs.

MongoDB Data Storage: Data storage in a MongoDB database, offering flexibility and scalability for handling unstructured and semi-structured data.

SQL Data Warehouse: Migration of data from the data lake to a SQL database, facilitating efficient querying and analysis using SQL.

Data Visualization: Presentation of retrieved data through Streamlit's data visualization features, enabling users to analyze data via charts and graphs.

Technologies Used

The project utilizes the following technologies:

Python: Programming language used for application development and scripting.
Streamlit: Python library for building interactive web applications and visualizations.
YouTube API: Google API for fetching channel and video data from YouTube.
MongoDB: NoSQL database used for storing retrieved YouTube data.
SQL (MySQL): Relational database for storing migrated YouTube data.
SQLAlchemy: Python library for SQL database connectivity and interaction.
Pandas: Data manipulation library for data processing and analysis.
Matplotlib: Data visualization library for creating charts and graphs.
Installation and Setup

To run the project, follow these steps:

Install Python on your machine.
Install required libraries such as Streamlit, MongoDB driver, SQLAlchemy, Pandas, and Matplotlib using pip or conda.
Set up a Google API project and obtain API credentials for accessing the YouTube API.
Configure MongoDB and SQL databases for data storage.
Update configuration files or environment variables with API credentials and database connection details.
Launch the Streamlit application via the command-line interface.
Usage

Once the project is set up, users can access the Streamlit application via a web browser to:

Enter a YouTube channel ID to retrieve data.
Store retrieved data in the MongoDB data lake.
Collect and store data for multiple YouTube channels.
Select a channel and migrate its data to the SQL data warehouse.
Search and retrieve data from the SQL database using various options.
Perform data analysis and visualization using provided features.
Features

The project offers the following features:

Retrieval of channel and video data from YouTube using the YouTube API.
Storage of data in MongoDB as a data lake.
Migration of data from the data lake to SQL for efficient querying.
Advanced search and retrieval options from the SQL database.
Data analysis and visualization through charts and graphs.
Support for managing multiple YouTube channels and their data.
Future Enhancements

Potential future enhancements include:

Implementing authentication and user management.
Setting up automated data harvesting for selected channels.
Enhancing search functionality with advanced criteria.
Supporting data retrieval from other social media platforms.
Incorporating advanced analytics and machine learning.
Adding features for data export and reporting.
Conclusion

The YouTube Data Collection and Storage project provides a robust solution for accessing, storing, and analyzing YouTube data. By leveraging SQL, MongoDB, and Streamlit, users can efficiently manage and visualize YouTube data, enabling valuable insights from the vast amount of available data.
