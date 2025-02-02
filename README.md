# IMDB Data Pipeline

## Overview
Data pipeline extracting and processing IMDB movie data using Spark/Polars/DuckDB in Microsoft Fabric. The pipeline culminates in a Power BI report that serves as a discovery tool for finding and evaluating popular movies and TV shows. It helps users make informed watching decisions based on various metrics and attributes.

## Technical Details

### Data Sources
- [IMDb Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/)
- Web scraping (IMDB, Box Office Mojo, Wikipedia)

### Data Model
- **Raw**: Raw TSV files from IMDB Datasets
  - title.principals.tsv.gz
  - name.basics.tsv.gz
  - title.basics.tsv.gz
  - title.crew.tsv.gz
  - title.ratings.tsv.gz

- **Bronze**: TSV Files converted to Delta tables with proper data types applied
  - t_title_principals
  - t_name_basics
  - t_title_basics
  - t_title_crew
  - t_title_ratings

- **Silver**: Cleansed, combined, and enhanced data
  - t_title: Basic title informations
  - t_crew: Detailed crew information (1:M relationship with titles)
  - t_top_title: Web-scraped IMDB data including popularity/rating ranks (1:1 with titles)
  - t_boxoffice: Box office revenue data (1:1 with titles)

- **Gold**: Single analytics-ready table for Power BI accessed via _Direct Lake_ mode
  - t_imdb: Consolidated view with one row per IMDB title
    - Flattened crew information (comma-separated lists)
    - Aggregated ranking information
    - Combined attributes from all silver tables

### Implementations
- Apache Spark
- Polars
- DuckDB

### Power BI Report
The final output is a Power BI report serving as a discovery tool for finding and evaluating popular movies and TV shows. It helps users make informed watching decisions based on various metrics and attributes:
- Popularity ranking based on IMDB data.
- Visual indicators highlighting titles on top-rated lists (e.g., IMDB Top 250), highly-rated titles, high box office revenue titles, and titles with many votes.

## Setup

## Prerequisites
- Microsoft Fabric workspace with Trial or PAYG Capacity

## Getting Started
- Connect to this GitHub repository (Workspace Settings -> Git Integration).

## Usage
1. Open one of the `Prepare IMDB Data` notebook (Spark/Polars/DuckDB) from the workspace.
2. Point the notebook to `lh_imdb` lakehouse.
3. Execute the Notebook.
4. Monitor data processing through the layers.
5. Access the Power BI report.