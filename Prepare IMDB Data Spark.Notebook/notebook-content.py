# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b33f982a-d05e-4877-9dfe-ea9c74f4d2ca",
# META       "default_lakehouse_name": "lh_imdb",
# META       "default_lakehouse_workspace_id": "615cf6ff-d2fe-4d59-99f8-81f581d8ae5d"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Import Libraries

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import requests
from bs4 import BeautifulSoup
from datetime import date
from urllib.request import urlretrieve
import re
import json
from concurrent.futures import ThreadPoolExecutor
from sempy import fabric
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Initialize Constants

# CELL ********************

DATASET_URL = "https://datasets.imdbws.com/"
BOXOFFICE_URL = "https://www.boxofficemojo.com"
LANGUAGE_URL = "https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes"
IMDB_URL = "https://www.imdb.com/"
CHART_URL = f"{IMDB_URL}chart/"
INDIA_URL = f"{IMDB_URL}india/"
IMDB_SEARCH_URL = f"{IMDB_URL}search/title/"
TOP_1000_URL = f"{IMDB_SEARCH_URL}?groups=top_1000"
LANG_URL = f"{IMDB_SEARCH_URL}?primary_language"
BOXOFFICE_CHART = f"{BOXOFFICE_URL}/chart/ww_top_lifetime_gross/?area=XWW&offset="
BOXOFFICE_YEAR = f"{BOXOFFICE_URL}/year/world/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Initialize Variables

# CELL ********************

workspace_id = fabric.get_notebook_workspace_id()
lakehouse_id = fabric.get_lakehouse_id()
abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
files_path = "/lakehouse/default/Files"
raw_path = f"{files_path}/raw/"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"
file_list = [
    "title.principals.tsv.gz",
    "name.basics.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.ratings.tsv.gz",
]
parallelism = os.cpu_count() * 2
os.makedirs(raw_path, exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Initialize IMDB URLs

# CELL ********************

url_list = [
    f"{CHART_URL}{suffix}" for suffix in ["toptv/", "top/"]
] + [
    f"{INDIA_URL}top-rated-{language}-movies/" for language in ["indian", "malayalam", "tamil", "telugu"]
]

top_search_languages = ["ml", "ta", "hi", "te", "kn"]
sort_options = ["moviemeter,asc", "num_votes,desc"]

url_list += [
    f"{LANG_URL}={lang}&sort={sort}"
    for lang in top_search_languages
    for sort in sort_options
]

date_ranges = [
    ("1900-01-01", "1940-12-31"), ("1941-01-01", "1950-12-31"), ("1951-01-01", "1958-12-31"),
    ("1959-01-01", "1963-12-31"), ("1964-01-01", "1970-12-31"), ("1971-01-01", "1975-12-31"),
    ("1976-01-01", "1982-12-31"), ("1983-01-01", "1987-12-31"), ("1988-01-01", "1991-12-31"),
    ("1992-01-01", "1994-12-31"), ("1995-01-01", "1997-12-31"), ("1998-01-01", "2000-12-31"),
    ("2001-01-01", "2002-12-31"), ("2003-01-01", "2004-09-30"), ("2004-10-01", "2006-12-31"),
    ("2007-01-01", "2008-12-31"), ("2009-01-01", "2010-12-31"), ("2011-01-01", "2012-12-31"),
    ("2013-01-01", "2014-06-30"), ("2014-07-01", "2015-12-31"), ("2016-01-01", "2017-12-31"),
    ("2018-01-01", "2019-12-31"), ("2020-01-01", "2022-12-31"), ("2023-01-01", "9999-12-31")
]

url_list += [
    f"{IMDB_SEARCH_URL}?release_date={start},{end}&groups=top_1000"
    for start, end in date_ranges
]

url_list += [
    f"{IMDB_SEARCH_URL}?release_date=,9999-12-31&moviemeter={n},{n+49}"
    for n in range(1, 2000, 50)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Initialize BoxOffice URLs

# CELL ********************

yearly_box_office_url_list = [BOXOFFICE_YEAR + str(n) for n in range(1977, date.today().year + 1)] + [f"{BOXOFFICE_CHART}{n}" for n in range(0, 10000, 200)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Function to get delta table path

# CELL ********************

def delta_table_path(schema_name, table_name):
  """
  Returns the ABFSS path for a Delta table.

  Args:
    schema_name: The schema (e.g., "silver", "gold") of the table.
    table_name: The name of the table.

  Returns:
    The full ABFSS path of the Delta table.
  """
  return f"{abfss_path}/Tables/{schema_name}/{table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Functions to scrap movie data

# CELL ********************

def get_soup(url):
    """Get BeautifulSoup object from URL."""
    response = requests.get(url, headers=HEADERS)
    return BeautifulSoup(response.text, "html.parser")


def parse_box_office_data(url):
    """Parse movie data from box office pages."""
    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            movie_data = []
            is_yearly = BOXOFFICE_YEAR in url

            soup = get_soup(url)
            movie_entries = soup.find_all("table")[0].find_all("tr")
            for entry in movie_entries:
                row_cols = entry.find_all("td")
                if not row_cols:
                    continue

                movie_dict = {
                    "tconst": "NA",
                    "rnk": -1,
                    "type": "NA",
                    "movie_name": row_cols[1].text,
                    "movie_year": -1,
                    "box_office": int(row_cols[2].text.replace("$", "").replace(",", "")),
                    "url": url
                }

                if is_yearly:
                    movie_dict.update({
                        "movie_year": int(url.split("/")[-1])
                    })
                else:
                    movie_dict.update({
                        "type": url.split("/")[-2],
                        "rnk": int(row_cols[0].text.replace(",", "")),
                        "tconst": re.search(r"(tt\d+)", row_cols[1].find("a")["href"]).group(1),
                        "movie_year": int(row_cols[7].text)
                    })

                movie_data.append(movie_dict)

            return movie_data

        except (IndexError, ValueError) as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Max retries reached for {url}. Skipping.")
                return []


def parse_imdb_json_data(url):
    """Parse movie data from JSON structure."""
    soup = get_soup(url)
    script_tag = soup.find("script", id="__NEXT_DATA__")
    if not script_tag:
        print("Error: Could not find the '__NEXT_DATA__' script tag.")
        return []

    data = json.loads(script_tag.string)
    movie_data = []
    page_props = data.get("props", {}).get("pageProps", {})
    
    edges = (
            page_props.get("pageData", {}).get("chartTitles", {}).get("edges", [])
            or page_props.get("cmsContext", {}).get("transformedPlacements", {}).get("center-3", {}).get("transformedArguments", {}).get("items", [])
            or page_props.get("searchResults", {}).get("titleResults", {}).get("titleListItems", [])
    )
    
    for rank, item in enumerate(edges, start=1):
        node = item.get("node", {}) or item.get("item", {})
        movie = {
            "tconst": node.get("id") or item.get("titleId") or "NA",
            "rnk": item.get("currentRank") or rank or -1,
            "type": url.split("/")[-1] if IMDB_SEARCH_URL in url else url.split("/")[-2],
            "movie_name": node.get("titleText", {}).get("text") or item.get("titleText"),
            "movie_year": node.get("releaseYear", {}).get("year") or item.get("releaseYear"),
            "box_office": -1,
            "url": url
        }
        movie_data.append(movie)
    
    return movie_data


def scrape_movie_data(url):
    """
    Scrapes movie data from the given URL and returns a list of movie dictionaries.
    """
    if BOXOFFICE_URL in url:
        return parse_box_office_data(url)
    return parse_imdb_json_data(url)


def scrape_movie_data_threaded(urls):
    results = []
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = [executor.submit(scrape_movie_data, url) for url in urls]
        for future in futures:
            try:
                result = future.result()
                results.extend(result)
            except Exception as e:
                print(f"Error during execution: {e}")
    return results


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Function to extract Language dimension

# CELL ********************

def extract_lang_data(url):
    """
    Extracts language names and codes from the specified Wikipedia ISO 639-1 codes page.

    Args:
        url: The URL of the Wikipedia page.

    Returns:
        A list of dictionaries, each containing 'lang_name' and 'lang_code'.
    """
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    table = soup.find("table", class_="wikitable")
    if not table:
        raise ValueError("Could not find the expected table on the page.")

    rows = table.find_all("tr")
    get_cell_text = lambda cell: cell.text.strip()
    headers = list(map(get_cell_text, rows[0].find_all(["th", "td"])))

    try:
        name_idx, code_idx = map(
            lambda x: headers.index(next(h for h in headers if x in h)),
            ("ISO Language Names", "Set 1"),
        )
    except StopIteration:
        raise ValueError("Required columns not found in the table.")

    fields = {"lang_name": name_idx, "lang_code": code_idx}

    data = [
        {field: get_cell_text(cells[idx]) for field, idx in fields.items()}
        for row in rows[2:]
        if (cells := row.find_all(["th", "td"]))
    ]

    return data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Engine Utilities

# CELL ********************

def detect_engine(engine):
    """
    Detects the type of engine based on its attributes.
    
    Args:
        engine: The engine object to detect
    Returns:
        String indicating the detected engine type ('spark', 'polars', 'pyarrow', or 'deltalake')
    """
    if hasattr(engine, "sparkContext"):
        return "spark"
    elif hasattr(engine, "scan_delta"):
        return "polars"
    elif hasattr(engine, "Table"):
        return "pyarrow"
    elif hasattr(engine, "write_deltalake"):
        return "deltalake"
    else:
        raise ValueError(
            "Invalid engine. Provide a SparkSession, polars, pyarrow, or deltalake.writer module."
        )

def delta_table_path(schema_name, table_name):
  """
  Returns the ABFSS path for a Delta table.

  Args:
    schema_name: The schema (e.g., "silver", "gold") of the table.
    table_name: The name of the table.

  Returns:
    The full ABFSS path of the Delta table.
  """
  return f"{abfss_path}/Tables/{schema_name}/{table_name}"


def create_or_replace_delta_table(df, schema, table, engine, duckdb_con=None):
    """
    Creates or replaces a Delta Lake table.

    Args:
        df: Input dataframe (pandas, polars, or spark)
        schema: Schema (e.g., "silver", "gold") of the table.
        table: Name of the table
        engine: Engine to use for writing.
                - If a SparkSession object is passed, it uses Spark to write.
                - If the polars module is passed, it uses Polars to write.
                - If deltalake.writer is passed, it uses deltalake to write
        duckdb_con: Optional DuckDB connection (DuckDBLakehouseConnector instance) to create schema/view
    """
    path = delta_table_path(schema, table)
    
    delta_write_options = {
        "schema_mode": "overwrite",
        "max_rows_per_group": 16_000_000,
        "min_rows_per_group": 8_000_000,
        "max_rows_per_file": 48_000_000,
    }
    
    engine_type = detect_engine(engine)
    mode = "overwrite"
    
    if engine_type == "spark":
        spark_options = {k: v for k, v in delta_write_options.items() if k == "schema_mode"}
        df.write.format("delta").options(**spark_options).mode(mode).save(path)
    elif engine_type == "polars":
        if isinstance(df, pl.LazyFrame):
            df = df.collect(streaming=True)
        df.write_delta(path, mode=mode, delta_write_options=delta_write_options)
    elif engine_type == "deltalake":
        engine.write_deltalake(path, df, mode=mode, **delta_write_options, engine='pyarrow')
    else:
        raise ValueError("Only Spark, Polars and DeltaLake engines are supported for writing delta tables")
    
    if duckdb_con is not None:
        duckdb_con.create_view(schema=schema, table=table)

def read_delta_table(schema, table, engine):
    """
    Reads a Delta table using either Spark or Polars based on the provided engine.

    Args:
        schema (str): The schema (e.g., database or folder) where the Delta table resides.
        table (str): The name of the Delta table.
        engine (Union[pyspark.sql.SparkSession, module]): Either a SparkSession object 
                                                          or the polars module itself 
                                                          to indicate the desired engine.

    Returns:
        pyspark.sql.DataFrame or polars.LazyFrame: The Delta table as a Spark DataFrame 
                                                  or a Polars LazyFrame, depending on 
                                                  the `engine` parameter.
    """
    delta_path = delta_table_path(schema, table)
    engine_type = detect_engine(engine)
    
    if engine_type == "polars":
        return engine.scan_delta(delta_path)
    elif engine_type == "spark":
        return engine.read.format("delta").load(delta_path)
    else:
        raise ValueError("Only Spark and Polars engines are supported for reading delta tables")

def scrape_and_convert_data(urls, data_extractor, engine):
    """
    Scrapes data from a list of URLs, extracts it using a provided function,
    and converts it to a PyArrow Table, Polars DataFrame, or Spark DataFrame.
    """
    scraped_data = data_extractor(urls)
    engine_type = detect_engine(engine)
    
    if engine_type == "spark":
        return engine.createDataFrame(scraped_data)
    elif engine_type == "pyarrow":
        if not scraped_data:
            return engine.Table.from_pydict({})
        column_names = scraped_data[0].keys()
        data_dict = {
            col: [entry.get(col) for entry in scraped_data] for col in column_names
        }
        return engine.Table.from_pydict(data_dict)
    elif engine_type == "polars":
        return engine.DataFrame(scraped_data).lazy()
    else:
        raise ValueError("Only Spark, Polars and PyArrow engines are supported")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Function for parallel table load

# CELL ********************

def parallel_load_tables(file_list, parallelism, load_table_func):
  """Loads multiple tables in parallel using a ThreadPoolExecutor.

  Args:
    file_list: A list of file paths representing the tables to load.
    parallelism: The maximum number of threads to use for parallel processing.
    load_table_func: A function that takes a file path as input and loads the 
                     corresponding table.
  """
  with ThreadPoolExecutor(max_workers=parallelism) as executor:
    executor.map(load_table_func, file_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Load IMDB FULL data

# CELL ********************

def load_table(file):
    raw_file_path = raw_path + file
    spark_file_path = raw_file_path.replace('/lakehouse/default/','')
    urlretrieve(DATASET_URL + file, raw_file_path)
    table_name = "t_" + file.replace('.tsv.gz', '').replace(".", "_")

    schema = {
        "primaryTitle": T.StringType(),
        "averageRating": T.FloatType(),
        "numVotes": T.IntegerType(),
        "startYear": T.IntegerType(),
        "endYear": T.IntegerType(),
        "deathYear": T.IntegerType(),
        "birthYear": T.IntegerType(),
        "runtimeMinutes": T.IntegerType(),
        "isAdult": T.IntegerType(),
    }

    df = (
        spark.read.format("csv")
        .option("inferSchema", "false")
        .option("header", "true")
        .option("delimiter", "\t")
        .option("nullValue", "\\N")
        .load(spark_file_path)
    )

    column_names = df.columns

    df = df.withColumns(
        {
            col_name: F.col(col_name).cast(data_type)
            for col_name, data_type in schema.items()
            if col_name in column_names
        }
    )

    create_or_replace_delta_table(
        df, bronze_schema, table_name, spark
    )


parallel_load_tables(file_list, parallelism, load_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB Title table

# CELL ********************

title_df = (
    read_delta_table(bronze_schema, "t_title_basics", spark)
    .alias("tbs")
    .join(
        read_delta_table(bronze_schema, "t_title_ratings", spark).alias("trt"),
        on="tconst",
        how="left_outer",
    )
    .select(
        "tbs.tconst",
        F.col("tbs.titletype").alias("title_type"),
        F.col("tbs.primarytitle").alias("primary_title"),
        F.col("tbs.originaltitle").alias("original_title"),
        F.col("tbs.startyear").alias("release_year"),
        F.when(F.col("tbs.isadult") == 1, "Y").otherwise("N").alias("is_adult"),
        F.col("tbs.runtimeminutes").alias("runtime_min"),
        "tbs.genres",
        F.col("trt.averagerating").alias("avg_rating"),
        F.col("trt.numvotes").alias("num_votes"),
    )
)

create_or_replace_delta_table(title_df, silver_schema, "t_title", spark)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Extract data for IMDB URLs

# CELL ********************

PRIMARY_LANGUAGE_PATTERN = "%primary_language%,%"
POPULAR_MOVIE_PATTERN = "%release_date%moviemeter%"

imdb_df = scrape_and_convert_data(url_list, scrape_movie_data_threaded, spark)
lang_df = scrape_and_convert_data(LANGUAGE_URL, extract_lang_data, spark)

top_title_df = (
    imdb_df.alias("imd")
    .filter(F.col("imd.tconst").isNotNull())
    .join(
        lang_df.alias("lng"),
        on=F.when(
            F.col("imd.type").like(PRIMARY_LANGUAGE_PATTERN),
            F.regexp_extract(F.col("imd.type"), r'primary_language=([a-z]+)', 1)
        ).when(
            F.col("imd.type").like("%malayalam%"), F.lit("ml")
        ).when(
            F.col("imd.type").like("%tamil%"), F.lit("ta")
        ).when(
            F.col("imd.type").like("%telugu%"), F.lit("te")
        ).otherwise(None) == F.col("lng.lang_code"),
        how="left",
    )
)

conditional_columns = {
    "is_top": F.col("imd.type") == "top",
    "is_toptv": F.col("imd.type") == "toptv",
    "is_top_indian_movies": F.col("imd.type") == "top-rated-indian-movies",
    "is_top_malayalam_movies": F.col("imd.type") == "top-rated-malayalam-movies",
    "is_top_tamil_movies": F.col("imd.type") == "top-rated-tamil-movies",
    "is_top_telugu_movies": F.col("imd.type") == "top-rated-telugu-movies",
    "is_in_top_1000": F.col("imd.type").like("%top_1000%"),
    "is_popular": F.col("imd.type").like(POPULAR_MOVIE_PATTERN),
    "is_primary_lang": F.col("imd.type").like(PRIMARY_LANGUAGE_PATTERN),
    "is_asc": F.col("imd.type").like("%,asc%"),
    "is_desc": F.col("imd.type").like("%,desc%"),
}

top_title_df = top_title_df.select(
    F.col("imd.tconst"),
    F.col("lng.lang_name"),
    (
        F.col("imd.rnk")
        + F.when(
            F.col("imd.type").like(POPULAR_MOVIE_PATTERN),
            F.regexp_extract("imd.type", r"moviemeter=(\d+),", 1).cast("int") - 1,
        ).otherwise(0)
    ).alias("rnk"),
    F.col("imd.type").alias("url_type"),
    *[
        F.when(condition, F.lit("Y")).otherwise(F.lit("N")).alias(col_name)
        for col_name, condition in conditional_columns.items()
    ],
)

top_title_df = top_title_df.withColumn(
    "is_top_title",
    F.when(
        (F.col("is_top") == "Y") | 
        (F.col("is_toptv") == "Y") | 
        (F.col("is_top_indian_movies") == "Y") | 
        (F.col("is_top_malayalam_movies") == "Y") | 
        (F.col("is_top_tamil_movies") == "Y") | 
        (F.col("is_top_telugu_movies") == "Y") | 
        (F.col("is_in_top_1000") == "Y"),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

top_title_df = top_title_df.groupBy("tconst").agg(
    F.concat_ws("; ", F.collect_set("lang_name")).alias("language_lst"),
    F.max(F.when(F.col("is_top_title") == "Y", F.col("rnk"))).alias("top_title_rnk"),
    F.max(F.when(F.col("is_popular") == "Y", F.col("rnk"))).alias("popularity_rnk"),
    F.max(F.when((F.col("is_primary_lang") == "Y") & (F.col("is_asc") == "Y"), F.col("rnk"))).alias("language_popularity_rnk"),
    F.max(F.when((F.col("is_primary_lang") == "Y") & (F.col("is_desc") == "Y"), F.col("rnk"))).alias("language_votes_rnk"),
    F.max(F.when(F.col("is_top") == "Y", F.col("rnk"))).alias("top_250_movie_rnk"),
    F.max(F.when(F.col("is_toptv") == "Y", F.col("rnk"))).alias("top_250_tv_rnk"),
    F.max(F.when(F.col("is_top_indian_movies") == "Y", F.col("rnk"))).alias("top_indian_movie_rnk"),
    F.max(F.when(F.col("is_top_malayalam_movies") == "Y", F.col("rnk"))).alias("top_malayalam_movie_rnk"),
    F.max(F.when(F.col("is_top_tamil_movies") == "Y", F.col("rnk"))).alias("top_tamil_movie_rnk"),
    F.max(F.when(F.col("is_top_telugu_movies") == "Y", F.col("rnk"))).alias("top_telugu_movie_rnk"),
    F.max(F.col("is_in_top_1000")).alias("is_top_1000_movie"),
    F.max(F.col("is_top_title")).alias("is_top_title")
)

create_or_replace_delta_table(
    top_title_df, silver_schema, "t_top_title", spark
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Extract Box Office data

# CELL ********************

bo_df = scrape_and_convert_data(
    yearly_box_office_url_list, scrape_movie_data_threaded, spark
)

bo_df_w_id = bo_df.filter(F.col("tconst") != "NA")

bo_df_wo_id = (
    bo_df.filter(F.col("tconst") == "NA").alias("bo")
    .join(
        read_delta_table(silver_schema, "t_title", spark).alias("ttl"),
        (F.col("ttl.primary_title") == F.col("bo.movie_name"))
        & (F.col("ttl.release_year") == F.col("bo.movie_year"))
        & (F.col("ttl.title_type") == "movie"),
        "inner",
    )
    .join(
        bo_df_w_id.alias("bo1"), F.col("bo1.tconst") == F.col("ttl.tconst"), "left_anti"
    )
    .join(
        bo_df_w_id.alias("bo2"),
        (F.col("bo2.movie_name") == F.col("bo.movie_name"))
        & (F.col("bo2.movie_year") == F.col("bo.movie_year"))
        & (F.col("bo2.box_office") == F.col("bo.box_office")),
        "left_anti",
    )
    .withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("bo.movie_name", "bo.movie_year").orderBy(
                F.col("ttl.num_votes").desc(), F.col("bo.box_office").desc()
            )
        ),
    )
    .filter(F.col("row_num") == 1)
    .select("ttl.tconst", "bo.box_office")
)

bo_df_final = (
    bo_df_w_id.select("tconst", "box_office")
    .unionByName(bo_df_wo_id)
)

create_or_replace_delta_table(
    bo_df_final, silver_schema, "t_boxoffice", spark
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB CREW table

# CELL ********************

unpivot_crew_df = (
    read_delta_table(bronze_schema, "t_title_crew", spark)
    .select(
        "tconst",
        F.expr(
            "stack(2, 'director', directors, 'writer', writers) as (category, nconst)"
        ),
    )
    .withColumn("nconst", F.explode(F.split("nconst", ",")))
)

comb_crew_df = (
    read_delta_table(bronze_schema, "t_title_principals", spark)
    .filter(~F.col("category").isin(["director", "writer"]))
    .select("tconst", "category", "nconst")
    .union(unpivot_crew_df.select("tconst", "category", "nconst"))
)

tnb_df = (
    read_delta_table(bronze_schema, "t_name_basics", spark)
    .withColumn("known_for_tconst", F.explode(F.split("knownForTitles", ",")))
    .join(
        read_delta_table(bronze_schema, "t_title_basics", spark).alias("tbs"),
        F.col("tbs.tconst") == F.col("known_for_tconst"),
        "left",
    )
    .groupBy(
        "nconst",
        F.col("primaryName").alias("name"),
        F.col("birthYear").alias("birth_year"),
        F.col("deathYear").alias("death_year"),
        F.col("primaryProfession").alias("primary_profession"),
    )
    .agg(
        F.concat_ws(
            ", ",
            F.collect_set(
                F.concat(
                    F.col("tbs.primaryTitle"),
                    F.lit("("),
                    F.col("tbs.startYear"),
                    F.lit(")"),
                )
            ),
        ).alias("known_for_titles")
    )
)

crew_df = (
    comb_crew_df.alias("crw")
    .join(tnb_df.alias("tnb"), F.col("tnb.nconst") == F.col("crw.nconst"), "inner")
    .select(
        F.col("crw.tconst"),
        F.col("crw.category"),
        F.col("tnb.name"),
        F.col("tnb.birth_year"),
        F.col("tnb.death_year"),
        F.col("tnb.primary_profession"),
        F.col("tnb.known_for_titles"),
    )
    .orderBy("name") # done to reduce parquet file size
)

create_or_replace_delta_table(crew_df, silver_schema, "t_crew", spark)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB Gold table

# CELL ********************

imdb_df = (
    read_delta_table(silver_schema, "t_title", spark).alias("ttl")
    .join(
        read_delta_table(silver_schema, "t_crew", spark).alias("tcr"),
        F.col("tcr.tconst") == F.col("ttl.tconst"),
        "left_outer",
    )
    .join(
        read_delta_table(silver_schema, "t_top_title", spark).alias("top"),
        F.col("top.tconst") == F.col("ttl.tconst"),
        "left_outer",
    )
    .join(
        read_delta_table(silver_schema, "t_boxoffice", spark).alias("tbo"),
        F.col("tbo.tconst") == F.col("ttl.tconst"),
        "left_outer",
    )
    .filter(
        F.col("ttl.title_type").isin(
            "movie", "tvMiniSeries", "short", "tvSeries", "tvShort", "tvSpecial"
        )
    )
    .groupBy(
        "ttl.tconst",
        "ttl.title_type",
        "ttl.primary_title",
        "ttl.original_title",
        "ttl.release_year",
        "ttl.is_adult",
        "ttl.runtime_min",
        "ttl.genres",
        "ttl.avg_rating",
        "ttl.num_votes",
        "top.top_title_rnk",
        F.coalesce(F.col("top.top_250_movie_rnk"), F.lit(9999)).alias(
            "top_250_movie_rnk"
        ),
        F.coalesce(F.col("top.top_250_tv_rnk"), F.lit(9999)).alias("top_250_tv_rnk"),
        F.coalesce(F.col("top.top_indian_movie_rnk"), F.lit(9999)).alias(
            "top_indian_movie_rnk"
        ),
        F.coalesce(F.col("top.top_malayalam_movie_rnk"), F.lit(9999)).alias(
            "top_malayalam_movie_rnk"
        ),
        F.coalesce(F.col("top.top_tamil_movie_rnk"), F.lit(9999)).alias(
            "top_tamil_movie_rnk"
        ),
        F.coalesce(F.col("top.top_telugu_movie_rnk"), F.lit(9999)).alias(
            "top_telugu_movie_rnk"
        ),
        F.coalesce(F.col("top.is_top_1000_movie"), F.lit("N")).alias(
            "is_top_1000_movie"
        ),
        F.coalesce(F.col("top.is_top_title"), F.lit("N")).alias("is_top_title"),
        "top.language_lst",
        "top.popularity_rnk",
        "top.language_popularity_rnk",
        "top.language_votes_rnk",
        "tbo.box_office"
    )
    .agg(
        F.concat_ws(
            "; ",
            F.collect_list(
                F.when(
                    F.col("tcr.category") == "director", F.col("tcr.name")
                )
            ),
        ).alias("director_lst"),
        F.concat_ws(
            "; ",
            F.collect_list(
                F.when(F.col("tcr.category") == "actor", F.col("tcr.name"))
            ),
        ).alias("actor_lst"),
        F.concat_ws(
            "; ",
            F.collect_list(
                F.when(F.col("tcr.category") == "actress", F.col("tcr.name"))
            ),
        ).alias("actress_lst"),
        F.current_date().alias("last_refresh_date"),
    )
)

imdb_df = imdb_df.withColumns(
    {
        "top_title_rnk": F.when(
            F.col("is_top_title") == "Y",
            F.row_number().over(
                Window.orderBy(
                    F.col("top_250_movie_rnk"),
                    F.col("top_250_tv_rnk"),
                    F.col("top_indian_movie_rnk"),
                    F.col("top_malayalam_movie_rnk"),
                    F.col("top_tamil_movie_rnk"),
                    F.col("top_telugu_movie_rnk"),
                    F.col("is_top_title").desc(),
                    F.col("box_office").desc(),
                    F.col("num_votes").desc(),
                    F.col("avg_rating").desc(),
                )
            ),
        ).otherwise(F.lit(9999)),
        "overall_popularity_rnk": F.row_number().over(
            Window.orderBy(
                F.col("popularity_rnk").asc_nulls_last(),
                F.col("language_popularity_rnk").asc_nulls_last(),
                F.col("top_title_rnk").asc_nulls_last(),
                F.col("language_votes_rnk").asc_nulls_last(),
                F.col("box_office").desc(),
                F.col("num_votes").desc(),
                F.col("avg_rating").desc(),
            )
        ),
    }
).drop("popularity_rnk", "language_popularity_rnk", "language_votes_rnk")

create_or_replace_delta_table(imdb_df, gold_schema, "t_imdb", spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Refresh Semantic Model

# CELL ********************

fabric.refresh_dataset("IMDB Model", refresh_type = "full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
