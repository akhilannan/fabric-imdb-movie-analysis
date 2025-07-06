# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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

# # Configure Compute

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "vCores": 4
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Import Libraries

# CELL ********************

import polars as pl
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
    urlretrieve(DATASET_URL + file, raw_file_path)
    table_name = "t_" + file.replace('.tsv.gz', '').replace(".", "_")

    schema = {
        "primaryTitle": pl.String,
        "averageRating": pl.Float64,
        "numVotes": pl.Int64,
        "startYear": pl.Int64,
        "endYear": pl.Int64,
        "deathYear": pl.Int64,
        "birthYear": pl.Int64,
        "runtimeMinutes": pl.Int64,
        "isAdult": pl.Int8,
    }

    df = pl.scan_csv(
        raw_file_path,
        separator="\t",
        null_values="\\N",
        infer_schema_length=0,
        has_header=True,
        new_columns=None,
        try_parse_dates=False,
        quote_char=None,
        low_memory=True,
    )

    column_names = df.collect_schema().names()

    df = df.with_columns(
        [
            pl.col(col_name).cast(data_type, strict=False)
            for col_name, data_type in schema.items()
            if col_name in column_names
        ]
    )

    create_or_replace_delta_table(
        df, bronze_schema, table_name, pl
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

title_lf = (
    read_delta_table(bronze_schema, "t_title_basics", pl)
    .join(
        read_delta_table(bronze_schema, "t_title_ratings", pl),
        on="tconst",
        how="left",
    )
    .select(
        [
            "tconst",
            pl.col("titleType").alias("title_type"),
            pl.col("primaryTitle").alias("primary_title"),
            pl.col("originalTitle").alias("original_title"),
            pl.col("startYear").alias("release_year"),
            pl.when(pl.col("isAdult") == 1)
            .then(pl.lit("Y"))
            .otherwise(pl.lit("N"))
            .alias("is_adult"),
            pl.col("runtimeMinutes").alias("runtime_min"),
            "genres",
            pl.col("averageRating").alias("avg_rating"),
            pl.col("numVotes").alias("num_votes"),
        ]
    )
)

create_or_replace_delta_table(
    title_lf, silver_schema, "t_title", pl
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB CREW table

# CELL ********************

unpivot_crew_lf = (
    read_delta_table(bronze_schema, "t_title_crew", pl)
    .unpivot(
        index=["tconst"],
        on=["directors", "writers"],
        variable_name="category",
        value_name="nconst",
    )
    .with_columns(
        [
            pl.col("category").str.replace_all("s$", ""),
            pl.col("nconst").str.split(","),
        ]
    )
    .explode("nconst")
)

comb_crew_lf = pl.concat(
    [
        read_delta_table(bronze_schema, "t_title_principals", pl)
        .filter(~pl.col("category").is_in(["director", "writer"]))
        .select(["tconst", "category", "nconst"]),
        unpivot_crew_lf.select(["tconst", "category", "nconst"]),
    ]
)

tnb_lf = (
    read_delta_table(bronze_schema, "t_name_basics", pl)
    .with_columns(pl.col("knownForTitles").str.split(",").alias("known_for_tconst"))
    .explode("known_for_tconst")
    .join(
        pl.scan_delta(delta_table_path(bronze_schema, "t_title_basics")),
        left_on="known_for_tconst",
        right_on="tconst",
        how="left",
    )
    .group_by(
        [
            "nconst",
            pl.col("primaryName").alias("name"),
            pl.col("birthYear").alias("birth_year"),
            pl.col("deathYear").alias("death_year"),
            pl.col("primaryProfession").alias("primary_profession"),
        ],
    )
    .agg(
        [
            (pl.col("primaryTitle") + " (" + pl.col("startYear").cast(pl.Utf8) + ")")
            .str.concat(", ")
            .alias("known_for_titles")
        ]
    )
)

crew_lf = (
    comb_crew_lf.join(tnb_lf, on="nconst", how="inner")
    .select(
        [
            "tconst",
            "category",
            "name",
            "birth_year",
            "death_year",
            "primary_profession",
            "known_for_titles",
        ]
    )
    .sort("name")
)

create_or_replace_delta_table(crew_lf, silver_schema, "t_crew", pl)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Extract data for IMDB URLs

# CELL ********************

PRIMARY_LANGUAGE_PATTERN = ".*primary_language.*"
POPULAR_MOVIE_PATTERN = ".*release_date.*moviemeter.*"

imdb_df = scrape_and_convert_data(url_list, scrape_movie_data_threaded, pl)
lang_df = scrape_and_convert_data(LANGUAGE_URL, extract_lang_data, pl)

top_title_lf = imdb_df.filter(pl.col("tconst").is_not_null()).join(
    lang_df,
    left_on=pl.when(pl.col("type").str.contains(PRIMARY_LANGUAGE_PATTERN))
    .then(pl.col("type").str.extract(r"primary_language=([a-z]+)", 1))
    .when(pl.col("type").str.contains("malayalam"))
    .then(pl.lit("ml"))
    .when(pl.col("type").str.contains("tamil"))
    .then(pl.lit("ta"))
    .when(pl.col("type").str.contains("telugu"))
    .then(pl.lit("te"))
    .otherwise(None),
    right_on="lang_code",
    how="left",
)

conditional_columns = {
    "is_top": pl.col("type") == "top",
    "is_toptv": pl.col("type") == "toptv",
    "is_top_indian_movies": pl.col("type") == "top-rated-indian-movies",
    "is_top_malayalam_movies": pl.col("type") == "top-rated-malayalam-movies",
    "is_top_tamil_movies": pl.col("type") == "top-rated-tamil-movies",
    "is_top_telugu_movies": pl.col("type") == "top-rated-telugu-movies",
    "is_popular": pl.col("type").str.contains(POPULAR_MOVIE_PATTERN),
    "is_primary_lang": pl.col("type").str.contains(PRIMARY_LANGUAGE_PATTERN),
    "is_asc": pl.col("type").str.contains(",asc"),
    "is_desc": pl.col("type").str.contains(",desc"),
    "is_in_top_1000": pl.col("type").str.contains("top_1000"),
}

top_title_lf = top_title_lf.with_columns(
    [
        *[
            pl.when(condition).then(pl.lit("Y")).otherwise(pl.lit("N")).alias(col_name)
            for col_name, condition in conditional_columns.items()
        ],
        (
            pl.col("rnk")
            + pl.when(pl.col("type").str.contains(POPULAR_MOVIE_PATTERN))
            .then(
                pl.col("type").str.extract(r"moviemeter=(\d+),", 1).cast(pl.Int64) - 1
            )
            .otherwise(0)
        ).alias("rnk")
    ]
)

top_title_lf = top_title_lf.with_columns(
    pl.when(
        (pl.col("is_top") == "Y") |
        (pl.col("is_toptv") == "Y") |
        (pl.col("is_top_indian_movies") == "Y") |
        (pl.col("is_top_malayalam_movies") == "Y") |
        (pl.col("is_top_tamil_movies") == "Y") |
        (pl.col("is_top_telugu_movies") == "Y") |
        (pl.col("is_in_top_1000") == "Y")
    ).then(pl.lit("Y")).otherwise(pl.lit("N")).alias("is_top_title")
)

top_title_lf = top_title_lf.group_by("tconst").agg([
    pl.col("lang_name").unique().str.concat(", ").alias("language_lst"),
    pl.col("rnk").filter(pl.col("is_top_title") == "Y").max().alias("top_title_rnk"),
    pl.col("rnk").filter(pl.col("is_popular") == "Y").max().alias("popularity_rnk"),
    pl.col("rnk").filter((pl.col("is_primary_lang") == "Y") & (pl.col("is_asc") == "Y")).max().alias("language_popularity_rnk"),
    pl.col("rnk").filter((pl.col("is_primary_lang") == "Y") & (pl.col("is_desc") == "Y")).max().alias("language_votes_rnk"),
    pl.col("rnk").filter(pl.col("is_top") == "Y").max().alias("top_250_movie_rnk"),
    pl.col("rnk").filter(pl.col("is_toptv") == "Y").max().alias("top_250_tv_rnk"),
    pl.col("rnk").filter(pl.col("is_top_indian_movies") == "Y").max().alias("top_indian_movie_rnk"),
    pl.col("rnk").filter(pl.col("is_top_malayalam_movies") == "Y").max().alias("top_malayalam_movie_rnk"),
    pl.col("rnk").filter(pl.col("is_top_tamil_movies") == "Y").max().alias("top_tamil_movie_rnk"),
    pl.col("rnk").filter(pl.col("is_top_telugu_movies") == "Y").max().alias("top_telugu_movie_rnk"),
    pl.col("is_in_top_1000").max().alias("is_top_1000_movie"),
    pl.col("is_top_title").max().alias("is_top_title")
])

create_or_replace_delta_table(
    top_title_lf, silver_schema, "t_top_title", pl
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
    yearly_box_office_url_list, scrape_movie_data_threaded, pl
)

bo_df_w_id = bo_df.filter(pl.col("tconst") != "NA")

bo_df_wo_id = (
    bo_df.filter(pl.col("tconst") == "NA")
    .drop("tconst")
    .join(
        read_delta_table(silver_schema, "t_title", pl).filter(
            pl.col("title_type") == "movie"
        ),
        left_on=["movie_name", "movie_year"],
        right_on=["primary_title", "release_year"],
        how="inner",
    )
    .join(bo_df_w_id, on="tconst", how="anti")
    .join(
        bo_df_w_id,
        left_on=["movie_name", "movie_year", "box_office"],
        right_on=["movie_name", "movie_year", "box_office"],
        how="anti",
    )
    .with_columns(
        pl.struct(["num_votes", "box_office"])
        .rank(method="ordinal", descending=True)
        .over(["movie_name", "movie_year"])
        .alias("rnk")
    )
    .filter(pl.col("rnk") == 1)
    .select(["tconst", "box_office"])
)

bo_df_final = pl.concat(
    [bo_df_w_id.select(["tconst", "box_office"]), bo_df_wo_id], how="vertical"
)

create_or_replace_delta_table(
    bo_df_final, silver_schema, "t_boxoffice", pl
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB Gold table

# CELL ********************

imdb_lf = (
    read_delta_table(silver_schema, "t_title", pl)
    .join(
        read_delta_table(silver_schema, "t_crew", pl),
        left_on="tconst",
        right_on="tconst",
        how="left",
    )
    .join(
        read_delta_table(silver_schema, "t_top_title", pl),
        left_on="tconst",
        right_on="tconst",
        how="left",
    )
    .join(
        read_delta_table(silver_schema, "t_boxoffice", pl),
        left_on="tconst",
        right_on="tconst",
        how="left",
    )
    .filter(
        pl.col("title_type").is_in(
            ["movie", "tvMiniSeries", "short", "tvSeries", "tvShort", "tvSpecial"]
        )
    )
    .group_by(
        [
            "tconst",
            "title_type",
            "primary_title",
            "original_title",
            "release_year",
            "is_adult",
            "runtime_min",
            "genres",
            "avg_rating",
            "num_votes",
            "top_title_rnk",
            "language_lst",
            "popularity_rnk",
            "language_popularity_rnk",
            "language_votes_rnk",
            "box_office",
            pl.coalesce(pl.col("top_250_movie_rnk"), pl.lit(9999)).alias(
                "top_250_movie_rnk"
            ),
            pl.coalesce(pl.col("top_250_tv_rnk"), pl.lit(9999)).alias("top_250_tv_rnk"),
            pl.coalesce(pl.col("top_indian_movie_rnk"), pl.lit(9999)).alias(
                "top_indian_movie_rnk"
            ),
            pl.coalesce(pl.col("top_malayalam_movie_rnk"), pl.lit(9999)).alias(
                "top_malayalam_movie_rnk"
            ),
            pl.coalesce(pl.col("top_tamil_movie_rnk"), pl.lit(9999)).alias(
                "top_tamil_movie_rnk"
            ),
            pl.coalesce(pl.col("top_telugu_movie_rnk"), pl.lit(9999)).alias(
                "top_telugu_movie_rnk"
            ),
            pl.coalesce(pl.col("is_top_1000_movie"), pl.lit("N")).alias(
                "is_top_1000_movie"
            ),
            pl.coalesce(pl.col("is_top_title"), pl.lit("N")).alias("is_top_title"),
        ]
    )
    .agg(
        [
            pl.when(pl.col("category") == "director")
            .then(pl.col("name"))
            .str.concat("; ")
            .alias("director_lst"),
            pl.when(pl.col("category") == "actor")
            .then(pl.col("name"))
            .str.concat("; ")
            .alias("actor_lst"),
            pl.when(pl.col("category") == "actress")
            .then(pl.col("name"))
            .str.concat("; ")
            .alias("actress_lst"),
            pl.lit(date.today()).alias("last_refresh_date"),
        ]
    )
)

positive_max = pl.lit(float("inf"))
negative_max = pl.lit(float("-inf"))
imdb_lf = imdb_lf.with_columns(
    [
        pl.when(pl.col("is_top_title") == "Y")
        .then(
            pl.struct(
                [
                    pl.col("top_250_movie_rnk"),
                    pl.col("top_250_tv_rnk"),
                    pl.col("top_indian_movie_rnk"),
                    pl.col("top_malayalam_movie_rnk"),
                    pl.col("top_tamil_movie_rnk"),
                    pl.col("top_telugu_movie_rnk"),
                    -pl.col("is_top_title").rank(),
                    -pl.col("box_office").fill_null(negative_max),
                    -pl.col("num_votes").fill_null(negative_max),
                    -pl.col("avg_rating").fill_null(negative_max),
                ]
            ).rank(method="ordinal", descending=False)
        )
        .otherwise(pl.lit(9999))
        .alias("top_title_rnk"),
        pl.struct(
            [
                pl.col("popularity_rnk").fill_null(positive_max),
                pl.col("language_popularity_rnk").fill_null(positive_max),
                pl.col("top_title_rnk").fill_null(positive_max),
                pl.col("language_votes_rnk").fill_null(positive_max),
                -pl.col("box_office").fill_null(negative_max),
                -pl.col("num_votes").fill_null(negative_max),
                -pl.col("avg_rating").fill_null(negative_max),
            ]
        )
        .rank(method="ordinal", descending=False)
        .alias("overall_popularity_rnk"),
    ]
).drop(["popularity_rnk", "language_popularity_rnk", "language_votes_rnk"])

create_or_replace_delta_table(imdb_lf, gold_schema, "t_imdb", pl)


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
