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

# # Upgrade Libraries

# CELL ********************

!pip install duckdb deltalake --upgrade -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Import Libraries

# CELL ********************

import duckdb
from deltalake.writer import write_deltalake
import pyarrow as pa
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
bronze_path = f"{files_path}/bronze/"
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
os.makedirs(bronze_path, exist_ok=True)

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

# # Function to Scrape and Convert data

# CELL ********************

def scrape_and_convert_data(urls, data_extractor, engine="pyarrow", spark=None):
    """
    Scrapes data from a list of URLs, extracts it using a provided function,
    and converts it to a PyArrow Table, Polars DataFrame, or Spark DataFrame.
    """
    scraped_data = data_extractor(urls)

    if engine == "spark":
        if spark is None:
            raise ValueError("SparkSession is required for 'spark' engine.")
        return spark.createDataFrame(scraped_data)
    elif engine == "pyarrow":
        if not scraped_data:
            return pa.Table.from_pydict({})
        column_names = scraped_data[0].keys()
        data_dict = {col: [entry.get(col) for entry in scraped_data] for col in column_names}
        return pa.Table.from_pydict(data_dict)
    elif engine == "polars":
        return pl.DataFrame(scraped_data).lazy()
    else:
        raise ValueError(f"Invalid engine: {engine}. Choose 'pyarrow', 'polars', or 'spark'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Function to create or replace delta table

# CELL ********************

def create_or_replace_delta_table(df, path: str, engine: str = "pyarrow") -> None:
    """
    Creates or replaces a Delta Lake table.
    Supports "pyarrow", "polars", and "spark" engines.
    """
    delta_write_options = {
        "schema_mode": "overwrite",
        "max_rows_per_group": 16_000_000,
        "min_rows_per_group": 8_000_000,
        "max_rows_per_file": 48_000_000,
    }

    mode = "overwrite"

    if engine == "pyarrow":
        write_deltalake(path, df, mode=mode, **delta_write_options, engine='pyarrow')
    elif engine == "polars":
        if isinstance(df, pl.LazyFrame):
            df = df.collect(streaming=True)
        df.write_delta(path, mode=mode, delta_write_options=delta_write_options)
    elif engine == "spark":
        spark_delta_options = {
            k: v
            for k, v in delta_write_options.items()
            if k == "schema_mode"
        }

        df.write.format("delta").options(**spark_delta_options).mode(mode).save(path)
    else:
        raise ValueError(f"Invalid engine: {engine}. Choose 'pyarrow', 'polars', or 'spark'.")

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

# # Set DuckDB Secret

# CELL ********************

duckdb.sql(f"""
    CREATE OR REPLACE SECRET onelake (
        TYPE AZURE,
        PROVIDER ACCESS_TOKEN,
        ACCESS_TOKEN '{notebookutils.credentials.getToken('storage')}'
    )
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Load IMDB FULL data

# CELL ********************

def load_table(file):
    con = duckdb.connect()

    bronze_file_path = bronze_path + file
    urlretrieve(DATASET_URL + file, bronze_file_path)
    table_name = "t_" + file.replace('.tsv.gz', '').replace(".", "_")

    schema = {
        "primaryTitle": "VARCHAR",
        "averageRating": "DOUBLE",
        "numVotes": "BIGINT",
        "startYear": "BIGINT",
        "endYear": "BIGINT",
        "deathYear": "BIGINT",
        "birthYear": "BIGINT",
        "runtimeMinutes": "BIGINT",
        "isAdult": "TINYINT",
    }

    detected_columns = con.sql(f"SELECT Columns FROM sniff_csv('{bronze_file_path}', sample_size = 1)").fetchone()[0]
    cast_statements = [
        f"CAST({col_info['name']} AS {schema[col_info['name']]}) AS {col_info['name']}"
        if col_info['name'] in schema
        else col_info['name']
        for col_info in detected_columns
    ]

    create_or_replace_delta_table(
        con.sql(f"""
            SELECT {','.join(cast_statements)}
            FROM read_csv('{bronze_file_path}',
                             header=true,
                             delim='\\t',
                             quote='',
                             nullstr='\\N',
                             ignore_errors=false)
        """).record_batch(),
        delta_table_path(silver_schema, table_name)
    )

    con.close()

parallel_load_tables(file_list, parallelism, load_table)

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

imdb_df = scrape_and_convert_data(url_list, scrape_movie_data_threaded)
lang_df = scrape_and_convert_data(LANGUAGE_URL, extract_lang_data)

create_or_replace_delta_table(
    duckdb.sql(f"""
        SELECT
            imd.tconst,
            lng.lang_name,
            (imd.rnk + 
                CASE 
                    WHEN imd.type LIKE '{POPULAR_MOVIE_PATTERN}' 
                    THEN CAST(regexp_extract(imd.type, 'moviemeter=(\\d+),', 1) AS INTEGER) - 1 
                    ELSE 0 
                END
            ) AS rnk,
            imd.type AS url_type,
            CASE 
                WHEN imd.type IN (
                    'toptv', 'top', 'top-rated-indian-movies',
                    'top-rated-malayalam-movies', 'top-rated-tamil-movies', 'top-rated-telugu-movies'
                ) THEN 'Y' 
                ELSE 'N' 
            END AS is_in_top_250,
            CASE 
                WHEN imd.type LIKE '%top_1000%' THEN 'Y' 
                ELSE 'N' 
            END AS is_in_top_1000,
            CASE 
                WHEN imd.type LIKE '{POPULAR_MOVIE_PATTERN}' THEN 'Y' 
                ELSE 'N' 
            END AS is_popular,
            CASE 
                WHEN imd.type LIKE '{PRIMARY_LANGUAGE_PATTERN}' THEN 'Y' 
                ELSE 'N' 
            END AS is_primary_lang,
            CASE 
                WHEN imd.type LIKE '%,asc%' THEN 'Y' 
                ELSE 'N' 
            END AS is_asc,
            CASE 
                WHEN imd.type LIKE '%,desc%' THEN 'Y' 
                ELSE 'N' 
            END AS is_desc
        FROM imdb_df imd
        LEFT JOIN lang_df lng
            ON CASE
                WHEN imd.type LIKE '{PRIMARY_LANGUAGE_PATTERN}'
                THEN regexp_extract(imd.type, 'primary_language=([a-z]+)', 1)
                ELSE NULL
            END = lng.lang_code
    """).record_batch(), 
    delta_table_path(silver_schema, "t_imdb_top")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Extract Box Office data

# CELL ********************

bo_df = scrape_and_convert_data(yearly_box_office_url_list, scrape_movie_data_threaded)

create_or_replace_delta_table(
    duckdb.sql(f"""
        SELECT
            tconst,
            box_office AS box_office_usd
        FROM
            bo_df
        WHERE
            tconst <> 'NA'
        UNION ALL
        SELECT
            tbs.tconst,
            tbo.box_office
        FROM
            bo_df AS tbo
        INNER JOIN
            delta_scan('{delta_table_path(silver_schema, "t_title_basics")}') AS tbs
            ON tbs.primaryTitle = tbo.movie_name
            AND tbs.startYear = tbo.movie_year
            AND tbs.titleType = 'movie'
        INNER JOIN
            delta_scan('{delta_table_path(silver_schema, "t_title_ratings")}') AS trt
            ON trt.tconst = tbs.tconst
        WHERE
            tbo.tconst = 'NA'
            AND NOT EXISTS (
                SELECT
                    0
                FROM
                    bo_df bo
                WHERE
                    bo.tconst = tbs.tconst
            )
            AND NOT EXISTS (
                SELECT
                    0
                FROM
                    bo_df boi
                WHERE
                    boi.tconst <> 'NA'
                    AND boi.movie_name = tbo.movie_name
                    AND boi.movie_year = tbo.movie_year 
                    AND boi.box_office = tbo.box_office
            )
        QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    tbo.movie_name,
                    tbo.movie_year
                ORDER BY
                    trt.numvotes DESC,
                    tbo.box_office DESC
            ) = 1
    """
    ).record_batch(),
    delta_table_path(silver_schema, "t_bo"),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB Gold table

# CELL ********************

create_or_replace_delta_table(
    duckdb.sql(f"""
        WITH crew_info AS (
            SELECT
                tconst,
                category,
                nconst
            FROM delta_scan('{delta_table_path(silver_schema, "t_title_principals")}')
            WHERE category IN ('actor', 'actress')
            UNION ALL
            SELECT
                tconst,
                'director' AS category,
                UNNEST(string_to_array(directors, ',')) AS nconst
            FROM delta_scan('{delta_table_path(silver_schema, "t_title_crew")}')
            WHERE directors IS NOT NULL
        )
        SELECT
            tbs.tconst,
            MAX(tbs.titletype) AS title_type,
            MAX(tbs.primarytitle) AS primary_title,
            MAX(tbs.originaltitle) AS original_title,
            MAX(tbs.startyear) AS release_year,
            MAX(CASE WHEN tbs.isadult = 1 THEN 'Y' ELSE 'N' END) AS is_adult,
            MAX(tbs.runtimeminutes) AS runtime_min,
            MAX(tbs.genres) AS genres,
            MAX(trt.averagerating) AS avg_rating,
            MAX(trt.numvotes) AS num_votes,
            MAX(tbo.box_office_usd) AS box_office,
            MAX(CASE WHEN imd.is_in_top_250 = 'Y' THEN imd.rnk END) AS top_250_rnk,
            MAX(CASE WHEN imd.is_in_top_1000 = 'Y' THEN imd.rnk END) AS top_1000_rnk,
            MAX(CASE WHEN imd.is_popular = 'Y' THEN imd.rnk END) AS popularity_rnk,
            MAX(CASE WHEN imd.is_primary_lang = 'Y' AND imd.is_asc = 'Y' THEN imd.rnk END) AS language_popularity_rnk,
            MAX(CASE WHEN imd.is_primary_lang = 'Y' AND imd.is_desc = 'Y' THEN imd.rnk END) AS language_votes_rnk,
            COALESCE(MAX(imd.is_in_top_1000), 'N') AS is_top_1000_movies,
            ARRAY_TO_STRING(LIST(DISTINCT imd.lang_name), '; ') AS language_lst,
            ARRAY_TO_STRING(LIST(DISTINCT CASE WHEN ci.category = 'director' THEN tnb.primaryname END), '; ') AS director_lst,
            ARRAY_TO_STRING(LIST(DISTINCT CASE WHEN ci.category = 'actor' THEN tnb.primaryname END), '; ') AS actor_lst,
            ARRAY_TO_STRING(LIST(DISTINCT CASE WHEN ci.category = 'actress' THEN tnb.primaryname END), '; ') AS actress_lst,
            CAST(CURRENT_DATE AS STRING) AS last_refresh_date,
            ROW_NUMBER() OVER (
                ORDER BY
                    popularity_rnk ASC NULLS LAST,
                    language_popularity_rnk ASC NULLS LAST,
                    top_250_rnk ASC NULLS LAST,
                    top_1000_rnk ASC NULLS LAST,
                    language_votes_rnk ASC NULLS LAST,
                    box_office DESC NULLS LAST,
                    num_votes DESC NULLS LAST,
                    avg_rating DESC NULLS LAST
            ) AS overall_popularity_rnk
        FROM delta_scan('{delta_table_path(silver_schema, "t_title_basics")}') AS tbs
        LEFT OUTER JOIN delta_scan('{delta_table_path(silver_schema, "t_title_ratings")}') AS trt
            ON trt.tconst = tbs.tconst
        LEFT OUTER JOIN crew_info AS ci
            ON ci.tconst = tbs.tconst
        LEFT OUTER JOIN delta_scan('{delta_table_path(silver_schema, "t_name_basics")}') AS tnb
            ON tnb.nconst = ci.nconst
        LEFT OUTER JOIN delta_scan('{delta_table_path(silver_schema, "t_imdb_top")}') AS imd
            ON imd.tconst = tbs.tconst
        LEFT OUTER JOIN delta_scan('{delta_table_path(silver_schema, "t_bo")}') AS tbo
            ON tbo.tconst = tbs.tconst
        WHERE tbs.titletype IN ('movie', 'tvMiniSeries', 'short', 'tvSeries', 'tvShort', 'tvSpecial')
        GROUP BY ALL
    """).record_batch(),
    delta_table_path(gold_schema, "t_imdb")
)

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
