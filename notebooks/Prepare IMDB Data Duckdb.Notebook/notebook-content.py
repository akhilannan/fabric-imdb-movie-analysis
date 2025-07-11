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

import duckdb
from deltalake import writer
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
        duckdb_con.setup_lakehouse_views(schema=schema, table=table)

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

# # Initialize DuckDB Connection

# CELL ********************

import time
import jwt


class DuckDBLakehouseConnector:
    """
    A wrapper for a DuckDB connection that handles Fabric Lakehouse connectivity
    with automatic token refresh.
    """

    # Buffer period (in seconds) before token expiration to trigger a refresh.
    TOKEN_REFRESH_BUFFER = 60

    def __init__(
        self,
        lakehouse_name=None,
        workspace_name=None,
        setup_views=True,
        database="temp",
    ):
        """
        Initialize the Lakehouse connector.

        Parameters:
        -----------
        lakehouse_name : str, optional
            Name of the lakehouse. If None, uses the current lakehouse.
        workspace_name : str, optional
            Name of the workspace. If None, uses the current workspace.
        setup_views : bool, optional
            Whether to set up lakehouse views during initialization (default: True).
            Set to False if you only need to query existing views or tables.
        database : str, optional
            Database configuration:
            - "memory": Creates an in-memory database that doesn't persist
            - "temp": Creates a temporary persistent database with random name (default)
            - "<path>": Creates/opens a persistent database at the specified path
        """
        self._workspace_id = fabric.resolve_workspace_id(workspace_name)
        self._lakehouse_id = (
            fabric.resolve_item_id(
                lakehouse_name, item_type="Lakehouse", workspace=workspace_name
            )
            if lakehouse_name
            else fabric.get_lakehouse_id()
        )
        self._abfss_path = (
            f"abfss://{self._workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{self._lakehouse_id}/Tables"
        )
        self._con = self._connect(database)
        self._current_token = self._get_fresh_token()
        self._update_onelake_secret(self._current_token)
        self._con.sql("SET enable_object_cache=true;")
        if setup_views:
            self.setup_lakehouse_views()

    def _connect(self, database):
        """Helper to create a DuckDB connection based on database configuration."""
        if database == "memory":
            return duckdb.connect(":memory:")
        elif database == "temp":
            return duckdb.connect(f"temp_{time.time_ns()}.duckdb")
        else:
            return duckdb.connect(database)

    def _update_onelake_secret(self, token):
        """Update the onelake secret with the provided token."""
        self._con.sql(
            f"""
            CREATE OR REPLACE SECRET onelake (
                TYPE AZURE,
                PROVIDER ACCESS_TOKEN,
                ACCESS_TOKEN '{token}'
            );
        """
        )

    def _is_token_valid(self, token):
        """
        Check if the token is still valid by decoding its JWT payload.
        The token is considered invalid if it expires within TOKEN_REFRESH_BUFFER seconds.
        """
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            expiration_time = decoded.get("exp", 0)
            return time.time() < (expiration_time - self.TOKEN_REFRESH_BUFFER)
        except Exception:
            return False

    def _get_fresh_token(self):
        """Get a fresh token from notebookutils."""
        return notebookutils.credentials.getToken("storage")

    def _check_and_refresh_token(self):
        """Refresh the token if it is no longer valid, with proper error handling."""
        if not self._is_token_valid(self._current_token):
            try:
                new_token = self._get_fresh_token()
                self._update_onelake_secret(new_token)
                self._current_token = new_token
            except Exception as e:
                raise RuntimeError("Failed to refresh token") from e

    def setup_lakehouse_views(self, schema=None, table=None):
        """
        Public method to set up lakehouse views, optionally filtered by schema and/or table.
        This method refreshes the token if necessary before creating the views.

        Parameters:
            schema (str, optional): If provided, only views in this schema will be set up.
            table (str, optional): If provided, only the view for this table will be set up.
        """
        self._check_and_refresh_token()

        conditions = []
        if schema:
            conditions.append(f"sch = '{schema}'")
        if table:
            conditions.append(f"tbl = '{table}'")
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        base_query = f"""
            WITH table_paths AS (
                SELECT DISTINCT 
                    regexp_extract(file, '^(.*?)/[^/]+\\.parquet$', 1) as path,
                    split_part(path, '/', -2) as sch,
                    split_part(path, '/', -1) as tbl
                FROM glob('{self._abfss_path}/*/*/*')
                {where_clause}
            )
            SELECT 
                string_agg('(''' || sch || ''', ''' || tbl || ''')', ',') as view_tuples,
                string_agg(
                    'CREATE SCHEMA IF NOT EXISTS ' || sch || '; ' ||
                    'CREATE OR REPLACE VIEW ' || sch || '.' || tbl ||
                    ' AS SELECT * FROM delta_scan(''' || '{self._abfss_path}/' || sch || '/' || tbl || ''')',
                    '; '
                ) as setup_commands
            FROM table_paths;
        """

        result = self._con.sql(base_query).fetchone()
        view_tuples, setup_commands = result[0], result[1]

        if setup_commands:
            self._con.sql(setup_commands)
            if view_tuples:
                show_query = f"SELECT * FROM (SHOW ALL TABLES) WHERE (schema, name) IN ({view_tuples})"
                self._con.sql(show_query).show(max_width=150)
            self._con.sql("CHECKPOINT")

    def sql(self, query):
        self._check_and_refresh_token()
        return self._con.sql(query)

    def __getattr__(self, name):
        return getattr(self._con, name)


con = DuckDBLakehouseConnector(setup_views=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Load IMDB FULL data

# CELL ********************

def load_table(file):
    max_retries = 3
    raw_file_path = raw_path + file 

    for attempt in range(max_retries):
        tmp_con = None
        try:
            tmp_con = DuckDBLakehouseConnector(setup_views=False)

            urlretrieve(DATASET_URL + file, raw_file_path)

            table_name = "t_" + file.replace(".tsv.gz", "").replace(".", "_")
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

            detected_columns = tmp_con.sql(
                f"SELECT Columns FROM sniff_csv('{raw_file_path}', sample_size = 1)"
            ).fetchone()[0]
            cast_statements = [
                (
                    f"CAST({col_info['name']} AS {schema[col_info['name']]}) AS {col_info['name']}"
                    if col_info["name"] in schema
                    else col_info["name"]
                )
                for col_info in detected_columns
            ]

            create_or_replace_delta_table(
                tmp_con.sql(
                    f"""
                    SELECT {','.join(cast_statements)}
                    FROM read_csv('{raw_file_path}',
                                 header=true,
                                 delim='\\t',
                                 quote='',
                                 nullstr='\\N',
                                 ignore_errors=false)
                """
                ).record_batch(),
                bronze_schema,
                table_name,
                writer,
                con,
            )

            tmp_con.close()
            return  # Success - exit the function

        except Exception as e:
            # Cleanup resources
            if tmp_con is not None:
                tmp_con.close()
            if os.path.exists(raw_file_path):
                os.remove(raw_file_path)  # Remove potentially corrupted file

            # Retry logic
            if attempt < max_retries:
                print(
                    f"Retrying {file} (attempt {attempt + 1}/{max_retries}) due to error: {str(e)}"
                )
                continue
            else:
                raise RuntimeError(
                    f"Failed to process {file} after {max_retries} attempts"
                ) from e


parallel_load_tables(file_list, parallelism, load_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB Title table

# CELL ********************

create_or_replace_delta_table(
    con.sql(f"""
        SELECT
            tbs.tconst,
            tbs.titletype AS title_type,
            tbs.primarytitle AS primary_title,
            tbs.originaltitle AS original_title,
            tbs.startyear AS release_year,
            CASE 
                WHEN tbs.isadult = 1 THEN 'Y' 
                ELSE 'N' 
            END AS is_adult,
            tbs.runtimeminutes AS runtime_min,
            tbs.genres AS genres,
            trt.averagerating AS avg_rating,
            trt.numvotes AS num_votes
        FROM 
            {bronze_schema}.t_title_basics AS tbs
        LEFT OUTER JOIN 
            {bronze_schema}.t_title_ratings AS trt
            ON trt.tconst = tbs.tconst
    """).record_batch(), 
    silver_schema, 
    "t_title",
    writer,
    con
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB CREW table

# CELL ********************

create_or_replace_delta_table(
    con.sql(
        f"""
        WITH unpivot_crew AS (
            UNPIVOT {bronze_schema}.t_title_crew
            ON directors, writers
            INTO
                NAME category
                VALUE nconst
        ),
        comb_crew AS (
            SELECT
                tconst,
                category,
                nconst
            FROM
                {bronze_schema}.t_title_principals
            WHERE
                category NOT IN ('director', 'writer')
            
            UNION
            
            SELECT
                tconst,
                regexp_replace(category, 's$', ''),
                UNNEST(string_to_array(nconst, ','))
            FROM
                unpivot_crew
        ),
        tnb AS (
            SELECT
                tnb.nconst,
                tnb.primaryName AS name,
                tnb.birthYear AS birth_year,
                tnb.deathYear AS death_year,
                tnb.primaryProfession AS primary_profession,
                STRING_AGG(tbs.primaryTitle || ' (' || tbs.startYear || ')', ', ') AS known_for_titles
            FROM
                (
                    SELECT
                        * EXCLUDE(knownForTitles),
                        UNNEST(string_split(knownForTitles, ',')) AS known_for_tconst
                    FROM
                        {bronze_schema}.t_name_basics
                ) AS tnb
            LEFT OUTER JOIN
                {bronze_schema}.t_title_basics tbs
            ON
                tbs.tconst = tnb.known_for_tconst
            GROUP BY ALL
        )
        SELECT
            crw.tconst,
            crw.category,
            tnb.name,
            tnb.birth_year,
            tnb.death_year,
            tnb.primary_profession,
            tnb.known_for_titles
        FROM
            comb_crew crw
        INNER JOIN
            tnb
        ON
            tnb.nconst = crw.nconst
        """
    ).record_batch(),
    silver_schema, 
    "t_crew",
    writer,
    con,
)

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

imdb_df = scrape_and_convert_data(url_list, scrape_movie_data_threaded, pa)
lang_df = scrape_and_convert_data(LANGUAGE_URL, extract_lang_data, pa)

create_or_replace_delta_table(
    con.sql(f"""
        WITH base AS (
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
                CASE WHEN imd.type = 'top' THEN 'Y' ELSE 'N' END AS is_top,
                CASE WHEN imd.type = 'toptv' THEN 'Y' ELSE 'N' END AS is_toptv,
                CASE WHEN imd.type = 'top-rated-indian-movies' THEN 'Y' ELSE 'N' END AS is_top_indian_movies,
                CASE WHEN imd.type = 'top-rated-malayalam-movies' THEN 'Y' ELSE 'N' END AS is_top_malayalam_movies,
                CASE WHEN imd.type = 'top-rated-tamil-movies' THEN 'Y' ELSE 'N' END AS is_top_tamil_movies,
                CASE WHEN imd.type = 'top-rated-telugu-movies' THEN 'Y' ELSE 'N' END AS is_top_telugu_movies,
                CASE WHEN imd.type LIKE '{POPULAR_MOVIE_PATTERN}' THEN 'Y' ELSE 'N' END AS is_popular,
                CASE WHEN imd.type LIKE '{PRIMARY_LANGUAGE_PATTERN}' THEN 'Y' ELSE 'N' END AS is_primary_lang,
                CASE WHEN imd.type LIKE '%,asc%' THEN 'Y' ELSE 'N' END AS is_asc,
                CASE WHEN imd.type LIKE '%,desc%' THEN 'Y' ELSE 'N' END AS is_desc,
                CASE WHEN imd.type LIKE '%top_1000%' THEN 'Y' ELSE 'N' END AS is_in_top_1000,
                CASE 
                    WHEN is_top = 'Y' OR is_toptv = 'Y' OR is_top_indian_movies = 'Y' 
                         OR is_top_malayalam_movies = 'Y' OR is_top_tamil_movies = 'Y' 
                         OR is_top_telugu_movies = 'Y' OR is_in_top_1000 = 'Y' 
                    THEN 'Y' 
                    ELSE 'N' 
                END AS is_top_title
            FROM imdb_df imd
            LEFT JOIN lang_df lng
                ON CASE
                    WHEN imd.type LIKE '{PRIMARY_LANGUAGE_PATTERN}'
                    THEN regexp_extract(imd.type, 'primary_language=([a-z]+)', 1)
                    WHEN imd.type LIKE '%malayalam%'
                    THEN 'ml'
                    WHEN imd.type LIKE '%tamil%'
                    THEN 'ta'
                    WHEN imd.type LIKE '%telugu%'
                    THEN 'te'
                END = lng.lang_code
        )
        SELECT
            tconst,
            ARRAY_TO_STRING(LIST(DISTINCT lang_name), '; ') AS language_lst,
            MAX(CASE WHEN is_top_title = 'Y' THEN rnk END) AS top_title_rnk,
            MAX(CASE WHEN is_popular = 'Y' THEN rnk END) AS popularity_rnk,
            MAX(CASE WHEN is_primary_lang = 'Y' AND is_asc = 'Y' THEN rnk END) AS language_popularity_rnk,
            MAX(CASE WHEN is_primary_lang = 'Y' AND is_desc = 'Y' THEN rnk END) AS language_votes_rnk,
            MAX(CASE WHEN is_top = 'Y' THEN rnk END) AS top_250_movie_rnk,
            MAX(CASE WHEN is_toptv = 'Y' THEN rnk END) AS top_250_tv_rnk,
            MAX(CASE WHEN is_top_indian_movies = 'Y' THEN rnk END) AS top_indian_movie_rnk,
            MAX(CASE WHEN is_top_malayalam_movies = 'Y' THEN rnk END) AS top_malayalam_movie_rnk,
            MAX(CASE WHEN is_top_tamil_movies = 'Y' THEN rnk END) AS top_tamil_movie_rnk,
            MAX(CASE WHEN is_top_telugu_movies = 'Y' THEN rnk END) AS top_telugu_movie_rnk,
            MAX(is_in_top_1000) AS is_top_1000_movie,
            MAX(is_top_title) AS is_top_title
        FROM base
        GROUP BY ALL
    """).record_batch(), 
    silver_schema, 
    "t_top_title",
    writer,
    con
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Extract Box Office data

# CELL ********************

bo_df = scrape_and_convert_data(yearly_box_office_url_list, scrape_movie_data_threaded, pa)

create_or_replace_delta_table(
    con.sql(f"""
        SELECT
            tconst,
            box_office
        FROM
            bo_df
        WHERE
            tconst <> 'NA'
        UNION ALL
        SELECT
            ttl.tconst,
            tbo.box_office
        FROM
            bo_df AS tbo
        INNER JOIN
            {silver_schema}.t_title AS ttl
            ON ttl.primary_title = tbo.movie_name
            AND ttl.release_year = tbo.movie_year
            AND ttl.title_type = 'movie'
        WHERE
            tbo.tconst = 'NA'
            AND NOT EXISTS (
                SELECT
                    0
                FROM
                    bo_df bo
                WHERE
                    bo.tconst = ttl.tconst
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
                    ttl.num_votes DESC,
                    tbo.box_office DESC
            ) = 1
    """
    ).record_batch(),
    silver_schema,
    "t_boxoffice",
    writer,
    con
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
    con.sql(f"""
        SELECT 
            * EXCLUDE (top_title_rnk, popularity_rnk, language_popularity_rnk, language_votes_rnk), 
            CASE 
                WHEN is_top_title = 'Y' THEN 
                    ROW_NUMBER() OVER (
                        ORDER BY
                            top_250_movie_rnk,
                            top_250_tv_rnk,
                            top_indian_movie_rnk,
                            top_malayalam_movie_rnk,
                            top_tamil_movie_rnk,
                            top_tamil_movie_rnk,
                            top_telugu_movie_rnk,
                            is_top_title DESC,
                            box_office DESC NULLS LAST,
                            num_votes DESC NULLS LAST,
                            avg_rating DESC NULLS LAST
                    ) 
                ELSE 9999 
            END AS top_title_rnk,
            ROW_NUMBER() OVER (
                ORDER BY
                    popularity_rnk ASC NULLS LAST,
                    language_popularity_rnk ASC NULLS LAST,
                    top_title_rnk ASC NULLS LAST,
                    language_votes_rnk ASC NULLS LAST,
                    box_office DESC NULLS LAST,
                    num_votes DESC NULLS LAST,
                    avg_rating DESC NULLS LAST
            ) AS overall_popularity_rnk
        FROM (
            SELECT
                ttl.tconst,
                ttl.title_type,
                ttl.primary_title,
                ttl.original_title,
                ttl.release_year,
                ttl.is_adult,
                ttl.runtime_min,
                ttl.genres,
                ttl.avg_rating,
                ttl.num_votes,
                tbo.box_office,
                top.top_title_rnk,
                top.popularity_rnk,
                top.language_popularity_rnk,
                top.language_votes_rnk,
                COALESCE(top.top_250_movie_rnk, 9999) AS top_250_movie_rnk,
                COALESCE(top.top_250_tv_rnk, 9999) AS top_250_tv_rnk,
                COALESCE(top.top_indian_movie_rnk, 9999) AS top_indian_movie_rnk,
                COALESCE(top.top_malayalam_movie_rnk, 9999) AS top_malayalam_movie_rnk,
                COALESCE(top.top_tamil_movie_rnk, 9999) AS top_tamil_movie_rnk,
                COALESCE(top.top_telugu_movie_rnk, 9999) AS top_telugu_movie_rnk,
                COALESCE(top.is_top_1000_movie, 'N') AS is_top_1000_movie,
                COALESCE(top.is_top_title, 'N') AS is_top_title,
                top.language_lst AS language_lst,
                STRING_AGG(CASE WHEN tcr.category = 'director' THEN tcr.name END, '; ') AS director_lst,
                STRING_AGG(CASE WHEN tcr.category = 'actor' THEN tcr.name END, '; ') AS actor_lst,
                STRING_AGG(CASE WHEN tcr.category = 'actress' THEN tcr.name END, '; ') AS actress_lst,
                CURRENT_DATE AS last_refresh_date
            FROM 
                {silver_schema}.t_title AS ttl
            LEFT OUTER JOIN 
                {silver_schema}.t_crew AS tcr
                ON ttl.tconst = tcr.tconst
                AND tcr.category IN ('actor', 'actress', 'director')
            LEFT OUTER JOIN 
                {silver_schema}.t_top_title AS top
                ON top.tconst = ttl.tconst
            LEFT OUTER JOIN 
                {silver_schema}.t_boxoffice AS tbo
                ON tbo.tconst = ttl.tconst
            WHERE 
                ttl.title_type IN ('movie', 'tvMiniSeries', 'short', 'tvSeries', 'tvShort', 'tvSpecial')
            GROUP BY 
                ALL
        )
    """).record_batch(),
    gold_schema,
    "t_imdb",
    writer,
    con
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
