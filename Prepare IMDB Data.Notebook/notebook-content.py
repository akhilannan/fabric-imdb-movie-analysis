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
# META   "language_group": "jupyter_python"
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
abfss_table_path = f"{abfss_path}/Tables"
files_path = "/lakehouse/default/Files"
bronze_path = f"{files_path}/bronze/"
silver_path = f"{abfss_table_path}/silver/"
gold_path = f"{abfss_table_path}/gold/"
top_table_path = silver_path + "t_imdb_top"
bo_table_path = silver_path + "t_bo"
imdb_table_path = gold_path + "t_imdb"
file_list = [
    "name.basics.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]
parallelism = os.cpu_count() * 2
notebookutils.fs.mkdirs(bronze_path)

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
            "tconst": None,
            "rnk": None,
            "type": None,
            "movie_name": None,
            "movie_year": None,
            "box_office": int(row_cols[2].text.replace("$", "").replace(",", "")),
            "url": url
        }
        
        if is_yearly:
            movie_dict.update({
                "movie_name": row_cols[1].text,
                "movie_year": int(url.split("/")[-1])
            })
        else:
            movie_dict.update({
                "type": url.split("/")[-2],
                "rnk": int(row_cols[0].text.replace(",", "")),
                "tconst": re.search(r"(tt\d+)", row_cols[1].find("a")["href"]).group(1)
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
            "tconst": node.get("id") or item.get("titleId"),
            "rnk": item.get("currentRank") or rank,
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


def process_movie_urls_polars(url_list: list[str]) -> pl.DataFrame:
    """
    Takes a list of URLs as input and returns a Polars DataFrame
    containing movie data scraped from those URLs.

    Args:
        url_list: A list of strings, where each string is a URL to scrape.

    Returns:
        A Polars DataFrame with the scraped movie data.
    """
    return pl.DataFrame(scrape_movie_data_threaded(url_list))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Function to extract Language dimension

# CELL ********************

def extract_lang_data_polars(url):
    """
    Extracts language names and codes from the specified Wikipedia ISO 639-1 codes page
    and returns a Polars DataFrame.

    Args:
        url: The URL of the Wikipedia page.

    Returns:
        A Polars DataFrame with 'lang_name' and 'lang_code' columns.
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

    return pl.DataFrame(data)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Function to create or replace delta table

# CELL ********************

def create_or_replace_delta_table(df: pl.DataFrame | pl.LazyFrame, path: str) -> None:
    """
    Creates or replaces a Delta Lake table with the given Polars DataFrame or LazyFrame.
    Uses the .write_delta method directly on the DataFrame/LazyFrame.

    Args:
        df: The Polars DataFrame or LazyFrame to write.
        path: The path to the Delta Lake table.
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    df.write_delta(
        path, 
        mode="overwrite", 
        delta_write_options={"schema_mode": "overwrite"}
    )


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

url_list += [f"{BOXOFFICE_CHART}{n}" for n in range(0, 1000, 200)]

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

# 1. Create DataFrame and Apply UDF (scrape_movie_data equivalent)
imdb_df = process_movie_urls_polars(url_list)

# 2. Join with Language DataFrame and extract language code using a conditional join
imdb_df = imdb_df.filter(pl.col("tconst").is_not_null()).join(
    extract_lang_data_polars(LANGUAGE_URL),
    left_on=pl.when(pl.col("type").str.contains(PRIMARY_LANGUAGE_PATTERN))
    .then(pl.col("type").str.extract(r"primary_language=([a-z]+)", 1))
    .otherwise(None),
    right_on="lang_code",
    how="left",
)

# 3. Define Conditional Columns using a dictionary
conditional_columns = {
    "box_office": pl.col("box_office") >= 0,
    "is_in_top_250": pl.col("type").is_in(
        [
            "toptv",
            "top",
            "top-rated-indian-movies",
            "top-rated-malayalam-movies",
            "top-rated-tamil-movies",
            "top-rated-telugu-movies",
        ]
    ),
    "is_in_top_1000": pl.col("type").str.contains("top_1000"),
    "is_popular": pl.col("type").str.contains(POPULAR_MOVIE_PATTERN),
    "is_primary_lang": pl.col("type").str.contains(PRIMARY_LANGUAGE_PATTERN),
    "is_asc": pl.col("type").str.contains(",asc"),
    "is_desc": pl.col("type").str.contains(",desc"),
}

# 4. Select Final Columns with dynamic conditional column creation and inline rank adjustment
imdb_df = imdb_df.select(
    [
        "tconst",
        "lang_name",
        (
            pl.col("rnk")
            + pl.when(pl.col("type").str.contains(POPULAR_MOVIE_PATTERN))
            .then(
                pl.col("type").str.extract(r"moviemeter=(\d+),", 1).cast(pl.Int64) - 1
            )
            .otherwise(0)
        ).alias("rnk"),
        pl.col("type").alias("url_type"),
        "url",
        *[
            pl.when(condition)
            .then(
                pl.lit("Y")
                if (is_bool_col := col_name.startswith("is_"))
                else pl.col(col_name)
            )
            .otherwise(pl.lit("N") if is_bool_col else None)
            .alias(col_name)
            for col_name, condition in conditional_columns.items()
        ],
    ]
)

# 5. Create or Replace Delta Table
create_or_replace_delta_table(
    imdb_df, top_table_path
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Extract Box Office data

# CELL ********************

# Generate the list of URLs
current_year = date.today().year
rng = range(1977, current_year + 1, 1)
yearly_box_office_url_list = [BOXOFFICE_YEAR + str(n) for n in rng]

# Create a Polars DataFrame
bo_df = process_movie_urls_polars(yearly_box_office_url_list)

# Filter out rows with null movie_name and select the desired columns
bo_df = bo_df.filter(pl.col("movie_name").is_not_null()).select(
    pl.col("movie_name"),
    pl.col("movie_year"),
    pl.col("box_office"),
)

create_or_replace_delta_table(
    bo_df, bo_table_path
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Load IMDB FULL data

# CELL ********************

def load_table(file):
    bronze_file_path = bronze_path + file
    urlretrieve(DATASET_URL + file, bronze_file_path)
    silver_table_name = "t_" + file.replace('.tsv.gz', '').replace(".", "_")
    silver_table_path = silver_path + silver_table_name
    delim = "\t"

    # Define schema with specified data types
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

    # Read the CSV file, handling potential errors more concisely.
    df = pl.read_csv(
        bronze_file_path,
        separator=delim,
        infer_schema_length=0,
        has_header=True,
        new_columns=None,
        try_parse_dates=False,
        quote_char=None,
        low_memory=True,
    )

    # Replace "\\N" with None (NULL) in all columns before casting
    for col_name in df.columns:
        df = df.with_columns(
            pl.when(pl.col(col_name) == r"\\N")
            .then(None)
            .otherwise(pl.col(col_name))
            .alias(col_name)
        )

    # Apply schema only to existing columns
    for col_name, data_type in schema.items():
        if col_name in df.columns:
            df = df.with_columns(pl.col(col_name).cast(data_type, strict=False))

    # Write to Delta format
    create_or_replace_delta_table(df, silver_table_path)

# Call the function in parallel
with ThreadPoolExecutor(max_workers=parallelism) as executor:
    executor.map(lambda file: load_table(file), file_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Create IMDB Gold table

# CELL ********************

# Get Crew information
crew_df = pl.concat([
    pl.scan_delta(silver_path + "t_title_principals")
    .filter(pl.col("category").is_in(["actor", "actress"]))
    .select(["tconst", "category", "nconst"]),
    
    pl.scan_delta(silver_path + "t_title_crew")
    .filter(pl.col("directors").is_not_null())
    .select([
        "tconst",
        pl.lit("director").alias("category"), 
        pl.col("directors").str.split(",").alias("nconst")
    ]).explode("nconst")
], how="vertical")

# Get Box office information
box_office_df = (
    pl.scan_delta(silver_path + "t_bo")
    .join(
        pl.scan_delta(silver_path + "t_title_basics"),
        left_on=[
            "movie_name",
            "movie_year",
        ],
        right_on=[
            "primaryTitle",
            "startYear",
        ]
    )
    .filter(pl.col("titleType") == "movie")
    .join(
        pl.scan_delta(silver_path + "t_title_ratings"),
        left_on="tconst",
        right_on="tconst"
    )
    .select([
        pl.col("tconst"),
        "box_office",
        "numVotes"
    ])
    .with_columns([
        pl.col("numVotes")
        .rank("dense", descending=True)
        .over(["tconst"])
        .alias("rnk")
    ])
    .filter(pl.col("rnk") == 1)
    .select(["tconst", "box_office"])
)

# Final dataframe processing
final_df = (
    pl.scan_delta(silver_path + "t_title_basics")
    .join(
        pl.scan_delta(silver_path + "t_title_ratings"),
        left_on="tconst",
        right_on="tconst",
        how="left"
    )
    .join(
        crew_df,
        left_on="tconst",
        right_on="tconst",
        how="left"
    )
    .join(
        pl.scan_delta(silver_path + "t_name_basics"),
        left_on="nconst",
        right_on="nconst",
        how="left"
    )
    .join(
        pl.scan_delta(silver_path + "t_imdb_top"),
        left_on="tconst",
        right_on="tconst",
        how="left"
    )
    .join(
        box_office_df,
        left_on="tconst",
        right_on="tconst",
        how="left"
    )
    .filter(pl.col("titleType").is_in([
        "movie", "tvMiniSeries", "short", "tvSeries", "tvShort", "tvSpecial"
    ]))
    .group_by("tconst")
    .agg([
        pl.col("titleType")
            .str.to_titlecase()
            .str.replace("Tv", "TV ")
            .max()
            .alias("title_type"),
        pl.col("primaryTitle").max().alias("primary_title"),
        pl.col("originalTitle").max().alias("original_title"),
        pl.col("startYear").max().alias("release_year"),
        pl.when(pl.col("isAdult") == 1)
            .then(pl.lit("Y"))
            .otherwise(pl.lit("N"))
            .max()
            .alias("is_adult"),
        pl.col("runtimeMinutes").max().alias("runtime_min"),
        pl.col("genres")
            .filter(~pl.col("genres").str.contains("_N"))
            .max()
            .alias("genres"),
        pl.col("averageRating").max().alias("avg_rating"),
        pl.col("numVotes").max().alias("num_votes"),
        pl.coalesce(
            [
                pl.col("box_office"),
                pl.col("box_office_right")
            ]
        ).abs().max().alias("box_office"),
        pl.when(pl.col("is_in_top_250") == "Y")
            .then(pl.col("rnk"))
            .max()
            .alias("top_250_rnk"),
        pl.when(pl.col("is_in_top_1000") == "Y")
            .then(pl.col("rnk"))
            .max()
            .alias("top_1000_rnk"),
        pl.when(pl.col("is_popular") == "Y")
            .then(pl.col("rnk"))
            .max()
            .alias("popularity_rnk"),
        pl.when((pl.col("is_primary_lang") == "Y") & (pl.col("is_asc") == "Y"))
            .then(pl.col("rnk"))
            .max()
            .alias("language_popularity_rnk"),
        pl.when((pl.col("is_primary_lang") == "Y") & (pl.col("is_desc") == "Y"))
            .then(pl.col("rnk"))
            .max()
            .alias("language_votes_rnk"),
        pl.coalesce(
            pl.col("is_in_top_1000").first(),
            pl.lit("N")
        ).alias("is_top_1000_movies"),
        pl.col("lang_name").unique().str.concat("; ").alias("language_lst"),
        pl.when(pl.col("category") == "director")
            .then(pl.col("primaryName"))
            .str.concat("; ")
            .alias("director_lst"),
        pl.when(pl.col("category") == "actor")
            .then(pl.col("primaryName"))
            .str.concat("; ")
            .alias("actor_lst"),
        pl.when(pl.col("category") == "actress")
            .then(pl.col("primaryName"))
            .str.concat("; ")
            .alias("actress_lst"),
        pl.lit(date.today()).alias("last_refresh_date")
    ])
    .sort(by=[
        pl.col("popularity_rnk"),
        pl.col("language_popularity_rnk"),
        pl.col("top_250_rnk"),
        pl.col("top_1000_rnk"),
        pl.col("language_votes_rnk"),
        pl.col("num_votes").sort(descending=True),
    ], nulls_last=True)
    .with_row_index("overall_popularity_rnk")
    .with_columns((pl.col("overall_popularity_rnk") + 1).alias("overall_popularity_rnk"))
)

# Write the final result
create_or_replace_delta_table(final_df, imdb_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
