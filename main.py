import os
from utils import *
import matplotlib.pyplot as plt
import seaborn as sns
from schema import MOVIE_SCHEMA, CREDIT_SCHEMA
import logging



 #declare constant variables
BASE_URL = "https://api.themoviedb.org/3/movie/"
API_ACCESS_TOKEN = os.getenv('API_ACCESS_TOKEN')
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_ACCESS_TOKEN}"
}

# Flag to determine whether to fetch data from the internet or load from local files
INTERNET_CONNECTION = True


# Configure logger to write ETL logs to a file
logger = logging.getLogger(__name__)
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)



# Top-level ETL function that orchestrates the full pipeline
# 1. Extracts movie and credit data
# 2. Transforms and cleans the data
# 3. Loads the processed data into storage
def etl(spark, movie_ids, movie_schema, credit_schema):
    logger.info("---INITIATING ETL PIPELINE----")
    movies_df, credits_df = extract_data(spark, movie_ids,  movie_schema, credit_schema)
    transformed_df = transform(movies_df, credits_df)
    load_data(transformed_df)
    return transformed_df


# Extract movie and credit data from TMDB API or local cache
# If INTERNET_CONNECTION is False, it loads data from local JSON files
# Writes raw data to JSON for caching and future offline use
def extract_data(spark, movie_ids,  movie_schema, credit_schema):
    logger.info("EXTRACTING DATA FROM API")
    movies_df = spark.createDataFrame([], schema=movie_schema)
    credits_df = spark.createDataFrame([], schema=credit_schema)
    if (INTERNET_CONNECTION != False):
        movies_df,credits_df = get_all_data(spark, BASE_URL, movie_ids, HEADERS, movie_schema=movie_schema, credit_schema=credit_schema)
        movies_df.write.mode("overwrite").json('./data/movies_raw_data.json')
        credits_df.write.mode("overwrite").json('./data/credits_raw_data.json')
    else:
        movies_df = spark.read.json('./data/movies_raw_data.json', schema=movie_schema)  
        credits_df = spark.read.json('./data/credits_raw_data.json', schema=credit_schema)

    return movies_df, credits_df


# Apply a series of transformations to clean and enrich the data:
# - Drop irrelevant or redundant columns
# - Evaluate nested JSON columns
# - Join movie and credit datasets
# - Replace invalid or placeholder values
# - Convert budget and revenue to millions
# - Drop duplicates and incomplete records
# - Filter to include only "Released" movies
# - Replace missing vote counts using genre averages
# - Reorder and reindex columns for consistency
def transform(movies_df, credits_df):
    logger.info("TRANSFORMING DATA")
    #Dropping irrelevant columns
    columns_to_drop = ['adult', 'original_title', 'imdb_id', 'video', 'homepage']
    movies_df = drop_cols(movies_df, columns_to_drop)

    #evaluating json-like columns in movie_df and credits_df
    movies_df = eval_movies_json_col(movies_df)
    credits_df = eval_credits_json_col(credits_df)

    #Dropping irrelevant columns
    # credits_df
    credits_df = drop_cols(credits_df, ['crew'])

    # combining movies df and credits df using inner join
    combined_df = join_dfs(movies_df, credits_df, on='id', how='inner')

    ## Replacing invalid data with Nan
    cols_with_zero_val = ['budget', 'revenue', 'runtime']
    cols_with_placeholders = ['overview', 'tagline']
    cols_to_musd = ['revenue', 'budget']

    combined_df = replace_with_nan(combined_df, cols_with_zero_val)
    combined_df = replace_known_placeholders(combined_df, cols_with_placeholders)

    combined_df = convert_to_milions(combined_df, cols_to_musd)

        #Drop Duplicate
    combined_df = combined_df.drop_duplicates()

    #drop unknown id and title
    combined_df = combined_df.dropna(subset=['id', 'title'])

    #kekep only roow where at least 1- columns have non_Non values
    combined_df = combined_df.dropna(thresh=10)

    # filter to include only released movies
    combined_df = combined_df.filter(F.col("status") == "Released")
    # #drop status column
    combined_df = drop_cols(combined_df, ['status'])

    # Replacing Movies with vote_count = 0 with avearge count per genre
    combined_df = replace_zero_count_vote(combined_df)

    #reorder columns
    reordered_df = reorder_col_and_reindex(combined_df)

    return reordered_df

# Save the final cleaned dataset into a JSON file
def load_data(df):
    logger.info("LOADING PROCESSED DATA")
    df.coalesce(1).write.mode("overwrite").json('./data/processed_data')

# Generate and log KPIs using a helper function
def gen_kpi(df):
    logger.info("KPI IMPLEMENTAION")
    result = kpi_implementation(df)
    logger.info(result)


# Log insights from the dataset based on several filters and conditions
# Includes:
# - Best Sci-Fi/Action movie with Bruce Willis
# - Movies starring specified actors and directed by a certain director
# - Franchise vs Standalone comparison
# - Most successful franchise movie
def gen_insights(df):
    logger.info("GENERATING SOME INSIGHTS")
    logger.info(f"Best Scifi Action Movie with Actor;Bruce WIllis-::\n {get_best_scifi_action_movies_with_actor(df)}")
    logger.info(f"MOVIE Ditrected By 'Quentin Tarantino' and starring Uma Thurman-:\n: {get_starring_directed_by(df, ['Uma Thurman'], 'Quentin Tarantino')}")
    logger.info(f"Franchise Vrs Standalone-::\n{frachise_vrs_standalone(df)}")
    logger.info(f"Most Succesfule Franchise Movie-::\n {most_succesful_franchises(df)}")





if __name__ == "__main__":

    movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445,181808, 330457, 351286, 109445, 321612, 260513]
    
    # Create Spark session
    spark = create_spark_session("imdb_movie_data_analysis")
    movies_df = spark.createDataFrame([], schema=MOVIE_SCHEMA)
    credits_df = spark.createDataFrame([], schema=CREDIT_SCHEMA)

    # Run the ETL pipeline
    transformed_df = etl(spark, movie_ids, MOVIE_SCHEMA, CREDIT_SCHEMA)
    transformed_df.show(truncate=True)
    
    # Generate KPIs and insights
    gen_kpi(transformed_df)
    gen_insights(transformed_df)
