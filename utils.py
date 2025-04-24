import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
import numpy as np


# TODO : Convert panda dependant functions to spark functions

def fetch_from_api(url, headers):
    """
    Fetches JSON data from a specified API endpoint.
    Returns the JSON response if the status is 200 (OK), otherwise logs an error and returns None.
    """
    api_response = requests.get(url, headers=headers)
    if api_response.status_code == 200:
        print(f"{url}: Success")
        return api_response.json()
    else:
        print(f"Error occured trying to fetch from api route: {url}. \nError Status Code: {api_response.status_code}")
        return None
    
def get_all_data(spark, base_url, movie_ids, headers, movie_schema, credit_schema):
    """
    Retrieves movie and credit data for each movie ID from the TMDB API.
    Aggregates the results into separate DataFrames for movies and credits.
    """
    movies = []
    credits = []

    for id in movie_ids:
        movie_detail_url = f"{base_url}{id}"
        credits_url = f"{movie_detail_url}/credits"

        movie_response = fetch_from_api(movie_detail_url, headers)
        credit_response = fetch_from_api(credits_url, headers)

        if (movie_response != None) and (credit_response != None):
            movies.append(movie_response)
            credits.append(credit_response)

    # print(movies)
    
    movies_df = convert_to_dataframe(spark, movies, schema=movie_schema)
    credits_df = convert_to_dataframe(spark, credits, schema=credit_schema)


    return movies_df, credits_df

def create_spark_session(app_name):
    return SparkSession.builder.appName("process_data")\
        .getOrCreate()

def convert_to_dataframe(spark, json_data, schema):
    """
    Converts a list of JSON objects into a Pandas DataFrame.
    """
    return spark.createDataFrame(json_data, schema)

def join_json_key_value(df, col, sep, is_list, key):
    """
    Extracts and joins values from JSON-like columns into a string using a separator.
    Handles both list of dicts and single dicts.
    """
    udf_flatten_nested = F.udf(lambda lst: sep.join([element[key] for element in lst])if lst else None, StringType())
    udf_extract_key = F.udf(lambda struct: struct[key] if isinstance(struct, dict) else None, StringType())
    if df is None:
        raise ValueError("Input DataFrame is None.")
    try:
        if is_list:
            df = df.withColumn(col, udf_flatten_nested(F.col(col)))
        else:
            df = df.withColumn(col, udf_extract_key(F.col(col)))
        return df

    except Exception as e:
        print(f'Error: {e}')
        return df
    

def eval_movies_json_col(df):
    """
    Processes movie-specific JSON-like columns:
    - Flattens nested data such as genres, languages, countries, and collections into string format.
    - Converts origin_country list to string.
    """
    cols = ['genres', 'production_companies', 'production_countries', 'spoken_languages', 'belongs_to_collection']
    
    for col in cols:
       if col == 'spoken_languages':
           df = join_json_key_value(df, col, sep='|', is_list=True, key='english_name')
       elif col == 'belongs_to_collection':
           df = df.withColumn(col,  F.col("belongs_to_collection.name"))
        #    df = join_json_key_value(df, col, sep='|', is_list=False, key='name') 
       else:
            df = join_json_key_value(df, col, sep='|', is_list=True, key='name') 

    df = stringify_list(df, 'origin_country')
    return df

def stringify_list(df, col):
    """
    Simplifies list-type columns by keeping only the first element.
    """
    udf_strigify =  F.udf((lambda content: content[0] if isinstance(content, list) else None))
    df = df.withColumn(col, udf_strigify(F.col(col)))
    return df

# #evaluating json-like columns in credits_df
def eval_credits_json_col(df):
    """
    Processes credit-related JSON-like columns:
    - Joins cast names into a string.
    - Extracts director name and calculates crew/cast sizes.
    """
    udf_extract_dir = F.udf(lambda crew_list:  next((crew['name'] for crew in crew_list if crew['job'] =='Director'), None)) 

    df = df.withColumn('cast_size', F.size(F.col('cast')))
    df = join_json_key_value(df, 'cast', '|', True, 'name')
    df = df.withColumn('director', udf_extract_dir(F.col('crew')))
    # df['director'] = df['crew'].apply(lambda crew_list:  next((crew['name'] for crew in crew_list if crew['job'] =='Director'), None))
    df = df.withColumn('crew_size', F.size(F.col('crew')))

    return df

# # Coverting columns to specific datatypes
# def convert_datatypes(df):
#     """
#     Converts specific columns to appropriate data types, including datetime and numeric types.
#     """
#     df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
#     df['id'] = pd.to_numeric(df['id'], errors='coerce')
#     df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')

#     df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
#     return df


def drop_cols(df, cols):
    """
    Drops specified columns from the DataFrame.
    """
    return df.drop(*cols)

def join_dfs(df_1, df_2, on, how=None):
    """
    Merges two DataFrames on a specified column.
    Optional 'how' parameter defines merge type.
    """
    if how:
        df = df_1.join(df_2, on=on, how=how)
    else:
        df = df_1.join(df_2, on=on)
    return df

def replace_with_nan(df, cols):
    """
    Replaces zero values in specified columns with NaN for cleaner analysis.
    """
    df = df.replace(0, float('nan'), subset=cols)
    return df

def replace_known_placeholders(df, cols):
    """
    Replaces placeholder text like 'No Data' with NaN in specified columns.
    """
    df = df.replace(['No Data'], None, subset=cols)
    # df[cols] = df[cols].replace('No Data', pd.NA)
    return df

def convert_to_milions(df, cols):
    """
    Converts monetary values to millions and renames the columns with '_musd' suffix.
    """
    for col in cols:
        df = df.withColumn(col, F.col(col)/1000)
        col_rename = f"{col}_musd"
        df = df.withColumnRenamed(col, col_rename)
    return df
        
def rank_movies_with_col(df, cols, asc=False):

    if isinstance(cols, str):
        cols = [cols]
    if isinstance(asc, bool):
        asc = [asc] * len(cols)
    
    sort_exprs = [
        F.col(c).asc() if a else F.col(c).desc()
        for c, a in zip(cols, asc)
    ]
    return df.orderBy(*sort_exprs)
    # if asc == True:
    #     return df.orderBy(F.col(col).asc())
    # return df.orderBy(F.col(col).desc())

def get_topmost(df, col):
    return df.select(F.col(col)).first()



def cal_roi(df):
    """
    Calculates Return on Investment (ROI) for movies with a budget ≥ $10M.
    Adds a new column 'roi_musd' to the DataFrame.
    """
    filtered_df = df.filter(F.col('budget_musd') >= 10)
    filtered_df = filtered_df.withColumn('roi_musd',  F.col('revenue_musd')/F.col('budget_musd'))
    return filtered_df

def replace_zero_count_vote(df):
    """
    Fills in missing or zero vote_count and vote_average using genre-wise averages.
    Helps in dealing with sparse rating data.
    """
    df = df.withColumn('genres_list', F.split(F.col('genres'), '\\|'))
    df_exploded = df.withColumn('genres_list', F.explode('genres_list'))

    df_exploded_valid_votes = df_exploded.dropna(subset=['vote_count', 'vote_average'])

    genres_stats = df_exploded_valid_votes.groupby('genres_list').agg(
        F.avg("vote_count").alias("genre_vote_count"),
        F.avg("vote_average").alias("genre_vote_average")
    )

    df_exploded =  join_dfs(df_exploded, genres_stats, 'genres_list', how='left')

    filled_votes = df_exploded.groupby('id').agg(
       F.avg("genre_vote_count").alias("filled_vote_count"),
        F.avg("genre_vote_average").alias("filled_vote_average")
    )
  
    df = join_dfs(df, filled_votes, 'id', how='left')

    df = df.withColumn('vote_average', 
                       F.when(F.col('vote_average').isNull(), F.col('filled_vote_average'))\
                        .otherwise(F.col('vote_average')))
    

    df = df.withColumn(
        "vote_count",
        F.when((F.col("vote_count").isNull()) | (F.col("vote_count") == 0), F.col("filled_vote_count"))
        .otherwise(F.col("vote_count"))
    )
    
    df = drop_cols(df, ["filled_vote_count","genres_list", "filled_vote_average"])
    return df


def reorder_col_and_reindex(df):
    """
    Reorders the DataFrame columns to a preferred structure.
    """
    reordered_df = df.select(['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection', 'original_language',
                                    'budget_musd', 'revenue_musd', 'production_companies','production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
    'overview', 'spoken_languages', 'poster_path','cast', 'cast_size', 'director', 'crew_size'])
    return reordered_df