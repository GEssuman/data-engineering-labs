import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType



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
    Converts a list of JSON objects into a PySpark DataFrame.
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
    """
    This function sorts a Spark DataFrame based on one or multiple columns.
    Parameters:
    - df: The input Spark DataFrame.
    - cols: A string or list of column names to sort by.
    - asc: A boolean or list of booleans indicating sort order for each column 
           (True for ascending, False for descending). If a single boolean is provided, 
           it's applied to all columns.
    The function uses PySpark's `orderBy` with dynamically constructed sort expressions.
    Returns a DataFrame sorted by the specified columns and sort order.
    """

    if isinstance(cols, str):
        cols = [cols]
    if isinstance(asc, bool):
        asc = [asc] * len(cols)
    
    sort_exprs = [
        F.col(c).asc() if a else F.col(c).desc()
        for c, a in zip(cols, asc)
    ]
    return df.orderBy(*sort_exprs)

def get_topmost(df, col):
    """
    This function retrieves the topmost (first) value from a specified column in a Spark DataFrame.
    Parameters:
    - df: The input Spark DataFrame.
    - col: The name of the column to extract the value from.
    Returns the value of the specified column from the first row of the DataFrame.
    Note: If the DataFrame is not ordered, the result may be non-deterministic.
    """
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


def kpi_implementation(df):
    kpi_results = {}

    #movie with the highest revenue
    kpi_results['highest_revenue'] = get_topmost(rank_movies_with_col(df, 'revenue_musd'), 'title')

    # #movie with the highest budget
    kpi_results['highest_budget'] = get_topmost(rank_movies_with_col(df, 'budget_musd'), 'title')

    #movie with the highest and lowest profit
    df = df.withColumn('profit_musd', F.col('revenue_musd') - F.col('budget_musd')) # column for profit
    kpi_results['highest_profit'] = get_topmost(rank_movies_with_col(df, 'profit_musd'), 'title')
    kpi_results['lowest_profit'] = get_topmost(rank_movies_with_col(df, 'profit_musd', asc=True), 'title')

    #movie with the highest vote count
    kpi_results['most_voted'] = get_topmost(rank_movies_with_col(df, 'vote_count'), 'title')

     #movie with the highest popularity
    kpi_results['most_pupular'] = get_topmost(rank_movies_with_col(df, 'popularity'), 'title')
 
    #movie with the highest and lowest roi
    filtered_df = cal_roi(df) # generate a df with roi column 
    kpi_results['highest_roi'] = get_topmost(rank_movies_with_col(filtered_df, 'roi_musd'), 'title')
    kpi_results['lowest_roi'] = get_topmost(rank_movies_with_col(filtered_df, 'roi_musd', asc=True), 'title')

    #movie with the highest and lowest profit
    filtered_df = df.filter(F.col('vote_count') >= 10)
    kpi_results['highest_rated'] = get_topmost(rank_movies_with_col(filtered_df, 'vote_average'), 'title')
    kpi_results['lowest_rated'] = get_topmost(rank_movies_with_col(filtered_df, 'vote_average', asc=True), 'title')

    return kpi_results


def get_best_scifi_action_movies_with_actor(df, actor_name='Bruce Willis'):
    """
    This function filters movies that belong to both 'Science Fiction' and 'Action' genres
    and feature a specific actor (default is 'Bruce Willis').
    It then ranks them by 'vote_average' in descending order.
    Parameters:
    - df: The input Spark DataFrame containing movie data.
    - actor_name: The name of the actor to filter by (default: 'Bruce Willis').
    Returns a DataFrame with the title, genres, and vote_average of the top-rated filtered movies.
    """
    genre_filter = (
        F.col('genres').contains('Science Fiction') &
        F.col('genres').contains('Action')
    )

    actor_filter = F.col('cast').contains(actor_name)
    return rank_movies_with_col(df.filter(genre_filter & actor_filter), 'vote_average').select('title', 'genres', 'vote_average')

def get_starring_directed_by(df, starring, directed_by):
    """
    This function filters movies that feature *all* actors in the `starring` list
    and are directed by the specified `directed_by` director.
    It then ranks the filtered movies by their 'runtime' in descending order.
    Parameters:
    - df: The input Spark DataFrame containing movie data.
    - starring: A list of actor names to check in the 'cast' field.
    - directed_by: The director's name to match in the 'director' field.
    Returns a DataFrame with the title, cast, director, runtime, and vote_average
    of the matching movies sorted by runtime.
    """
    starring_filter = F.lit(True)
    for star in starring:
       starring_filter = starring_filter & F.col('cast').contains(star)

    director_filter = F.col('director') == directed_by
    
    return rank_movies_with_col(df.filter(starring_filter & director_filter), 'runtime').select('title', 'cast', 'director', 'runtime', 'vote_average')

##Comparing interms of mean revenue
def frachise_vrs_standalone(df):
    df = df.withColumn(
    "collection_type",
    F.when(F.col("belongs_to_collection").isNull(), "Standalone").otherwise("Franchise"))
    
    df = df.withColumn('roi_musd',  F.col('revenue_musd')/F.col('budget_musd'))
    return df.groupby('collection_type').agg(
        F.mean('revenue_musd').alias('mean_revenue_musd'),
        F.mean('id').alias('movie_count'),
        F.mean('budget_musd').alias('mean_budget_musd'),
        F.mean('popularity').alias('mean_popularity'),
        F.mean('vote_average').alias('mean_rating'),
        F.median('roi_musd').alias('median_roi_musd'))



def most_succesful_franchises(df):
    result = {}

    # Based on total number of movies in franchise
    most_successfull_in_franchise_by_total_movies = (
        df.groupby('belongs_to_collection').agg(
        F.count('id').alias('movies_count')
    ))
    result['most_successfull_in_franchise_by_total_movies'] =get_topmost(rank_movies_with_col(most_successfull_in_franchise_by_total_movies, 'movies_count'), 'belongs_to_collection')
    
    # Based on total number of movies and mean budget in franchise
    most_successfull_in_franchise_by_mean_budget = (
        df.groupby('belongs_to_collection').agg(
            F.count('id').alias('total_movies'),
            F.mean('budget_musd').alias('mean_budget_musd')
        ))
    result['most_successfull_in_franchise_by_mean_budget'] = get_topmost(rank_movies_with_col(most_successfull_in_franchise_by_mean_budget, ['total_movies', 'mean_budget_musd']), 'belongs_to_collection')


    # Based on total number of movies and mean revenue in franchise
    most_successfull_in_franchise_by_mean_revenue = (
        df.groupby('belongs_to_collection')
        .agg(
             F.count('id').alias('total_movies'),
            F.mean('revenue_musd').alias('mean_revenue_musd')
        ))    
    result['most_successfull_in_franchise_by_mean_revenue'] = get_topmost(rank_movies_with_col(most_successfull_in_franchise_by_mean_revenue, ['total_movies', 'mean_revenue_musd']), 'belongs_to_collection')
  

    # Based on mean rating in franchise
    most_successfull_in_franchise_by_mean_average = (
        df.groupby('belongs_to_collection')
        .agg(F.mean('vote_average').alias('mean_average'))
    )
    result['most_successfull_in_franchise_by_mean_average'] = get_topmost(rank_movies_with_col(most_successfull_in_franchise_by_mean_average, 'mean_average'), 'belongs_to_collection')

    return result
