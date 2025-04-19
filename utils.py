import requests
import pandas as pd
import numpy as np


def fetch_from_api(url, headers):
    """
    Fetches JSON data from a specified API endpoint.
    Returns the JSON response if the status is 200 (OK), otherwise logs an error and returns None.
    """
    api_response = requests.get(url, headers)
    if api_response.status_code == 200:
        print(f"{url}: Success")
        return api_response.json()
    else:
        print(f"Error occured trying to fetch from api route: {url}. \nError Status Code: {api_response.status_code}")
        return None
    
def get_all_data(base_url, movie_ids, headers):
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
    
    movies_df = convert_to_dataframe(movies)
    credits_df = convert_to_dataframe(credits)


    return movies_df, credits_df


def convert_to_dataframe(json_data):
    """
    Converts a list of JSON objects into a Pandas DataFrame.
    """
    return pd.DataFrame(json_data)

def join_json_key_value(df, col, sep, is_list, key):
    """
    Extracts and joins values from JSON-like columns into a string using a separator.
    Handles both list of dicts and single dicts.
    """
    if df is None:
        raise ValueError("Input DataFrame is None.")
    try:
        if is_list:
            df[col] = df[col].apply(lambda list: sep.join([element[key] for element in list]))
        else:
            df[col] = df[col].apply(lambda struct: struct[key] if isinstance(struct, dict) else None )
        return df

    except Exception as e:
        print(f'Error: {e}')
        return pd.DataFrame()
    

def eval_movies_json_col(df):
    """
    Processes movie-specific JSON-like columns:
    - Flattens nested data such as genres, languages, countries, and collections into string format.
    - Converts origin_country list to string.
    """
    cols = ['genres', 'production_companies', 'production_countries', 'spoken_languages', 'belongs_to_collection']
    for col in cols:
       if col == 'spoken_languages':
           df = join_json_key_value(df, col, '|', True, 'english_name')
       elif col == 'belongs_to_collection':
           df = join_json_key_value(df, col, '|', False, 'name') 
       else:
            df = join_json_key_value(df, col, '|', True, 'name') 

    df = stringify_list(df, 'origin_country')
    return df

def stringify_list(df, col):
    """
    Simplifies list-type columns by keeping only the first element.
    """
    df[col] = df[col].apply(lambda content: content[0] if isinstance(content, list) else None)
    return df

# #evaluating json-like columns in credits_df
def eval_credits_json_col(df):
    """
    Processes credit-related JSON-like columns:
    - Joins cast names into a string.
    - Extracts director name and calculates crew/cast sizes.
    """
    join_json_key_value(df, 'cast', '|', True, 'name')

    df['cast_size'] = df['cast'].apply(lambda cast_list: len(cast_list))
    df['director'] = df['crew'].apply(lambda crew_list:  next((crew['name'] for crew in crew_list if crew['job'] =='Director'), None))
    df['crew_size'] = df['crew'].apply(lambda crew_list: len(crew_list))

    return df

# Coverting columns to specific datatypes
def convert_datatypes(df):
    """
    Converts specific columns to appropriate data types, including datetime and numeric types.
    """
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')

    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    return df


def drop_cols(df, cols):
    """
    Drops specified columns from the DataFrame.
    """
    df_copy = df.copy()
    df_copy = df_copy.drop(cols, axis=1)
    return df_copy

def merge_dfs(df_1, df_2, on, how=None):
    """
    Merges two DataFrames on a specified column.
    Optional 'how' parameter defines merge type.
    """
    if how:
        df = df_1.merge(df_2, on=on, how=how)
    else:
        df = df_1.merge(df_2, on=on)
    return df

def replace_with_nan(df, cols):
    """
    Replaces zero values in specified columns with NaN for cleaner analysis.
    """
    df[cols] = df[cols].replace(0, pd.NA)
    return df

def replace_known_placeholders(df, cols):
    """
    Replaces placeholder text like 'No Data' with NaN in specified columns.
    """
    df[cols] = df[cols].replace('No Data', pd.NA)
    return df

def convert_to_milions(df, cols):
    """
    Converts monetary values to millions and renames the columns with '_musd' suffix.
    """
    for col in cols:
        df[col] = df[col]/1000000 
        col_rename = f"{col}_musd"
        df.rename({col:col_rename}, axis=1, inplace=True)
    return df
        
# def rank_movies_with_col(df, col, asc=False):
#     return df.sort_values(col, ascending=asc, ignore_index=True)['title']
def rank_movies_with_col(df, col, asc=False):
    """
    Sorts the DataFrame based on a specific column to rank movies.
    Allows ascending/descending order.
    """
    return df.sort_values(col, ascending=asc, ignore_index=True)

def cal_roi(df):
    """
    Calculates Return on Investment (ROI) for movies with a budget ≥ $10M.
    Adds a new column 'roi_musd' to the DataFrame.
    """
    filtered_df = df[df['budget_musd']>=10].copy()
    filtered_df['roi_musd'] = filtered_df['revenue_musd']/filtered_df['budget_musd']
    return filtered_df

def replace_zero_count_vote(df):
    """
    Fills in missing or zero vote_count and vote_average using genre-wise averages.
    Helps in dealing with sparse rating data.
    """
    df['genres_list'] = df['genres'].str.split('|')
    df_exploded = df.explode('genres_list')
    df_exploded_valid_votes = df_exploded.dropna(subset=['vote_count', 'vote_average'])
    genres_stats = df_exploded_valid_votes.groupby('genres_list').agg({
        'vote_count': 'mean',
        'vote_average': 'mean'
    }).rename(columns={'vote_count': "genre_vote_count", "vote_average": "genre_vote_average"}).reset_index().sort_values('genre_vote_average', ascending=False)
    df_exploded =  merge_dfs(df_exploded, genres_stats, 'genres_list', how='left')

    filled_votes = df_exploded.groupby('id').agg({
        "genre_vote_count": "mean",
        "genre_vote_average": "mean"
    }).rename(columns={"genre_vote_count": "filled_vote_count", "genre_vote_average":"filled_vote_average"})
  
    df = merge_dfs(df, filled_votes, 'id', how='left')
    df['vote_average'] = df['vote_average'].fillna(df['filled_vote_average']).astype(float)
    df['vote_count'] = df['vote_count'].fillna(df['filled_vote_count']).astype(int)
    df['vote_count'] = np.where(df['vote_count']==0, df['filled_vote_count'], df['vote_count'])
    
    df = drop_cols(df, ["filled_vote_count","genres_list", "filled_vote_average"])
    return df


def reorder_col_and_reindex(df):
    """
    Reorders the DataFrame columns to a preferred structure and resets the index.
    """
    reordered_df = df.reindex(['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection', 'original_language',
                                    'budget_musd', 'revenue_musd', 'production_companies','production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
    'overview', 'spoken_languages', 'poster_path','cast', 'cast_size', 'director', 'crew_size'], axis=1)
    reordered_df.reset_index(drop=True, inplace=True)
    return reordered_df