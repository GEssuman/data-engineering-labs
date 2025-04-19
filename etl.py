import os
import pandas as pd
from utils import *
import sys
sys.setrecursionlimit(10000) 
# Constants
BASE_URL = "https://api.themoviedb.org/3/movie/"
API_ACCESS_TOKEN = os.environ.get('API_ACCESS_TOKEN')
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_ACCESS_TOKEN}"
}
MOVIE_IDS = [0, 299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445,181808, 330457, 351286, 109445, 321612, 260513]
INTERNET_CONNECTION = False



def extract():
    if INTERNET_CONNECTION:
        movies_df, credits_df = get_all_data(BASE_URL, MOVIE_IDS, HEADERS)
        movies_df.to_json('./datasets/movies_raw_data.json')
        credits_df.to_json('./datasets/credits_raw_data.json')
    else:
        try:
            movies_df = pd.read_json('./datasets/movies_raw_data.json')
            credits_df = pd.read_json('./datasets/credits_raw_data.json')
        except FileExistsError:
            raise "Files don't exist"
    return movies_df, credits_df


def transform(movies_df, credits_df):
    columns_to_drop = ['adult', 'original_title', 'imdb_id', 'video', 'homepage']
    movies_df = drop_cols(movies_df, columns_to_drop)
    movies_df = eval_movies_json_col(movies_df)
    credits_df = eval_credits_json_col(credits_df)
    credits_df = drop_cols(credits_df, 'crew')
    combined_df = merge_dfs(movies_df, credits_df, 'id')
    
    combined_df = convert_datatypes(combined_df)
    combined_df = replace_with_nan(combined_df, ['budget', 'revenue', 'runtime'])
    combined_df = replace_known_placeholders(combined_df, ['overview', 'tagline'])
    combined_df = convert_to_milions(combined_df, ['revenue', 'budget'])

    # Data cleaning
    combined_df.drop_duplicates(inplace=True, ignore_index=True)
    combined_df.dropna(how='any', subset=['id', 'title'], ignore_index=True, inplace=True)
    combined_df.dropna(thresh=10, ignore_index=True, inplace=True)
    combined_df = combined_df[movies_df['status'] == 'Released']
    combined_df = drop_cols(combined_df, 'status')

    # Vote fixing
    combined_df = replace_zero_count_vote(combined_df)
    reordered_df = reorder_col_and_reindex(combined_df)
    return reordered_df



def load(df):
    df.to_csv('./datasets/final_movie_data.csv', index=False)
    print("Data successfully loaded.")



if __name__ == '__main__':
    print(f"Extracting Data...")
    movies_df, credits_df = extract()
    print(f"Transforming Data...")
    transformed_df = transform(movies_df, credits_df)
    print(f"Loading Data...")
    load(transformed_df)
