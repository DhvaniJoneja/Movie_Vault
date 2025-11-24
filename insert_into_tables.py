import pandas as pd
import mysql.connector as myconnect
from mysql.connector import Error


def to_int(x):
    try:
        if pd.isna(x) or x == "":
            return None
        return int(float(x))
    except Exception:
        return None

def to_float(x):
    try:
        if pd.isna(x) or x == "":
            return None
        return float(x)
    except Exception:
        return None

def to_adult_str(x):
    if pd.isna(x) or x == "":
        return "False" 
    
    s = str(x).strip().lower()
    return "True" if s == "true" else "False"

def to_date(x):
    if pd.isna(x) or x == "":
        return None
    try:
        return pd.to_datetime(x, errors='coerce').date()
    except Exception:
        return None

def split_list_field(x):
    if pd.isna(x) or x == "":
        return []
    items = [s.strip() for s in str(x).split(",") if s.strip() != ""]
    return items

def load_and_clean_csv(path):
    df = pd.read_csv(path, quotechar='"', skipinitialspace=True, nrows=10000)
    df = df.drop(columns=["backdrop_path", "homepage", "tagline", "original_title", "overview", "poster_path", "production_companies", "production_countries", "keywords"], errors="ignore")

    df['id'] = df['id'].astype(str).str.strip()  
    df['id'] = df['id'].apply(lambda x: to_int(x))

    df['title'] = df['title'].replace({pd.NA: None, "": None}).astype(object)
    df['imdb_id'] = df.get('imdb_id', pd.Series([None]*len(df)))
    df['original_title'] = df.get('original_title', pd.Series([None]*len(df)))
    df['original_language'] = df.get('original_language', pd.Series([None]*len(df)))

    df['vote_average'] = df['vote_average'].apply(to_float)
    df['vote_count'] = df['vote_count'].apply(to_int)
    df['popularity'] = df['popularity'].apply(to_float)
    df['runtime'] = df['runtime'].apply(to_int)
    df['budget'] = df['budget'].apply(to_int)
    df['revenue'] = df['revenue'].apply(to_int)

    df['adult'] = df['adult'].apply(to_adult_str)

    df['release_date'] = df['release_date'].apply(to_date)

    df['genres_list'] = df['genres'].apply(split_list_field)
    df['spoken_languages_list'] = df['spoken_languages'].apply(split_list_field)

    return df

def insert_from_df(conn, df, batch_size=100):
    cursor = conn.cursor()
    try:
        movie_info_stmt = """
            INSERT INTO MOVIE_INFO (ID, TITLE)
            VALUES (%s,%s)
        """
        movie_info_data = []
        for _, row in df.iterrows():
            movie_info_data.append((
                row['id'],
                row['title']
            ))

        for i in range(0, len(movie_info_data), batch_size):
            cursor.executemany(movie_info_stmt, movie_info_data[i:i+batch_size])

        pop_stmt = """
            INSERT INTO MOVIE_POPULARITY (M_ID, VOTE_AVG, VOTE_COUNT, POPULARITY, RUNTIME)
            VALUES (%s,%s,%s,%s,%s)
        """
        pop_data = [
            (row['id'], row['vote_average'], row['vote_count'], row['popularity'], row['runtime'])
            for _, row in df.iterrows()
        ]
        for i in range(0, len(pop_data), batch_size):
            cursor.executemany(pop_stmt, pop_data[i:i+batch_size])

        status_stmt = """
            INSERT INTO MOVIE_STATUS (M_ID, STATUS, RELEASE_DATE)
            VALUES (%s,%s,%s)
        """
        status_data = [
            (row['id'], row.get('status'), row.get('release_date'))
            for _, row in df.iterrows()
        ]
        for i in range(0, len(status_data), batch_size):
            cursor.executemany(status_stmt, status_data[i:i+batch_size])

        category_stmt = """
            INSERT INTO MOVIE_CATEGORY (M_ID, ADULT, ORIGINAL_LANG)
            VALUES (%s,%s,%s)
        """
        category_data = [
            (row['id'], row.get('adult'), row.get('original_language'))
            for _, row in df.iterrows()
        ]
        for i in range(0, len(category_data), batch_size):
            cursor.executemany(category_stmt, category_data[i:i+batch_size])

        econ_stmt = """
            INSERT INTO MOVIE_ECONOMICS (M_ID, BUDGET, REVENUE)
            VALUES (%s,%s,%s)
        """
        econ_data = [
            (row['id'], row.get('budget'), row.get('revenue'))
            for _, row in df.iterrows()
        ]
        for i in range(0, len(econ_data), batch_size):
            cursor.executemany(econ_stmt, econ_data[i:i+batch_size])

        genre_stmt = """
            INSERT INTO MOVIE_GENRE (M_ID, GENRE) VALUES (%s,%s)
        """
        genre_rows = []
        for _, row in df.iterrows():
            mid = row['id']
            for g in row['genres_list']:
                genre_rows.append((mid, g))
        if genre_rows:
            for i in range(0, len(genre_rows), batch_size):
                cursor.executemany(genre_stmt, genre_rows[i:i+batch_size])

        sp_stmt = """
            INSERT INTO MOVIE_SPOKEN_LANG (M_ID, SP_LANG) VALUES (%s,%s)
            ON DUPLICATE KEY UPDATE SP_LANG=SP_LANG
        """
        sp_rows = []
        for _, row in df.iterrows():
            mid = row['id']
            for s in row['spoken_languages_list']:
                sp_rows.append((mid, s))
        if sp_rows:
            for i in range(0, len(sp_rows), batch_size):
                cursor.executemany(sp_stmt, sp_rows[i:i+batch_size])


        conn.commit()
        print("All inserts committed successfully.")
    except Error as e:
        conn.rollback()
        print("Error during insert, rolled back. Error:", e)
    finally:
        cursor.close()