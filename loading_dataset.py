import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

# -------------------------------------
# 1. Load CSV
# -------------------------------------
df = pd.read_csv(
    "IMDB TMDB Movie Metadata Big Dataset (1M).csv",
    low_memory=False,
    nrows=5000
)

# Drop rows missing critical fields
df = df.dropna(subset=["id", "title"])

# -------------------------------------
# 2. Rename columns to match PostgreSQL
# -------------------------------------
column_map = {
    "Director": "director",
    "Star1": "star1",
    "Star2": "star2",
    "Star3": "star3",
    "Star4": "star4",
    "Writer": "writer",
    "Director_of_Photography": "director_of_photography",
    "Producers": "producers",
    "Music_Composer": "music_composer",
    "Cast_list": "cast_members",
    "Poster_Link": "poster_path"
}

df = df.rename(columns=column_map)

# -------------------------------------
# 3. Prepare expected PostgreSQL columns
# -------------------------------------
expected_columns = [
    "id","title","vote_average","vote_count","status","release_date",
    "revenue","runtime","adult","genres","overview_sentiment",
    "cast_members","crew_members","genres_list","keywords",
    "director_of_photography","producers","music_composer","star1","star2",
    "star3","star4","writer","original_language","original_title",
    "popularity","budget","tagline","production_companies",
    "production_countries","spoken_languages","homepage","imdb_id",
    "tmdb_id","video","poster_path","backdrop_path","release_year",
    "director","certificate","collection_name","collection_id","genres_id",
    "original_language_code","overview","all_combined_keywords"
]

# -------------------------------------
# 4. Add missing columns as None
# -------------------------------------
for col in expected_columns:
    if col not in df.columns:
        df[col] = None

# Keep only expected columns (order matters)
df = df[expected_columns]

# Convert NaN → None
df = df.where(pd.notnull(df), None)

# -------------------------------------
# 5. Connect to PostgreSQL
# -------------------------------------
conn = psycopg2.connect(
    dbname="movie_db",
    user="user",
    password="password",
    host="localhost",
    port=5434
)
cur = conn.cursor()

# -------------------------------------
# 6. Prepare SQL INSERT
# -------------------------------------
sql = """
INSERT INTO movies (
    id, title, vote_average, vote_count, status, release_date,
    revenue, runtime, adult, genres, overview_sentiment,
    cast_members, crew_members, genres_list, keywords,
    director_of_photography, producers, music_composer, star1, star2,
    star3, star4, writer, original_language, original_title,
    popularity, budget, tagline, production_companies,
    production_countries, spoken_languages, homepage, imdb_id,
    tmdb_id, video, poster_path, backdrop_path, release_year,
    director, certificate, collection_name, collection_id, genres_id,
    original_language_code, overview, all_combined_keywords
)
VALUES (
    %(id)s, %(title)s, %(vote_average)s, %(vote_count)s, %(status)s, %(release_date)s,
    %(revenue)s, %(runtime)s, %(adult)s, %(genres)s, %(overview_sentiment)s,
    %(cast_members)s, %(crew_members)s, %(genres_list)s, %(keywords)s,
    %(director_of_photography)s, %(producers)s, %(music_composer)s, %(star1)s, %(star2)s,
    %(star3)s, %(star4)s, %(writer)s, %(original_language)s, %(original_title)s,
    %(popularity)s, %(budget)s, %(tagline)s, %(production_companies)s,
    %(production_countries)s, %(spoken_languages)s, %(homepage)s, %(imdb_id)s,
    %(tmdb_id)s, %(video)s, %(poster_path)s, %(backdrop_path)s, %(release_year)s,
    %(director)s, %(certificate)s, %(collection_name)s, %(collection_id)s, %(genres_id)s,
    %(original_language_code)s, %(overview)s, %(all_combined_keywords)s
)
ON CONFLICT (id) DO NOTHING;
"""

# -------------------------------------
# 7. Insert in batches (fast)
# -------------------------------------
execute_batch(
    cur,
    sql,
    df.to_dict(orient="records"),
    page_size=500
)

conn.commit()
cur.close()
conn.close()

print("✅ Data successfully loaded into PostgreSQL!")
