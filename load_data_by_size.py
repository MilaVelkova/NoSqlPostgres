import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from ast import literal_eval
import sys

"""
Script to load a specific number of rows from the movie dataset.
Usage: python load_data_by_size.py <number_of_rows>
Example: python load_data_by_size.py 15000
"""

def safe_list(val):
    """Helper to safely parse lists"""
    if val is None or pd.isna(val):
        return []
    try:
        return literal_eval(val) if isinstance(val, str) else []
    except:
        return []


def clear_database(conn):
    """Clear all data from the database"""
    cur = conn.cursor()
    
    print("Clearing existing data...")
    
    # Drop relation tables first (due to foreign keys)
    cur.execute("TRUNCATE TABLE movie_people, movie_genres, movie_keywords, movie_companies, movie_countries, movie_languages CASCADE;")
    
    # Drop lookup tables
    cur.execute("TRUNCATE TABLE people, genres, keywords, companies, countries, languages CASCADE;")
    
    # Drop movies table
    cur.execute("TRUNCATE TABLE movies CASCADE;")
    
    conn.commit()
    cur.close()
    print("✓ Database cleared")


def load_movies(df, conn):
    """Load movies into the database"""
    cur = conn.cursor()
    
    movie_sql = """
    INSERT INTO movies (
        id, title, vote_average, vote_count, status, release_date, release_year,
        runtime, budget, revenue, adult, backdrop_path, homepage, imdb_id,
        original_language, original_title, overview, popularity, poster_path,
        tagline, overview_sentiment, AverageRating, Poster_Link, Certificate,
        IMDB_Rating, Meta_score
    )
    VALUES (
        %(id)s, %(title)s, %(vote_average)s, %(vote_count)s, %(status)s,
        %(release_date)s, %(release_year)s, %(runtime)s, %(budget)s, %(revenue)s,
        %(adult)s, %(backdrop_path)s, %(homepage)s, %(imdb_id)s,
        %(original_language)s, %(original_title)s, %(overview)s, %(popularity)s,
        %(poster_path)s, %(tagline)s, %(overview_sentiment)s, %(AverageRating)s,
        %(Poster_Link)s, %(Certificate)s, %(IMDB_Rating)s, %(Meta_score)s
    )
    ON CONFLICT (id) DO NOTHING;
    """
    
    movies_data = df[[
        "id", "title", "vote_average", "vote_count", "status", "release_date",
        "release_year", "runtime", "budget", "revenue", "adult", "backdrop_path",
        "homepage", "imdb_id", "original_language", "original_title", "overview",
        "popularity", "poster_path", "tagline", "overview_sentiment",
        "AverageRating", "Poster_Link", "Certificate", "IMDB_Rating", "Meta_score"
    ]].to_dict(orient="records")
    
    execute_batch(cur, movie_sql, movies_data, page_size=500)
    conn.commit()
    cur.close()
    print(f"✓ {len(movies_data)} Movies inserted")


def load_people(df, conn):
    """Load people into the database"""
    cur = conn.cursor()
    
    all_people = set()
    
    for _, row in df.iterrows():
        # Single-name fields
        for p in [
            row.get("director"), row.get("writer"), row.get("music_composer"),
            row.get("director_of_photography"), row.get("star1"), row.get("star2"),
            row.get("star3"), row.get("star4")
        ]:
            if p and isinstance(p, str):
                all_people.add(p.strip())
        
        # Producers (comma-separated)
        producers = row.get("producers")
        if producers and isinstance(producers, str):
            for prod in producers.split(','):
                all_people.add(prod.strip())
        
        # Cast list
        cast = safe_list(row.get("cast_members"))
        for actor in cast:
            all_people.add(actor.strip())
    
    people_data = [{"name": p} for p in all_people]
    
    people_sql = """
    INSERT INTO people (name)
    VALUES (%(name)s)
    ON CONFLICT (name) DO NOTHING;
    """
    
    execute_batch(cur, people_sql, people_data, page_size=500)
    conn.commit()
    
    # Load back people IDs
    cur.execute("SELECT id, name FROM people;")
    people_map = {name: pid for pid, name in cur.fetchall()}
    
    cur.close()
    print(f"✓ {len(people_data)} People inserted")
    
    return people_map


def load_movie_people_relations(df, people_map, conn):
    """Load movie-people relationships"""
    cur = conn.cursor()
    
    relations = []
    
    for _, row in df.iterrows():
        movie_id = row["id"]
        
        roles = {
            "director": row.get("director"),
            "writer": row.get("writer"),
            "composer": row.get("music_composer"),
            "directory_of_photography": row.get("director_of_photography"),
            "star": row.get("star1"),
            "star2": row.get("star2"),
            "star3": row.get("star3"),
            "star4": row.get("star4"),
        }
        
        for role, person in roles.items():
            if person and person in people_map:
                relations.append({
                    "movie_id": movie_id,
                    "person_id": people_map[person],
                    "role": role
                })
        
        # Cast list
        cast = safe_list(row.get("cast_members"))
        for actor in cast:
            if actor in people_map:
                relations.append({
                    "movie_id": movie_id,
                    "person_id": people_map[actor],
                    "role": "actor"
                })
        
        # Producers
        producers = row.get("producers")
        if producers and isinstance(producers, str):
            for prod in producers.split(","):
                prod = prod.strip()
                if prod in people_map:
                    relations.append({
                        "movie_id": movie_id,
                        "person_id": people_map[prod],
                        "role": "producer"
                    })
    
    rel_sql = """
    INSERT INTO movie_people (movie_id, person_id, role)
    VALUES (%(movie_id)s, %(person_id)s, %(role)s)
    ON CONFLICT DO NOTHING;
    """
    
    execute_batch(cur, rel_sql, relations, page_size=1000)
    conn.commit()
    cur.close()
    print(f"✓ {len(relations)} Movie-People relations inserted")


def load_genres(df, conn):
    """Load genres into the database"""
    cur = conn.cursor()
    
    all_genres = set()
    
    for g_list in df["genres_list"]:
        for g in safe_list(g_list):
            all_genres.add(g)
    
    genre_data = [{"name": g} for g in all_genres]
    
    genre_sql = """
    INSERT INTO genres (name)
    VALUES (%(name)s)
    ON CONFLICT (name) DO NOTHING;
    """
    
    execute_batch(cur, genre_sql, genre_data)
    conn.commit()
    
    # Load back genres
    cur.execute("SELECT id, name FROM genres;")
    genre_map = {name: gid for gid, name in cur.fetchall()}
    
    cur.close()
    print(f"✓ {len(genre_data)} Genres inserted")
    
    return genre_map


def load_movie_genre_relations(df, genre_map, conn):
    """Load movie-genre relationships"""
    cur = conn.cursor()
    
    movie_genre_rel = []
    
    for _, row in df.iterrows():
        movie_id = row["id"]
        genres = safe_list(row.get("genres_list"))
        
        for g in genres:
            if g in genre_map:
                movie_genre_rel.append({
                    "movie_id": movie_id,
                    "genre_id": genre_map[g]
                })
    
    mg_sql = """
    INSERT INTO movie_genres (movie_id, genre_id)
    VALUES (%(movie_id)s, %(genre_id)s)
    ON CONFLICT DO NOTHING;
    """
    
    execute_batch(cur, mg_sql, movie_genre_rel)
    conn.commit()
    cur.close()
    print(f"✓ {len(movie_genre_rel)} Movie-Genre relations inserted")


def load_keywords(df, conn):
    """Load keywords into the database"""
    cur = conn.cursor()
    
    keywords = set()
    for kw_list in df["keywords"]:
        for kw in safe_list(kw_list):
            keywords.add(kw)
    
    keyword_data = [{"name": kw} for kw in keywords]
    
    keyword_sql = """
    INSERT INTO keywords (name)
    VALUES (%(name)s)
    ON CONFLICT (name) DO NOTHING;
    """
    execute_batch(cur, keyword_sql, keyword_data)
    conn.commit()
    
    # Load back keyword IDs
    cur.execute("SELECT id, name FROM keywords;")
    keyword_map = {name: kid for kid, name in cur.fetchall()}
    
    cur.close()
    print(f"✓ {len(keyword_data)} Keywords inserted")
    
    return keyword_map


def load_movie_keyword_relations(df, keyword_map, conn):
    """Load movie-keyword relationships"""
    cur = conn.cursor()
    
    movie_keyword_rel = []
    for _, row in df.iterrows():
        movie_id = row["id"]
        keywords = safe_list(row.get("keywords"))
        for kw in keywords:
            if kw in keyword_map:
                movie_keyword_rel.append({
                    "movie_id": movie_id,
                    "keyword_id": keyword_map[kw]
                })
    
    mk_sql = """
    INSERT INTO movie_keywords (movie_id, keyword_id)
    VALUES (%(movie_id)s, %(keyword_id)s)
    ON CONFLICT DO NOTHING;
    """
    execute_batch(cur, mk_sql, movie_keyword_rel)
    conn.commit()
    cur.close()
    print(f"✓ {len(movie_keyword_rel)} Movie-Keyword relations inserted")


def load_companies(df, conn):
    """Load production companies into the database"""
    cur = conn.cursor()
    
    all_companies = set()
    for comp_list in df["production_companies"]:
        if isinstance(comp_list, str):
            for c in comp_list.split(","):
                all_companies.add(c.strip())
    
    company_data = [{"name": c} for c in all_companies]
    
    company_sql = """
    INSERT INTO companies (name)
    VALUES (%(name)s)
    ON CONFLICT (name) DO NOTHING;
    """
    execute_batch(cur, company_sql, company_data)
    conn.commit()
    
    # Load back company IDs
    cur.execute("SELECT id, name FROM companies;")
    company_map = {name: cid for cid, name in cur.fetchall()}
    
    cur.close()
    print(f"✓ {len(company_data)} Companies inserted")
    
    return company_map


def load_movie_company_relations(df, company_map, conn):
    """Load movie-company relationships"""
    cur = conn.cursor()
    
    movie_company_rel = []
    for _, row in df.iterrows():
        movie_id = row["id"]
        comp_list = row.get("production_companies")
        if isinstance(comp_list, str):
            for c in comp_list.split(","):
                c = c.strip()
                if c in company_map:
                    movie_company_rel.append({
                        "movie_id": movie_id,
                        "company_id": company_map[c]
                    })
    
    mc_sql = """
    INSERT INTO movie_companies (movie_id, company_id)
    VALUES (%(movie_id)s, %(company_id)s)
    ON CONFLICT DO NOTHING;
    """
    execute_batch(cur, mc_sql, movie_company_rel)
    conn.commit()
    cur.close()
    print(f"✓ {len(movie_company_rel)} Movie-Company relations inserted")


def load_countries(df, conn):
    """Load production countries into the database"""
    cur = conn.cursor()
    
    all_countries = set()
    for c_list in df["production_countries"]:
        if isinstance(c_list, str):
            countries = safe_list(c_list)
            if not countries:
                countries = [x.strip() for x in c_list.split(",") if x.strip()]
            for c in countries:
                all_countries.add(c.strip())
    
    country_data = [{"name": c} for c in all_countries]
    
    country_sql = """
    INSERT INTO countries (name)
    VALUES (%(name)s)
    ON CONFLICT (name) DO NOTHING;
    """
    execute_batch(cur, country_sql, country_data)
    conn.commit()
    
    # Load back country IDs
    cur.execute("SELECT id, name FROM countries;")
    country_map = {name: cid for cid, name in cur.fetchall()}
    
    cur.close()
    print(f"✓ {len(country_data)} Countries inserted")
    
    return country_map


def load_movie_country_relations(df, country_map, conn):
    """Load movie-country relationships"""
    cur = conn.cursor()
    
    movie_country_rel = []
    for _, row in df.iterrows():
        movie_id = row["id"]
        c_list = row.get("production_countries")
        if isinstance(c_list, str):
            countries = safe_list(c_list)
            if not countries:
                countries = [x.strip() for x in c_list.split(",") if x.strip()]
            for c in countries:
                if c in country_map:
                    movie_country_rel.append({
                        "movie_id": movie_id,
                        "country_id": country_map[c]
                    })
    
    mcou_sql = """
    INSERT INTO movie_countries (movie_id, country_id)
    VALUES (%(movie_id)s, %(country_id)s)
    ON CONFLICT DO NOTHING;
    """
    execute_batch(cur, mcou_sql, movie_country_rel)
    conn.commit()
    cur.close()
    print(f"✓ {len(movie_country_rel)} Movie-Country relations inserted")


def load_languages(df, conn):
    """Load spoken languages into the database"""
    cur = conn.cursor()
    
    all_languages = set()
    for l_list in df["spoken_languages"]:
        if isinstance(l_list, str):
            langs = safe_list(l_list)
            if not langs:
                langs = [x.strip() for x in l_list.split(",") if x.strip()]
            for l in langs:
                all_languages.add(l.strip())
    
    language_data = [{"name": l} for l in all_languages]
    
    language_sql = """
    INSERT INTO languages (name)
    VALUES (%(name)s)
    ON CONFLICT (name) DO NOTHING;
    """
    execute_batch(cur, language_sql, language_data)
    conn.commit()
    
    # Load back language IDs
    cur.execute("SELECT id, name FROM languages;")
    language_map = {name: lid for lid, name in cur.fetchall()}
    
    cur.close()
    print(f"✓ {len(language_data)} Languages inserted")
    
    return language_map


def load_movie_language_relations(df, language_map, conn):
    """Load movie-language relationships"""
    cur = conn.cursor()
    
    movie_language_rel = []
    for _, row in df.iterrows():
        movie_id = row["id"]
        l_list = row.get("spoken_languages")
        if isinstance(l_list, str):
            langs = safe_list(l_list)
            if not langs:
                langs = [x.strip() for x in l_list.split(",") if x.strip()]
            for l in langs:
                if l in language_map:
                    movie_language_rel.append({
                        "movie_id": movie_id,
                        "language_id": language_map[l]
                    })
    
    ml_sql = """
    INSERT INTO movie_languages (movie_id, language_id)
    VALUES (%(movie_id)s, %(language_id)s)
    ON CONFLICT DO NOTHING;
    """
    execute_batch(cur, ml_sql, movie_language_rel)
    conn.commit()
    cur.close()
    print(f"✓ {len(movie_language_rel)} Movie-Language relations inserted")


def main(num_rows):
    """Main function to load specified number of rows"""
    
    print(f"\n{'='*80}")
    print(f"LOADING {num_rows} ROWS INTO DATABASE")
    print(f"{'='*80}\n")
    
    # Load CSV
    print(f"Loading CSV (first {num_rows} rows)...")
    df = pd.read_csv(
        "IMDB TMDB Movie Metadata Big Dataset (1M).csv",
        low_memory=False,
        nrows=num_rows
    )
    
    df = df.dropna(subset=["id", "title"])
    
    # Normalize column names
    df = df.rename(columns={
        "Director": "director",
        "Star1": "star1",
        "Star2": "star2",
        "Star3": "star3",
        "Star4": "star4",
        "Writer": "writer",
        "Music_Composer": "music_composer",
        "Cast_list": "cast_members",
        "genres_list": "genres_list",
        "Director_of_Photography": "director_of_photography",
        "Producers": "producers"
    })
    
    # Parse release_year
    df['release_year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
    
    print(f"✓ Loaded {len(df)} rows from CSV\n")
    
    # Connect to database
    conn = psycopg2.connect(
        dbname="movie_db",
        user="user",
        password="password",
        host="localhost",
        port=5434
    )
    
    # Clear existing data
    clear_database(conn)
    
    # Load data in order
    print("\nLoading data into database...")
    load_movies(df, conn)
    people_map = load_people(df, conn)
    load_movie_people_relations(df, people_map, conn)
    genre_map = load_genres(df, conn)
    load_movie_genre_relations(df, genre_map, conn)
    keyword_map = load_keywords(df, conn)
    load_movie_keyword_relations(df, keyword_map, conn)
    company_map = load_companies(df, conn)
    load_movie_company_relations(df, company_map, conn)
    country_map = load_countries(df, conn)
    load_movie_country_relations(df, country_map, conn)
    language_map = load_languages(df, conn)
    load_movie_language_relations(df, language_map, conn)
    
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"✓ ALL {num_rows} ROWS SUCCESSFULLY LOADED!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("\nUsage: python load_data_by_size.py <number_of_rows>")
        print("\nExamples:")
        print("  python load_data_by_size.py 5000")
        print("  python load_data_by_size.py 15000")
        print("  python load_data_by_size.py 30000")
        print("  python load_data_by_size.py 50000")
        sys.exit(1)
    
    try:
        num_rows = int(sys.argv[1])
        if num_rows <= 0:
            print("Error: Number of rows must be positive!")
            sys.exit(1)
        
        main(num_rows)
    except ValueError:
        print("Error: Please provide a valid number!")
        sys.exit(1)




