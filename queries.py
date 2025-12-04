import psycopg2
import time

conn = psycopg2.connect(
    dbname="movie_db",
    user="user",
    password="password",
    host="localhost",
    port=5434
)
cur = conn.cursor()

def timed_query(func, runs=10, *args, **kwargs):
    times = []
    results = None
    for _ in range(runs):
        start = time.time()
        results = func(*args, **kwargs)
        times.append(time.time() - start)
    avg_time = sum(times) / len(times)
    print(f"Average execution time over {runs} runs: {avg_time:.6f} seconds\n")
    return results

def run_query(sql, params=None):
    cur.execute(sql, params)
    return cur.fetchall()

def query_by_genre(genre="Action"):
    sql = "SELECT id, title FROM movies WHERE genres_list ILIKE %s;"
    results = run_query(sql, (f"%{genre}%",))
    print(f"[Genre: {genre}] Found {len(results)} movies")
    return results

def query_by_actor(actor="Tom Hanks"):
    sql = ("SELECT id, title FROM movies "
           "WHERE Star1 ILIKE %s OR Star2 ILIKE %s OR Star3 ILIKE %s OR Star4 ILIKE %s;")
    results = run_query(sql, (f"%{actor}%",)*4)
    print(f"[Actor: {actor}] Found {len(results)} movies")
    return results

def query_by_year(year=2015):
    sql = "SELECT id, title FROM movies WHERE release_year = %s;"
    results = run_query(sql, (year,))
    print(f"[Year: {year}] Found {len(results)} movies")
    return results

def query_by_actor_and_genre(actor="Tom Hanks", genre="Drama"):
    sql = ("SELECT id, title FROM movies "
           "WHERE (Star1 ILIKE %s OR Star2 ILIKE %s OR Star3 ILIKE %s OR Star4 ILIKE %s) "
           "AND genres_list ILIKE %s;")
    results = run_query(sql, (f"%{actor}%",)*4 + (f"%{genre}%",))
    print(f"[Actor: {actor} AND Genre: {genre}] Found {len(results)} movies")
    return results

def query_by_genre_and_year(genre="Action", year=2015):
    sql = "SELECT id, title FROM movies WHERE genres_list ILIKE %s AND release_year = %s;"
    results = run_query(sql, (f"%{genre}%", year))
    print(f"[Genre: {genre} AND Year: {year}] Found {len(results)} movies")
    return results

def top_rated_by_genre(genre="Drama", top_n=5):
    sql = "SELECT id, title, vote_average FROM movies WHERE genres_list ILIKE %s ORDER BY vote_average DESC LIMIT %s;"
    results = run_query(sql, (f"%{genre}%", top_n))
    print(f"Top {top_n} rated movies in Genre '{genre}': {[row[1] for row in results]}")
    return results

def top_rated_by_genre_and_year(genre="Romance", year=2019, top_n=3):
    sql = ("SELECT id, title, vote_average FROM movies "
           "WHERE genres_list ILIKE %s AND release_year = %s "
           "ORDER BY vote_average DESC LIMIT %s;")
    results = run_query(sql, (f"%{genre}%", year, top_n))
    print(f"Top {top_n} rated movies in Genre '{genre}' and Year '{year}': {[row[1] for row in results]}")
    return results

def count_movies_by_actor(actor="Leonardo DiCaprio"):
    sql = ("SELECT COUNT(*) FROM movies "
           "WHERE Star1 ILIKE %s OR Star2 ILIKE %s OR Star3 ILIKE %s OR Star4 ILIKE %s;")
    results = run_query(sql, (f"%{actor}%",)*4)
    print(f"Number of movies with actor '{actor}': {results[0][0]}")
    return results[0][0]

def count_high_rated_action_movies(genre="Action" ,min_rating=8.0):
    sql = "SELECT COUNT(*) FROM movies WHERE genres_list ILIKE %s AND vote_average >= %s;"
    results = run_query(sql, (f"%{genre}%", min_rating))
    print(f"Number of Action movies with rating >= {min_rating}: {results[0][0]}")
    return results[0][0]


#10 more complex queries added

def science_fiction_after_2010_with_rating(genre="Science Fiction",min_rating=7.5):
    sql = """
        SELECT COUNT(*) 
        FROM movies 
        WHERE genres_list ILIKE %s
          AND EXTRACT(YEAR FROM release_date) >= 2010
          AND vote_average >= %s;
    """
    results = run_query(sql, (f"%{genre}%", min_rating))
    print(f"Science Fiction movies after 2010 with rating >= {min_rating}: {results[0][0]}")
    return results[0][0]


def drama_or_thriller_long(genre1="Drama", genre2="Thriller",runtime_min=140):
    sql = """
        SELECT COUNT(*)
        FROM movies
        WHERE (genres_list ILIKE %s OR genres_list ILIKE %s)
          AND runtime > %s;
    """
    results = run_query(sql, (f"%{genre1}%", f"%{genre2}%", runtime_min))
    print(f"Drama/Thriller movies longer than {runtime_min} min: {results[0][0]}")
    return results[0][0]


def count_high_rated_per_genre(min_rating=8.0):
    sql = """
        SELECT COUNT(*)
        FROM movies
        WHERE vote_average >= %s;
    """
    results = run_query(sql, (min_rating,))
    print(f"Movies with rating >= {min_rating} across all genres: {results[0][0]}")
    return results[0][0]

def avg_comedy_rating_per_year(genre="Comedy"):
    sql = """
        SELECT AVG(vote_average)
        FROM movies
        WHERE genres_list ILIKE %s;
    """
    results = run_query(sql, (f"%{genre}%",))
    print(f"Average Comedy movie rating overall: {results[0][0]}")
    return results[0][0]


def top_10_longest_movies():
    sql = """
        SELECT MAX(runtime)
        FROM (
            SELECT runtime
            FROM movies
            ORDER BY runtime DESC
            LIMIT 10
        ) AS top10;
    """
    results = run_query(sql)
    print(f"Longest movie runtime (from top 10): {results[0][0]}")
    return results[0][0]

def top_5_scarlett_johansson(actor="Scarlett Johansson"):
    sql = """
        SELECT title, vote_average, release_date
FROM movies
WHERE Star1 ILIKE %s
   OR Star2 ILIKE %s
    OR Star3 ILIKE %s
    OR Star4 ILIKE %s
ORDER BY vote_average DESC
LIMIT 5;
    """
    results = run_query(sql, (f"%{actor}%", f"%{actor}%", f"%{actor}%", f"%{actor}%"))
    print("Top 5 Scarlett Johansson movies:")
    for title, rating, date in results:
        print(f"{title} ({date.year if date else 'N/A'}): {rating}")
    return results

def brad_pitt_2000_2020(actor="Brad Pitt",min_rating=7.8):
    sql = """
       SELECT title, vote_average, release_date
FROM movies
WHERE vote_average >= %s
  AND EXTRACT(YEAR FROM release_date) BETWEEN 2000 AND 2020
  AND (
        Star1 ILIKE %s
     OR Star2 ILIKE %s  
     OR Star3 ILIKE %s
     OR Star4 ILIKE %s
  )
ORDER BY vote_average DESC;
    """
    results = run_query(sql, (min_rating,f"%{actor}%", f"%{actor}%", f"%{actor}%", f"%{actor}%"))
    print(f"Brad Pitt movies 2000–2020 with rating >= {min_rating}:")
    for title, rating, date in results:
        print(f"{title} ({date.year if date else 'N/A'}): {rating}")
    return results

def drama_and_romance(genre1="Drama", genre2="Romance"):
    sql = """
        SELECT COUNT(*)
        FROM movies
        WHERE genres_list ILIKE %s
          AND genres_list ILIKE %s;
    """
    results = run_query(sql, (f"%{genre1}%", f"%{genre2}%"))
    print(f"Movies where genre are Drama AND Romance: {results[0][0]}")
    return results[0][0]

def top_5_actors():
    sql = """
        WITH actors AS (
            SELECT TRIM(unnest(string_to_array(cast_members, ','))) AS actor
            FROM movies
            WHERE cast_members IS NOT NULL
        ),
        cleaned AS (
            SELECT actor
            FROM actors
            WHERE actor NOT ILIKE '%unknown%' AND actor <> ''
        ),
        freq AS (
            SELECT actor, COUNT(*) AS c
            FROM cleaned
            GROUP BY actor
            ORDER BY c DESC
            LIMIT 5
        )
        SELECT actor, c FROM freq ORDER BY c DESC;
    """
    results = run_query(sql)
    print("Top 5 most frequent actors:")
    for actor, count in results:
        print(f"{actor}: {count} movies")
    return results



def count_long_movies_per_year(runtime_min=150):
    sql = """
        SELECT COUNT(*)
        FROM movies
        WHERE runtime > %s;
    """
    results = run_query(sql, (runtime_min,))
    print(f"Movies longer than {runtime_min} minutes: {results[0][0]}")
    return results[0][0]


# ----------- Main -----------

if __name__ == "__main__":
    print("=== Postgres Queries ===")
    timed_query(query_by_genre, 20, "Action")
    timed_query(query_by_actor, 20, "Tom Hanks")
    timed_query(query_by_year, 20, 2015)
    timed_query(query_by_actor_and_genre, 20, "Tom Hanks", "Drama")
    timed_query(query_by_genre_and_year, 20, "Action", 2015)
    timed_query(top_rated_by_genre, 20, "Drama", 5)
    timed_query(top_rated_by_genre_and_year, 20, "Romance", 2019, 3)
    timed_query(count_movies_by_actor, 20, "Leonardo DiCaprio")
    timed_query(count_high_rated_action_movies, 20, "Action",8.0,)

    print("=== Advanced Postgres Queries ===")

    timed_query(science_fiction_after_2010_with_rating, 20, "Science Fiction", 7.5,)
    timed_query(drama_or_thriller_long, 20,"Drama", "Thriller", 140)
    timed_query(count_high_rated_per_genre, 20, 8.0)
    timed_query(avg_comedy_rating_per_year, 20, "Comedy")
    timed_query(top_10_longest_movies, 20)
    timed_query(top_5_scarlett_johansson, 20, "Scarlett Johansson")
    timed_query(brad_pitt_2000_2020, 20, "Brad Pitt",7.8)
    timed_query(drama_and_romance, 20, "Drama", "Romance")
    timed_query(top_5_actors, 20)
    timed_query(count_long_movies_per_year, 20, 150)



    cur.close()
    conn.close()
