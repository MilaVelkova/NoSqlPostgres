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
    sql = """
    SELECT m.id, m.title
    FROM movies m
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    WHERE g.name ILIKE %s;
    """
    results = run_query(sql, (f"%{genre}%",))
    print(f"[Genre: {genre}] Found {len(results)} movies")
    return results

def query_by_actor(actor="Tom Hanks"):
    sql = """
    SELECT m.id, m.title
    FROM movies m
    JOIN movie_people mp ON m.id = mp.movie_id
    JOIN people p ON mp.person_id = p.id
    WHERE mp.role ILIKE 'actor' AND p.name ILIKE %s;
    """
    results = run_query(sql, (f"%{actor}%",))
    print(f"[Actor: {actor}] Found {len(results)} movies")
    return results

def query_by_year(year=2015):
    sql = "SELECT id, title FROM movies WHERE release_year = %s;"
    results = run_query(sql, (year,))
    print(f"[Year: {year}] Found {len(results)} movies")
    return results

def query_by_actor_and_genre(actor="Tom Hanks", genre="Drama"):
    sql = """
    SELECT DISTINCT m.id, m.title
    FROM movies m
    JOIN movie_people mp ON m.id = mp.movie_id
    JOIN people p ON mp.person_id = p.id
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    WHERE mp.role ILIKE 'actor' AND p.name ILIKE %s AND g.name ILIKE %s;
    """
    results = run_query(sql, (f"%{actor}%", f"%{genre}%"))
    print(f"[Actor: {actor} AND Genre: {genre}] Found {len(results)} movies")
    return results

def query_by_genre_and_year(genre="Action", year=2015):
    sql = """
    SELECT m.id, m.title
    FROM movies m
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    WHERE g.name ILIKE %s AND m.release_year = %s;
    """
    results = run_query(sql, (f"%{genre}%", year))
    print(f"[Genre: {genre} AND Year: {year}] Found {len(results)} movies")
    return results

def top_rated_by_genre(genre="Drama", top_n=5):
    sql = """
    SELECT m.id, m.title, m.vote_average
    FROM movies m
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    WHERE g.name ILIKE %s
    ORDER BY m.vote_average DESC
    LIMIT %s;
    """
    results = run_query(sql, (f"%{genre}%", top_n))
    print(f"Top {top_n} rated movies in Genre '{genre}': {[row[1] for row in results]}")
    return results

def top_rated_by_genre_and_year(genre="Romance", year=2019, top_n=3):
    sql = """
    SELECT m.id, m.title, m.vote_average
    FROM movies m
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    WHERE g.name ILIKE %s AND m.release_year = %s
    ORDER BY m.vote_average DESC
    LIMIT %s;
    """
    results = run_query(sql, (f"%{genre}%", year, top_n))
    print(f"Top {top_n} rated movies in Genre '{genre}' and Year '{year}': {[row[1] for row in results]}")
    return results

def count_movies_by_actor(actor="Leonardo DiCaprio"):
    sql = """
    SELECT COUNT(*)
    FROM movie_people mp
    JOIN people p ON mp.person_id = p.id
    WHERE mp.role ILIKE 'actor' AND p.name ILIKE %s;
    """
    results = run_query(sql, (f"%{actor}%",))
    print(f"Number of movies with actor '{actor}': {results[0][0]}")
    return results[0][0]

def count_high_rated_action_movies(genre="Action", min_rating=8.0):
    sql = """
    SELECT COUNT(DISTINCT m.id)
    FROM movies m
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    WHERE g.name ILIKE %s AND m.vote_average >= %s;
    """
    results = run_query(sql, (f"%{genre}%", min_rating))
    print(f"Number of Action movies with rating >= {min_rating}: {results[0][0]}")
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


    cur.close()
    conn.close()
