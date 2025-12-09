import psycopg2
from performance_monitor import benchmark_query

"""
Categorized queries for PostgreSQL performance testing:
1. SIMPLE QUERIES - Single table or simple JOINs
2. COMPLEX QUERIES - Multiple JOINs, subqueries
3. AGGREGATED QUERIES - GROUP BY, aggregation functions, statistics
"""

# Database connection
conn = psycopg2.connect(
    dbname="movie_db",
    user="user",
    password="password",
    host="localhost",
    port=5434
)
cur = conn.cursor()


# ============================================================================
# SIMPLE QUERIES (No JOINs, multiple WHERE conditions, single table)
# ============================================================================
def simple_query_profitable_movies(min_budget=10000000, min_revenue_multiplier=3):
    """Simple: Profitable movies with budget filtering (multiple conditions, no JOINs)"""
    sql = """
    SELECT id, title, budget, revenue, 
           (revenue - budget) as profit,
           (revenue::float / budget) as roi
    FROM movies
    WHERE budget >= %s
      AND revenue > budget * %s
      AND budget > 0
      AND revenue > 0
    ORDER BY roi DESC
    """
    cur.execute(sql, (min_budget, min_revenue_multiplier))
    return cur.fetchall()


def simple_query_popular_recent_movies(year_start=2015, min_popularity=50, min_vote_count=1000):
    """Simple: Popular recent movies with multiple filters (no JOINs)"""
    sql = """
    SELECT id, title, release_year, popularity, vote_count
    FROM movies
    WHERE release_year >= %s
      AND popularity >= %s
      AND vote_count >= %s
    ORDER BY popularity DESC
    """
    cur.execute(sql, (year_start, min_popularity, min_vote_count))
    return cur.fetchall()


def simple_query_long_high_rated_movies(min_runtime=150, min_rating=7.5, year_start=2000):
    """Simple: Long, highly-rated movies (multiple conditions, no JOINs)"""
    sql = """
    SELECT id, title, runtime, vote_average, release_year
    FROM movies
    WHERE runtime >= %s
      AND vote_average >= %s
      AND release_year >= %s
      AND runtime IS NOT NULL
      AND vote_average IS NOT NULL
    ORDER BY vote_average DESC, runtime DESC
    """
    cur.execute(sql, (min_runtime, min_rating, year_start))
    return cur.fetchall()


def simple_query_spanish_blockbusters(min_budget=10000000, min_revenue=20000000, language="es"):
    """Simple: Spanish language blockbusters (multiple conditions, no JOINs)"""
    sql = """
    SELECT id, title, budget, revenue, original_language, release_year
    FROM movies
    WHERE budget >= %s
      AND revenue >= %s
      AND original_language = %s
      AND budget > 0
      AND revenue > 0
    ORDER BY revenue DESC
    """
    cur.execute(sql, (min_budget, min_revenue, language))
    return cur.fetchall()


# ============================================================================
# COMPLEX QUERIES (Multiple JOINs, complex filtering, subqueries)
# ============================================================================

def complex_query_multi_genre(genres=["Action", "Adventure", "Science Fiction"], min_rating=7.0):
    """Complex: Movies with multiple genres (subquery)"""
    sql = """
    SELECT m.id, m.title, m.vote_average, 
           array_agg(DISTINCT g.name) as genres
    FROM movies m
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    WHERE m.vote_average >= %s
      AND m.id IN (
          SELECT mg2.movie_id
          FROM movie_genres mg2
          JOIN genres g2 ON mg2.genre_id = g2.id
          WHERE g2.name = ANY(%s)
          GROUP BY mg2.movie_id
          HAVING COUNT(DISTINCT g2.name) >= 3
      )
    GROUP BY m.id, m.title, m.vote_average
    ORDER BY m.vote_average DESC
    """
    cur.execute(sql, (min_rating, genres))
    return cur.fetchall()


def complex_query_genre_country_language(genre="Drama", country="United States of America", language="en"):
    """Complex: Movies by genre, production country, and language (4 JOINs)"""
    sql = """
    SELECT DISTINCT m.id, m.title, m.release_year, m.vote_average
    FROM movies m
    JOIN movie_genres mg ON m.id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.id
    JOIN movie_countries mc ON m.id = mc.movie_id
    JOIN countries c ON mc.country_id = c.id
    WHERE g.name = %s
      AND c.name = %s
      AND m.original_language = %s
      AND m.release_year >= 2010
      AND m.vote_average IS NOT NULL
    ORDER BY m.vote_average DESC
    """
    cur.execute(sql, (genre, country, language))
    return cur.fetchall()


def complex_query_high_budget_profit(min_budget=50000000):
    """Complex: Movies with high budget and profit margin (calculation + filtering)"""
    sql = """
        SELECT 
            m.id,
            m.title,
            m.budget,
            m.revenue,
            (m.revenue - m.budget) AS profit,
            array_agg(DISTINCT g.name) AS genres
        FROM movies m
        JOIN movie_genres mg ON m.id = mg.movie_id
        JOIN genres g ON mg.genre_id = g.id
        WHERE m.budget >= %s
          AND m.revenue > m.budget
          AND m.budget > 0
        GROUP BY m.id, m.title, m.budget, m.revenue
        ORDER BY profit DESC;
    """
    cur.execute(sql, (min_budget,))
    return cur.fetchall()


# ============================================================================
# AGGREGATED QUERIES (GROUP BY, COUNT, AVG, SUM, statistics)
# ============================================================================

def aggregate_movies_per_year():
    """Aggregate: Count movies per year"""
    sql = """
    SELECT release_year, COUNT(*) as movie_count
    FROM movies
    WHERE release_year IS NOT NULL
    GROUP BY release_year
    ORDER BY release_year DESC;
    """
    cur.execute(sql)
    return cur.fetchall()


def aggregate_avg_rating_per_genre():
    """Aggregate: Average rating per genre"""
    sql = """
    SELECT g.name, 
           COUNT(DISTINCT m.id) as movie_count,
           AVG(m.vote_average) as avg_rating,
           MAX(m.vote_average) as max_rating,
           MIN(m.vote_average) as min_rating
    FROM genres g
    JOIN movie_genres mg ON g.id = mg.genre_id
    JOIN movies m ON mg.movie_id = m.id
    WHERE m.vote_average IS NOT NULL
    GROUP BY g.name
    ORDER BY avg_rating DESC;
    """
    cur.execute(sql)
    return cur.fetchall()


def aggregate_top_actors_by_movie_count(top_n=10):
    """Aggregate: Most prolific actors"""
    sql = """
    SELECT p.name, 
           COUNT(DISTINCT m.id) as movie_count,
           AVG(m.vote_average) as avg_movie_rating
    FROM people p
    JOIN movie_people mp ON p.id = mp.person_id
    JOIN movies m ON mp.movie_id = m.id
    WHERE mp.role = 'actor' AND m.vote_average IS NOT NULL
    GROUP BY p.name
    HAVING COUNT(DISTINCT m.id) >= 3
    ORDER BY movie_count DESC, avg_movie_rating DESC
    LIMIT %s;
    """
    cur.execute(sql, (top_n,))
    return cur.fetchall()


def aggregate_yearly_trends():
    """Aggregate: Yearly movie industry trends"""
    sql = """
    SELECT m.release_year,
           COUNT(*) as movie_count,
           AVG(m.vote_average) as avg_rating,
           AVG(m.budget) as avg_budget,
           AVG(m.revenue) as avg_revenue,
           AVG(m.runtime) as avg_runtime,
           COUNT(CASE WHEN m.vote_average >= 7.0 THEN 1 END) as high_rated_count
    FROM movies m
    WHERE m.release_year IS NOT NULL
      AND m.release_year >= 1990
    GROUP BY m.release_year
    ORDER BY m.release_year DESC;
    """
    cur.execute(sql)
    return cur.fetchall()


def aggregate_genre_combinations():
    """Aggregate: Most common genre combinations"""
    sql = """
    WITH movie_genre_list AS (
        SELECT m.id, m.title,
               array_agg(g.name ORDER BY g.name) as genres
        FROM movies m
        JOIN movie_genres mg ON m.id = mg.movie_id
        JOIN genres g ON mg.genre_id = g.id
        GROUP BY m.id, m.title
        HAVING COUNT(*) > 1
    )
    SELECT genres,
           COUNT(*) as movie_count,
           AVG(m.vote_average) as avg_rating
    FROM movie_genre_list mgl
    JOIN movies m ON mgl.id = m.id
    WHERE m.vote_average IS NOT NULL
    GROUP BY genres
    HAVING COUNT(*) >= 3
    ORDER BY movie_count DESC
    """
    cur.execute(sql)
    return cur.fetchall()


# ============================================================================
# MAIN - Run all queries with performance monitoring
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("POSTGRESQL PERFORMANCE BENCHMARK - CATEGORIZED QUERIES")
    print("=" * 80)

    # Simple Queries
    print("\n" + "=" * 80)
    print("SIMPLE QUERIES")
    print("=" * 80)

    queries_simple = [
        (simple_query_profitable_movies, "Simple: Profitable Movies", 10000000, 3),
        (simple_query_popular_recent_movies, "Simple: Popular Recent Movies", 2015, 50, 1000),
        (simple_query_long_high_rated_movies, "Simple: Long High Rated Movies", 150, 7.5, 2000),
        (simple_query_spanish_blockbusters, "Simple: Spanish Blockbusters", 10000000, 20000000, "es"),
    ]

    for func, name, *args in queries_simple:
        results, avg_metrics, _ = benchmark_query(cur, func, *args, runs=10, query_name=name)
        print(f"\n{name}")
        print(f"  Rows: {avg_metrics['rows_returned']}")
        print(
            f"  Time: {avg_metrics['avg_execution_time']:.6f}s (min: {avg_metrics['min_execution_time']:.6f}s, max: {avg_metrics['max_execution_time']:.6f}s)")
        print(f"  CPU:  {avg_metrics['avg_cpu_percent']:.2f}%")
        print(f"  Mem:  {avg_metrics['avg_memory_mb']:.2f} MB")

    # Complex Queries
    print("\n" + "=" * 80)
    print("COMPLEX QUERIES")
    print("=" * 80)

    queries_complex = [
        (complex_query_multi_genre, "Complex: Multi-Genre", ["Action", "Adventure", "Science Fiction"], 7.0),
        (complex_query_genre_country_language, "Complex: Genre+Country+Language", "Drama", "United States of America",
         "en"),
        (complex_query_high_budget_profit, "Complex: Budget & Profit Analysis", 50000000),
    ]

    for func, name, *args in queries_complex:
        results, avg_metrics, _ = benchmark_query(cur, func, *args, runs=10, query_name=name)
        print(f"\n{name}")
        print(f"  Rows: {avg_metrics['rows_returned']}")
        print(
            f"  Time: {avg_metrics['avg_execution_time']:.6f}s (min: {avg_metrics['min_execution_time']:.6f}s, max: {avg_metrics['max_execution_time']:.6f}s)")
        print(f"  CPU:  {avg_metrics['avg_cpu_percent']:.2f}%")
        print(f"  Mem:  {avg_metrics['avg_memory_mb']:.2f} MB")

    # Aggregated Queries
    print("\n" + "=" * 80)
    print("AGGREGATED QUERIES")
    print("=" * 80)

    queries_aggregate = [
        (aggregate_movies_per_year, "Aggregate: Movies per Year"),
        (aggregate_avg_rating_per_genre, "Aggregate: Avg Rating per Genre"),
        (aggregate_top_actors_by_movie_count, "Aggregate: Top Actors by Count", 10),
        (aggregate_yearly_trends, "Aggregate: Yearly Trends"),
        (aggregate_genre_combinations, "Aggregate: Genre Combinations"),
    ]

    for func, name, *args in queries_aggregate:
        results, avg_metrics, _ = benchmark_query(cur, func, *args, runs=10, query_name=name)
        print(f"\n{name}")
        print(f"  Rows: {avg_metrics['rows_returned']}")
        print(
            f"  Time: {avg_metrics['avg_execution_time']:.6f}s (min: {avg_metrics['min_execution_time']:.6f}s, max: {avg_metrics['max_execution_time']:.6f}s)")
        print(f"  CPU:  {avg_metrics['avg_cpu_percent']:.2f}%")
        print(f"  Mem:  {avg_metrics['avg_memory_mb']:.2f} MB")

    print("\n" + "=" * 80)
    print("BENCHMARK COMPLETE")
    print("=" * 80)

    cur.close()
    conn.close()
