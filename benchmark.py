import psycopg2
import csv
import json
from datetime import datetime
from performance_monitor import benchmark_query, PerformanceMonitor
from queries_categorized import *

"""
Benchmark script for testing PostgreSQL performance with different dataset sizes.
Run this script multiple times with different row counts: 5000, 15000, 30000, etc.
"""


def get_current_row_count(cursor):
    """Get the current number of rows in the movies table"""
    cursor.execute("SELECT COUNT(*) FROM movies;")
    return cursor.fetchone()[0]


def run_all_benchmarks(cursor, dataset_size, runs=10):
    """
    Run all categorized queries and collect metrics
    
    Args:
        cursor: Database cursor
        dataset_size: Current dataset size (e.g., 5000, 15000)
        runs: Number of times to run each query
    
    Returns:
        List of benchmark results
    """
    all_results = []
    
    print(f"\n{'='*80}")
    print(f"RUNNING BENCHMARK FOR {dataset_size} ROWS")
    print(f"{'='*80}\n")
    
    # Define all queries with their categories
    benchmark_queries = [
        # SIMPLE QUERIES
        {
            'category': 'SIMPLE',
            'queries': [
                # (simple_query_recent_high_rated, "Simple: Recent High Rated Movies", [2010, 2020, 8.0]),
                (simple_query_profitable_movies, "Simple: Profitable Movies", [10000000, 3]),
                (simple_query_popular_recent_movies, "Simple: Popular Recent Movies", [2015, 50, 1000]),
                (simple_query_long_high_rated_movies, "Simple: Long High Rated Movies", [150, 7.5, 2000]),
                (simple_query_spanish_blockbusters, "Simple: Spanish Blockbusters", [10000000, 20000000, "es"]),
            ]
        },
        # COMPLEX QUERIES
        {
            'category': 'COMPLEX',
            'queries': [
                # (complex_query_actor_genre_year, "Complex: Actor+Genre+Year",
                #  ["Leonardo DiCaprio", "Drama", 2010, 2020]),
                (complex_query_multi_genre, "Complex: Multi-Genre", 
                 [["Action", "Adventure", "Science Fiction"], 7.0]),
                # (complex_query_director_actor_collab, "Complex: Director-Actor Movies",
                #  [7.5, 2010]),
                (complex_query_genre_country_language, "Complex: Genre+Country+Language", 
                 ["Drama", "United States of America", "en"]),
                (complex_query_high_budget_profit, "Complex: Budget & Profit", 
                 [50000000]),
            ]
        },
        # AGGREGATED QUERIES
        {
            'category': 'AGGREGATED',
            'queries': [
                (aggregate_movies_per_year, "Aggregate: Movies per Year", []),
                (aggregate_avg_rating_per_genre, "Aggregate: Avg Rating per Genre", []),
                (aggregate_top_actors_by_movie_count, "Aggregate: Top Actors", [10]),
                (aggregate_yearly_trends, "Aggregate: Yearly Trends", []),
                (aggregate_genre_combinations, "Aggregate: Genre Combinations", []),
            ]
        }
    ]
    
    # Run each category
    for category_info in benchmark_queries:
        category = category_info['category']
        print(f"\n{'='*80}")
        print(f"{category} QUERIES")
        print(f"{'='*80}")
        
        for func, name, args in category_info['queries']:
            print(f"\nRunning: {name}...", end=" ")
            
            try:
                results, avg_metrics, all_metrics = benchmark_query(
                    cursor, 
                    func,
                    *args,
                    runs=runs, 
                    query_name=name
                )
                
                # Add additional metadata
                avg_metrics['category'] = category
                avg_metrics['dataset_size'] = dataset_size
                avg_metrics['timestamp'] = datetime.now().isoformat()

                if avg_metrics.get('avg_memory_mb', 0) < 0:
                    avg_metrics['avg_memory_mb'] = 0
                
                all_results.append(avg_metrics)
                
                print(f"✓ Done ({avg_metrics['avg_execution_time']:.6f}s)")
                
            except Exception as e:
                print(f"✗ Error: {e}")
                continue
    
    return all_results


def save_results_to_csv(results, filename):
    """Save benchmark results to CSV file"""
    if not results:
        print("No results to save!")
        return
    
    fieldnames = [
        'timestamp', 'dataset_size', 'category', 'query_name', 'runs',
        'avg_execution_time', 'min_execution_time', 'max_execution_time',
        'avg_cpu_percent', 'avg_memory_mb', 'rows_returned'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in results:
            row = {key: result.get(key, '') for key in fieldnames}
            writer.writerow(row)
    
    print(f"\n✓ Results saved to: {filename}")


def save_results_to_json(results, filename):
    """Save benchmark results to JSON file"""
    with open(filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(results, jsonfile, indent=2, default=str)
    
    print(f"✓ Results saved to: {filename}")


def print_summary(results):
    """Print a summary of the benchmark results"""
    if not results:
        return
    
    print(f"\n{'='*80}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*80}\n")
    
    # Group by category
    categories = {}
    for result in results:
        cat = result['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(result)
    
    for category, cat_results in categories.items():
        print(f"\n{category} QUERIES:")
        print(f"{'Query Name':<50} {'Time (s)':<12} {'Rows':<8} {'CPU %':<8}")
        print("-" * 80)
        
        for result in cat_results:
            name = result['query_name'][:48]
            time_str = f"{result['avg_execution_time']:.6f}"
            rows = result['rows_returned']
            cpu = f"{result['avg_cpu_percent']:.2f}"
            
            print(f"{name:<50} {time_str:<12} {rows:<8} {cpu:<8}")
    
    # Overall statistics
    print(f"\n{'='*80}")
    print("OVERALL STATISTICS")
    print(f"{'='*80}")
    
    total_queries = len(results)
    avg_time = sum(r['avg_execution_time'] for r in results) / total_queries
    total_time = sum(r['avg_execution_time'] * r['runs'] for r in results)
    
    print(f"Total Queries Run: {total_queries}")
    print(f"Average Query Time: {avg_time:.6f} seconds")
    print(f"Total Execution Time: {total_time:.2f} seconds")
    print(f"Dataset Size: {results[0]['dataset_size']} rows")


def get_system_info():
    """Get system and database information"""
    import platform
    import psutil
    
    info = {
        'platform': platform.system(),
        'platform_version': platform.version(),
        'processor': platform.processor(),
        'cpu_count': psutil.cpu_count(),
        'total_memory_mb': psutil.virtual_memory().total / 1024 / 1024,
        'python_version': platform.python_version(),
    }
    
    return info


def main():
    """Main benchmark execution"""
    
    # Connect to database
    conn = psycopg2.connect(
        dbname="movie_db",
        user="user",
        password="password",
        host="localhost",
        port=5434
    )
    cur = conn.cursor()
    
    # Get current dataset size
    dataset_size = get_current_row_count(cur)
    print(f"\nCurrent dataset size: {dataset_size} rows")
    
    # Get database stats
    print("\nDatabase Statistics:")
    db_stats = PerformanceMonitor.get_database_stats(conn)
    print(f"  Database Size: {db_stats['db_size_mb']:.2f} MB")
    print(f"  Cache Hit Ratio: {db_stats['cache_hit_ratio']:.2f}%")
    
    # Get system info
    print("\nSystem Information:")
    sys_info = get_system_info()
    print(f"  Platform: {sys_info['platform']}")
    print(f"  CPU Count: {sys_info['cpu_count']}")
    print(f"  Total Memory: {sys_info['total_memory_mb']:.2f} MB")
    
    # Ask user for number of runs
    print("\n" + "="*80)
    runs = input("Enter number of runs per query (default 10): ").strip()
    runs = int(runs) if runs.isdigit() else 10
    
    # Run benchmarks
    results = run_all_benchmarks(cur, dataset_size, runs=runs)
    
    # Print summary
    print_summary(results)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"benchmark_results_{dataset_size}_rows_{timestamp}.csv"
    json_filename = f"benchmark_results_{dataset_size}_rows_{timestamp}.json"
    
    save_results_to_csv(results, csv_filename)
    save_results_to_json(results, json_filename)
    
    # Add system info to JSON
    with open(json_filename, 'r') as f:
        data = json.load(f)
    
    full_data = {
        'system_info': sys_info,
        'database_stats': db_stats,
        'dataset_size': dataset_size,
        'runs_per_query': runs,
        'timestamp': datetime.now().isoformat(),
        'results': data
    }
    
    with open(json_filename, 'w') as f:
        json.dump(full_data, f, indent=2, default=str)
    
    print(f"\n{'='*80}")
    print("BENCHMARK COMPLETE!")
    print(f"{'='*80}\n")
    
    # Close connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

