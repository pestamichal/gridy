from hbase_loader import run_base_loading
from sqlite_loader import run_sqlite_loading
from benchmark_runner import run_benchmarks
from visualize_results import run_visualizations

def main():
    run_base_loading()
    run_sqlite_loading()
    print("Data loading completed for both HBase and SQLite.")
    # Run benchmarks
    run_benchmarks()
    run_visualizations()
    print("Finished running benchmarks and visualizations.")

if __name__ == "__main__":
    main()


