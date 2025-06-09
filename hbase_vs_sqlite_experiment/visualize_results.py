import json
import matplotlib.pyplot as plt

# Load data
with open('benchmark_results.json') as f:
    benchmark = json.load(f)

with open('hbase_stable_report.json') as f:
    hbase_load = json.load(f)

with open('sqlite_basic_report.json') as f:
    sqlite_load = json.load(f)

# --- Load Time Plot ---
def plot_load_times():
    labels = ['HBase', 'SQLite']
    load_times = [hbase_load['load_time'], sqlite_load['load_time']]
    rps = [hbase_load['records_per_second'], sqlite_load['records_per_second']]

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.bar(labels, load_times, color='skyblue', label='Load Time (s)')
    ax2.plot(labels, rps, color='green', marker='o', label='Records/sec')

    ax1.set_ylabel('Load Time (s)')
    ax2.set_ylabel('Records/sec')
    ax1.set_title('Data Loading Performance')

    fig.tight_layout()

    # Save the figure
    fig.savefig('load_times.png')

    plt.show()

# --- Benchmark Performance Plot ---
def plot_benchmark_times():
    categories = list(benchmark['sqlite'].keys())
    sqlite_times = [benchmark['sqlite'][k] for k in categories]
    hbase_times = [benchmark['hbase'][k] for k in categories]

    x = range(len(categories))
    plt.figure(figsize=(12, 6))
    plt.bar([i - 0.2 for i in x], sqlite_times, width=0.4, label='SQLite', color='orange')
    plt.bar([i + 0.2 for i in x], hbase_times, width=0.4, label='HBase', color='blue')

    plt.xticks(ticks=x, labels=categories, rotation=45, ha='right')
    plt.ylabel('Time (s)')
    plt.title('Benchmark Operation Times')
    plt.legend()
    plt.tight_layout()

    # Save the figure
    plt.savefig('benchmark_times.png')

    plt.show()

# Run plots

def run_visualizations():
    print("Generating visualizations...")
    plot_load_times()
    plot_benchmark_times()
    print("Visualizations generated successfully.")

if __name__ == "__main__":
    run_visualizations()