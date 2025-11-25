# Spark Pipeline Benchmark Tool

A FastAPI-based benchmarking tool for Apache Spark data processing pipelines with a Gradio web interface.

## Features

- 🚀 RESTful API for running Spark benchmarks
- 📊 Gradio web interface for easy configuration and visualization
- 📈 Comprehensive performance metrics tracking
- 💾 CSV-based results logging
- 🔧 Dynamic Spark cluster configuration

## Project Structure

```
.
├── main.py              # FastAPI server
├── ui.py               # Gradio web interface
├── config.py           # Configuration and environment variables
├── connectors.py       # Spark session management
├── requirements.txt    # Python dependencies
├── data_science/
│   └── main.py        # Benchmark pipeline implementation
└── logs/              # Benchmark results (auto-created)
```

## Environment Variables

Configure these environment variables before running:

```bash
# Spark Master Configuration
export SPARK_MASTER_HOST="local[*]"  # or "spark-master-hostname" for cluster mode

# Dataset Configuration
export MAX_DATASET_SIZE="120000"     # Maximum number of rows in the dataset
export DATASET_PATH="./train.csv"    # Path to your dataset
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure you have a dataset (default: `train.csv`) in the project root.

## Usage

### Method 1: Using the Gradio UI (Recommended)

1. Start the FastAPI server:
```bash
python main.py
```

2. In a separate terminal, start the Gradio interface:
```bash
python ui.py
```

3. Open your browser and navigate to `http://localhost:7860`

4. Configure your benchmark parameters:
   - **Number of Workers**: Number of worker nodes (1-10)
   - **Memory per Worker**: Memory allocated per worker in GB
   - **Dataset Scale**: Scale factor for dataset (0.0 to 1.0)
   - **Log Directory**: Where to save results

5. Click "Run Benchmark" and view results in real-time

### Method 2: Using the API Directly

Start the server:
```bash
python main.py
```

Make a POST request to `/benchmark`:
```bash
curl -X POST "http://localhost:8000/benchmark" \
  -H "Content-Type: application/json" \
  -d '{
    "num_workers": 2,
    "mem_per_worker": 10,
    "dataset_scale": 1.0,
    "log_dir": "./logs/test_1"
  }'
```

### Method 3: Using Python

```python
import requests

response = requests.post(
    "http://localhost:8000/benchmark",
    json={
        "num_workers": 2,
        "mem_per_worker": 10,
        "dataset_scale": 1.0,
        "log_dir": "./logs/test_1"
    }
)

print(response.json())
```

## API Endpoints

### `POST /benchmark`

Run a benchmark with specified configuration.

**Request Body:**
```json
{
  "num_workers": 2,
  "mem_per_worker": 10,
  "dataset_scale": 1.0,
  "log_dir": "./logs/benchmark_1"
}
```

**Response:**
```json
{
  "success": true,
  "E2E_time": 12.34,
  "E2E_throughput": 9715.45,
  "num_rows": 120000,
  "num_stages": 3,
  "results_file": "./logs/benchmark_1/results.csv",
  "message": "Benchmark completed successfully"
}
```

### `GET /health`

Health check endpoint.

### `GET /`

API documentation and available endpoints.

## Benchmark Metrics

The tool captures the following metrics for each benchmark run:

### Configuration Metrics
- `num_workers`: Number of worker nodes
- `mem_per_worker`: Memory per worker in GB
- `dataset_scale`: Dataset scale factor
- `num_rows`: Actual number of rows processed

### Stage-Level Metrics (for each stage)
- `stage{i}_time`: Stage duration in seconds
- `stage{i}_throughput`: Processing throughput (rows/sec)
- `stage{i}_executorRunTime`: Total executor run time
- `stage{i}_executorCpuTime`: Total executor CPU time
- `stage{i}_jvmGCTime`: JVM garbage collection time
- `stage{i}_recordsRead`: Number of records read
- `stage{i}_bytesRead`: Number of bytes read

### End-to-End Metrics
- `E2E_time`: Total pipeline execution time
- `E2E_throughput`: Overall throughput (rows/sec)

## Results Storage

Benchmark results are stored in CSV format in the specified log directory:
- Location: `{log_dir}/results.csv`
- Format: CSV with headers
- Behavior: Appends to existing file or creates new one

Stage metrics are also saved as JSON in subdirectories for detailed analysis.

## Spark Cluster Configuration

The tool supports both local and cluster modes:

### Local Mode (Default)
```bash
export SPARK_MASTER_HOST="local[*]"
```

### Cluster Mode
```bash
export SPARK_MASTER_HOST="spark-master-hostname"
```

The connector automatically:
- Configures executors (1 per worker with 80% of allocated memory)
- Reserves 20% for heap overhead
- Sets driver memory to 16GB
- Enables off-heap memory
- Loads SparkMeasure for metrics collection

## Customizing the Pipeline

To modify the data processing pipeline, edit `data_science/main.py`:

```python
def run_data_cleaning(spark: SparkSession, df):
    """
    Add your custom pipeline logic here
    """
    # Your transformations
    clean_df = df.drop("text")
    
    # Trigger execution
    clean_df.collect()
    
    return clean_df
```

## Troubleshooting

### API Connection Errors
- Ensure FastAPI server is running: `python main.py`
- Check if port 8000 is available
- Verify with: `curl http://localhost:8000/health`

### Spark Connection Issues
- Check `SPARK_MASTER_HOST` environment variable
- Verify Spark master is running (cluster mode)
- Review Spark logs in the terminal

### Memory Issues
- Reduce `mem_per_worker` value
- Reduce `dataset_scale` to process fewer rows
- Check available system memory

### Missing Dataset
- Ensure `train.csv` exists in project root
- Update `DATASET_PATH` environment variable if using different location

## Performance Tips

1. **Weak Scaling Tests**: Keep dataset size constant, increase workers
2. **Strong Scaling Tests**: Increase dataset size proportionally with workers
3. **Memory Tuning**: Start with 10GB per worker and adjust based on data size
4. **Dataset Scale**: Use smaller scales (0.1-0.5) for quick tests

## License

This project is for educational and benchmarking purposes.
