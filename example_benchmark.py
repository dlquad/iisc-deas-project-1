"""
Example script for running benchmarks programmatically
"""

import requests
import time

API_URL = "http://localhost:8000"


def check_api_health():
    """Check if the API is running."""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False


def run_benchmark(num_workers, mem_per_worker, cores_per_worker, dataset_scale, log_dir):
    """Run a benchmark with specified configuration."""
    
    print(f"\n{'='*60}")
    print(f"Running Benchmark:")
    print(f"  Workers: {num_workers}")
    print(f"  Memory per Worker: {mem_per_worker} GB")
    print(f"  Cores per Worker: {cores_per_worker}")
    print(f"  Dataset Scale: {dataset_scale}")
    print(f"  Log Directory: {log_dir}")
    print(f"{'='*60}\n")
    
    try:
        response = requests.post(
            f"{API_URL}/benchmark",
            json={
                "num_workers": num_workers,
                "mem_per_worker": mem_per_worker,
                "cores_per_worker": cores_per_worker,
                "dataset_scale": dataset_scale,
                "log_dir": log_dir
            },
            timeout=300
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Benchmark completed successfully!")
            print(f"\nResults:")
            print(f"  Rows Processed: {data['num_rows']:,}")
            print(f"  Number of Stages: {data['num_stages']}")
            print(f"  E2E Time: {data['E2E_time']:.2f} seconds")
            print(f"  E2E Throughput: {data['E2E_throughput']:.2f} rows/sec")
            print(f"  Results File: {data['results_file']}")
            return data
        else:
            print(f"❌ Benchmark failed with status code: {response.status_code}")
            print(response.text)
            return None
            
    except Exception as e:
        print(f"❌ Error running benchmark: {e}")
        return None


def main():
    """Main function to run example benchmarks."""
    
    # Check API health
    print("Checking API health...")
    if not check_api_health():
        print("❌ API is not running. Please start the FastAPI server first:")
        print("   python main.py")
        return
    
    print("✅ API is running\n")
    
    # Example 1: Full dataset with 2 workers
    run_benchmark(
        num_workers=2,
        mem_per_worker=10,
        cores_per_worker=4,
        dataset_scale=1.0,
        log_dir="./logs/example_full"
    )
    
    time.sleep(2)
    
    # Example 2: Half dataset with 1 worker
    run_benchmark(
        num_workers=1,
        mem_per_worker=8,
        cores_per_worker=4,
        dataset_scale=0.5,
        log_dir="./logs/example_half"
    )
    
    time.sleep(2)
    
    # Example 3: Small dataset for testing
    run_benchmark(
        num_workers=1,
        mem_per_worker=4,
        cores_per_worker=2,
        dataset_scale=0.1,
        log_dir="./logs/example_small"
    )
    
    print(f"\n{'='*60}")
    print("All benchmarks completed!")
    print("Check the logs/ directory for detailed results.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
