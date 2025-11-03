import requests
import time
import json
import sys
from datetime import datetime

API_URL = "http://localhost:8000"


def check_api_health(max_retries=30, retry_delay=2):
    """Check if the API is running with retries."""
    for i in range(max_retries):
        try:
            response = requests.get(f"{API_URL}/health", timeout=5)
            if response.status_code == 200:
                return True
        except:
            pass
        time.sleep(retry_delay)
    return False


def run_benchmark(num_workers, mem_per_worker, cores_per_worker, dataset_scale, log_dir, remark=""):
    """Run a benchmark with specified configuration."""
    
    print(f"\n{'='*60}")
    print(f"Running Benchmark:")
    print(f"  Workers: {num_workers}")
    print(f"  Memory per Worker: {mem_per_worker} GB")
    print(f"  Cores per Worker: {cores_per_worker}")
    print(f"  Dataset Scale: {dataset_scale}")
    print(f"  Log Directory: {log_dir}")
    print(f"  Remark: {remark}")
    print(f"{'='*60}\n")
    
    try:
        response = requests.post(
            f"{API_URL}/benchmark",
            json={
                "num_workers": num_workers,
                "mem_per_worker": mem_per_worker,
                "cores_per_worker": cores_per_worker,
                "dataset_scale": dataset_scale,
                "log_dir": log_dir,
                "remark": remark
            },
            timeout=600
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
    """Main function to run benchmarks from config."""
    
    if len(sys.argv) < 2:
        print("Usage: python benchmark.py <config_file>")
        print("Example: python benchmark.py benchmark_configs.json")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    try:
        with open(config_file, 'r') as f:
            configs = json.load(f)
    except Exception as e:
        print(f"❌ Error loading config file: {e}")
        sys.exit(1)
    
    print("Waiting for API to be ready...")
    if not check_api_health():
        print("❌ API is not responding. Check if the deployment is healthy.")
        sys.exit(1)
    
    print("✅ API is ready\n")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for config in configs:
        config_name = config.get("name", "unnamed")
        timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_log_dir = f"./logs/{timestamp}/{config_name}"
        log_dir = config.get("log_dir", default_log_dir)
        remark = config.get("remark", "")
        
        print(f"\n{'#'*60}")
        print(f"# Configuration: {config_name}")
        print(f"{'#'*60}")
        
        result = run_benchmark(
            num_workers=config["num_workers"],
            mem_per_worker=config["mem_per_worker"],
            cores_per_worker=config["cores_per_worker"],
            dataset_scale=config["dataset_scale"],
            log_dir=log_dir,
            remark=remark
        )
        
        if not result:
            print(f"⚠️  Benchmark {config_name} failed, continuing to next...")
        
        time.sleep(3)
    
    print(f"\n{'='*60}")
    print("All benchmarks completed!")
    print(f"Results saved in: ./logs/{timestamp}/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
