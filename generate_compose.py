import yaml
import sys

NUMA_NODES = {
    0: list(range(0, 24)),
    1: list(range(24, 48)),
    2: list(range(48, 72)),
    3: list(range(72, 96)),
}
ALL_CORES = [core for node_cores in NUMA_NODES.values() for core in node_cores]

def get_core_allocation(num_workers: int, cores_per_worker: int):
    """Allocate CPU cores to workers."""
    allocations = []
    core_idx = 0
    
    for _ in range(num_workers):
        if core_idx + cores_per_worker > len(ALL_CORES):
            print(f"Warning: Not enough cores for {num_workers} workers with {cores_per_worker} cores each.")
            break
        
        # Pin to a single NUMA node if possible
        if cores_per_worker <= 24:
            for node_id in range(len(NUMA_NODES)):
                node_start = node_id * 24
                node_end = node_start + 24
                if core_idx >= node_start and core_idx + cores_per_worker <= node_end:
                    break
            else: # if no suitable node found, may need to adjust core_idx
                for node_id in range(len(NUMA_NODES)):
                    node_start = node_id * 24
                    if core_idx < node_start:
                        core_idx = node_start
                        break
        
        if core_idx + cores_per_worker > len(ALL_CORES):
            break

        cores = ALL_CORES[core_idx : core_idx + cores_per_worker]
        allocations.append(",".join(map(str, cores)))
        core_idx += cores_per_worker
        
    return allocations


def generate_docker_compose(num_workers: int, mem_per_worker: int, cores_per_worker: int, output_file: str = "docker-compose.yaml"):
    """Generate docker-compose configuration for Spark cluster."""
    
    core_allocations = get_core_allocation(num_workers, cores_per_worker)
    if len(core_allocations) != num_workers:
        print(f"Error: Could not allocate cores for all workers. Requested: {num_workers}, Allocated: {len(core_allocations)}")
        sys.exit(1)

    spark_worker_cores = cores_per_worker

    compose_config = {
        "services": {
            "spark-master": {
                "image": "docker.io/bitnamilegacy/spark:latest",
                "container_name": "spark-master",
                "cpuset": "48-51",
                "environment": {
                    "SPARK_MODE": "master",
                    "SPARK_RPC_AUTHENTICATION_ENABLED": "no",
                    "SPARK_RPC_ENCRYPTION_ENABLED": "no",
                    "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED": "no",
                    "SPARK_SSL_ENABLED": "no",
                    "SPARK_USER": "spark",
                    "SPARK_DAEMON_MEMORY": "1g"
                },
                "ports": [
                    "8080:8080",
                    "7077:7077"
                ],
                "volumes": [
                    "./data:/app/data"
                ],
                "networks": ["spark-network"]
            },
            "fastapi": {
                "image": "iisc-deas-server",
                "container_name": "iisc-deas-server",
                "environment": {
                    "SPARK_MASTER_HOST": "spark-master",
                    "DATASET_PATH": "/app/data/combined_final.csv",
                    "DRIVER_MEMORY": "16g",
                    "http_proxy": "http://proxy-dmz.intel.com:912",
                    "https_proxy": "http://proxy-dmz.intel.com:912",
                    "no_proxy": "localhost,spark-master,spark-worker-1,spark-worker-2"
                },
                "ports": ["8001:8000", "4040:4040"],
                "volumes": [
                    # "./:/app",
                    "./data:/app/data",
                    "./logs:/app/logs"
                ],
                "depends_on": {},
                "networks": ["spark-network"]
            }
        },
        "networks": {
            "spark-network": {
                "driver": "bridge"
            }
        }
    }
    
    # Add spark-master to fastapi depends_on
    compose_config["services"]["fastapi"]["depends_on"]["spark-master"] = {
        "condition": "service_started"
    }
    
    for i in range(1, num_workers + 1):
        worker_name = f"spark-worker-{i}"
        compose_config["services"][worker_name] = {
            "image": "docker.io/bitnamilegacy/spark:latest",
            "container_name": worker_name,
            "cpuset": core_allocations[i-1],
            "environment": {
                "SPARK_MODE": "worker",
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "SPARK_WORKER_MEMORY": f"{mem_per_worker}G",
                "SPARK_WORKER_CORES": str(spark_worker_cores),
                "SPARK_RPC_AUTHENTICATION_ENABLED": "no",
                "SPARK_RPC_ENCRYPTION_ENABLED": "no",
                "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED": "no",
                "SPARK_SSL_ENABLED": "no",
                "SPARK_USER": "spark",
                "SPARK_DAEMON_MEMORY": "1g",
                "HOME": "/opt/bitnami/spark",
                "http_proxy": "http://proxy-dmz.intel.com:912",
                "https_proxy": "http://proxy-dmz.intel.com:912",
                "no_proxy": "localhost,spark-master,spark-worker-1,spark-worker-2"
            },
            "ports": [f"{8080 + i}:8081"],
            "volumes": [
                "./data:/app/data",
                "./worker-init.sh:/docker-entrypoint-initdb.d/worker-init.sh"
            ],
            "depends_on": ["spark-master"],
            "healthcheck": {
                "test": ["CMD", "python", "-c", "import requests; requests.get('http://localhost:8081').raise_for_status()"],
                "interval": "10s",
                "timeout": "5s",
                "retries": 100,
                "start_period": "30s"
            },
            "networks": ["spark-network"]
        }
        
        # Add this worker to fastapi's depends_on with health check condition
        compose_config["services"]["fastapi"]["depends_on"][worker_name] = {
            "condition": "service_healthy"
        }
    
    with open(output_file, 'w') as f:
        yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False)
    
    print(f"Generated {output_file} with {num_workers} workers, {mem_per_worker}GB memory, {cores_per_worker} cores each ({spark_worker_cores} for Spark)")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python generate_compose.py <num_workers> <mem_per_worker> <cores_per_worker>")
        sys.exit(1)
    
    num_workers = int(sys.argv[1])
    mem_per_worker = int(sys.argv[2])
    cores_per_worker = int(sys.argv[3])
    
    generate_docker_compose(num_workers, mem_per_worker, cores_per_worker)
