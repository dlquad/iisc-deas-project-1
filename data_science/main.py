from connectors import getNewSparkSession
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics
from config import DATASET_PATH
import time
import os
import csv
import json


def run_data_cleaning(spark: SparkSession, df):
    """
    Execute the data cleaning pipeline.
    
    Args:
        spark: SparkSession object
        df: Input DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    
    # Make this pipeline a little more complex for benchmarking
    df = df.withColumn("dwelling_type", df["dwelling_type"].cast("string"))
    df = df.filter(df["dwelling_type"].isNotNull())
    clean_df = df.drop("dwelling_type")
    clean_df.collect()
    return clean_df


def extract_stage_metrics(stagemetrics_dir: str):
    """
    Extract stage metrics from the JSON file created by sparkmeasure.
    
    Args:
        stagemetrics_dir: Directory containing the stage metrics JSON files
    
    Returns:
        List of dictionaries containing stage metrics
    """
    json_files = [f for f in os.listdir(stagemetrics_dir) if f.endswith('.json')]
    
    if not json_files:
        return []
    
    json_file_path = os.path.join(stagemetrics_dir, json_files[0])
    
    stage_metrics = []
    with open(json_file_path, 'r') as f:
        for line in f:
            if line.strip():
                stage_metrics.append(json.loads(line))
    
    return stage_metrics


def bench_pipeline(num_workers: int, mem_per_worker: int, cores_per_worker: int, dataset_scale: float, log_dir: str):
    """
    Benchmark the Spark pipeline with given configuration.
    
    Args:
        num_workers: Number of worker nodes
        mem_per_worker: Memory per worker in GB
        cores_per_worker: Number of cores per worker
        dataset_scale: Scale factor for dataset (0 to 1)
        log_dir: Directory to save benchmark results
    
    Returns:
        Dictionary containing benchmark results
    """
    os.makedirs(log_dir, exist_ok=True)
    
    results_csv_path = os.path.join(log_dir, "results.csv")
    csv_exists = os.path.exists(results_csv_path)
    
    spark = getNewSparkSession(num_workers=num_workers, mem_per_worker=mem_per_worker, cores_per_worker=cores_per_worker)
    
    full_df = spark.read.csv(DATASET_PATH, header=True, inferSchema=True)
    
    total_rows = full_df.count()
    num_rows = int(total_rows * dataset_scale)
    
    df = full_df.limit(num_rows) if dataset_scale < 1.0 else full_df
    
    stagemetrics = StageMetrics(spark)
    stagemetrics_dir = os.path.join(log_dir, f"stagemetrics_{int(time.time())}")
    
    start_e2e_time = time.time()
    stagemetrics.begin()
    
    run_data_cleaning(spark=spark, df=df)
    
    stagemetrics.end()
    e2e_time = time.time() - start_e2e_time
    
    df_metrics = stagemetrics.create_stagemetrics_DF()
    stagemetrics.save_data(df_metrics.orderBy("jobId", "stageId"), stagemetrics_dir, fileformat="json")
    
    stage_metrics_list = extract_stage_metrics(stagemetrics_dir)
    
    results = {
        "num_workers": num_workers,
        "mem_per_worker": mem_per_worker,
        "cores_per_worker": cores_per_worker,
        "dataset_scale": dataset_scale,
        "num_rows": num_rows,
        "num_stages": len(stage_metrics_list),
        "E2E_time": e2e_time,
        "E2E_throughput": num_rows / e2e_time if e2e_time > 0 else 0
    }
    
    for i, stage_metric in enumerate(stage_metrics_list):
        stage_duration_ms = stage_metric.get("stageDuration", 0)
        stage_duration_s = stage_duration_ms / 1000.0
        records_read = stage_metric.get("recordsRead", 0)
        
        results[f"stage{i}_time"] = stage_duration_s
        results[f"stage{i}_throughput"] = records_read / stage_duration_s if stage_duration_s > 0 else 0
        results[f"stage{i}_executorRunTime"] = stage_metric.get("executorRunTime", 0)
        results[f"stage{i}_executorCpuTime"] = stage_metric.get("executorCpuTime", 0)
        results[f"stage{i}_jvmGCTime"] = stage_metric.get("jvmGCTime", 0)
        results[f"stage{i}_recordsRead"] = records_read
        results[f"stage{i}_bytesRead"] = stage_metric.get("bytesRead", 0)
    
    # Pad missing stages if fewer than NUM_STAGES
    # for i in range(len(stage_metrics_list), NUM_STAGES):
    #     results[f"stage{i}_time"] = 0
    #     results[f"stage{i}_throughput"] = 0
    #     results[f"stage{i}_executorRunTime"] = 0
    #     results[f"stage{i}_executorCpuTime"] = 0
    #     results[f"stage{i}_jvmGCTime"] = 0
    #     results[f"stage{i}_recordsRead"] = 0
    #     results[f"stage{i}_bytesRead"] = 0
    
    fieldnames = ["num_workers", "mem_per_worker", "cores_per_worker", "dataset_scale", "num_rows", "num_stages"]
    
    for i in range(len(stage_metrics_list)):
        fieldnames.extend([
            f"stage{i}_time",
            f"stage{i}_throughput",
            f"stage{i}_executorRunTime",
            f"stage{i}_executorCpuTime",
            f"stage{i}_jvmGCTime",
            f"stage{i}_recordsRead",
            f"stage{i}_bytesRead"
        ])
    
    fieldnames.extend(["E2E_time", "E2E_throughput"])
    
    with open(results_csv_path, 'a' if csv_exists else 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not csv_exists:
            writer.writeheader()
        
        writer.writerow(results)
    
    results["results_file"] = results_csv_path
    
    return results

    
    