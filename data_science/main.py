from sparkmeasure import StageMetrics
from data_science.config import DATASET_PATH
import time
import os
import csv
import json
from pyspark import SparkContext, Broadcast
from pyspark.sql import DataFrame, functions as F

from data_science.connectors import getNewSparkSession
from data_science.utils import remove_stopwords, count_special_char, count_fake_keywords, eng_stopwords

keywords = ['breaking', 'urgent', 'exclusive', 'shocking', 'leaked', 'exposed', 'viral']

def run_data_cleaning(df: DataFrame, stopwords_bcst: Broadcast[list[str]]):
    df = df.withColumn("title_length", F.length(F.col("title"))).withColumn("text_length", F.length(F.col("text")))
    df = df.withColumn("title_word_count", F.array_size(F.split(F.col("title"), F.lit(" ")))) \
           .withColumn("text_word_count", F.array_size(F.split(F.col("text"), F.lit(" "))))
    df = df.withColumn("title_text_length_ratio", F.try_divide(F.col("title_length"), F.try_add(F.col("text_length"), F.lit(1))))
    df = df.withColumn("title_text_wordcount_ratio", F.try_divide(F.col("title_word_count"), F.try_add(F.col("text_word_count"), F.lit(1))))

    df = df.withColumn("title_exclamation_count", F.try_subtract(F.array_size(F.split(F.col("title"), F.lit("!"))), F.lit(1)))
    df = df.withColumn("text_exclamation_count", F.try_subtract(F.array_size(F.split(F.col("text"), F.lit("!"))), F.lit(1)))
    df = df.withColumn("title_question_count", F.length(F.col("title")) - F.length(F.regexp_replace(F.col("title"), r'\?', '')))
    df = df.withColumn("text_question_count", F.length(F.col("text")) - F.length(F.regexp_replace(F.col("text"), r'\?', '')))
    df = df.withColumn("title_caps_ratio", F.try_divide(F.length(F.regexp_replace(F.col("title"), r'[^A-Z]', '')), F.try_add(F.length(F.col("title")), F.lit(1))))
    df = df.withColumn("text_digit_count", F.length(F.regexp_replace(F.col("text"), r'[^0-9]', '')))
    df = df.withColumn("text_specialchar_count", count_special_char(F.col("text")))
    df = df.withColumn("contains_url", F.when(F.col("text").rlike(r'http[s]?://'), 1).otherwise(0))
    df = df.withColumn("fake_keywords_count", count_fake_keywords(F.col("text"), F.col("title"), F.lit(keywords)))

    df = df.withColumn("stopwords", F.lit(stopwords_bcst.value))
    df = df.withColumn("title_clean", remove_stopwords(F.regexp_replace(F.lower(F.col("title")), F.lit(r'[^a-zA-Z\s]'), F.lit("")), F.col("stopwords")))
    df = df.withColumn("text_clean", remove_stopwords(F.regexp_replace(F.lower(F.col("text")), F.lit(r'[^a-zA-Z\s]'), F.lit("")), F.col("stopwords")))
    df.collect()

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


def bench_pipeline(num_workers: int, mem_per_worker: int, cores_per_worker: int, dataset_scale: float, log_dir: str, remark: str = "", benchmark_name: str = ""):
    """
    Benchmark the Spark pipeline with given configuration.
    
    Args:
        num_workers: Number of worker nodes
        mem_per_worker: Memory per worker in GB
        cores_per_worker: Number of cores per worker
        dataset_scale: Scale factor for dataset (0 to 1)
        log_dir: Directory to save benchmark results
        remark: Short comment about the benchmark configuration
        benchmark_name: Name of the benchmark configuration
    
    Returns:
        Dictionary containing benchmark results
    """
    os.makedirs(log_dir, exist_ok=True)

    filename = f"{benchmark_name}_{num_workers}W-{cores_per_worker}C.csv" if benchmark_name else f"{num_workers}W-{cores_per_worker}C.csv"
    results_csv_path = os.path.join(log_dir, filename)

    print("Creating Spark session...\n")
    spark = getNewSparkSession(num_workers=num_workers, mem_per_worker=mem_per_worker, cores_per_worker=cores_per_worker)

    print("Loading dataset...\n")
    full_df = spark.read.csv(DATASET_PATH, header=True, inferSchema=True)
    
    # Replicate the dataset 5 times
    print("Replicating dataset 5 times...\n")
    replicated_df = full_df
    for i in range(4):
        replicated_df = replicated_df.union(full_df)
    
    total_rows = replicated_df.count()
    num_rows = int(total_rows * dataset_scale)
    df = replicated_df.limit(num_rows)
    
    sc = SparkContext.getOrCreate()
    stopwords_bcst = sc.broadcast(eng_stopwords)
    
    stagemetrics = StageMetrics(spark)
    
    start_e2e_time = time.time()
    stagemetrics.begin()

    run_data_cleaning(df=df, stopwords_bcst=stopwords_bcst)

    stagemetrics.end()
    e2e_time = time.time() - start_e2e_time
    stagemetrics.print_report()
    df_metrics = stagemetrics.create_stagemetrics_DF()
    
    # Convert DataFrame to list of dictionaries
    stage_metrics_list = df_metrics.orderBy("jobId", "stageId").collect()
    
    results = {
        "num_workers": num_workers,
        "mem_per_worker": mem_per_worker,
        "cores_per_worker": cores_per_worker,
        "dataset_scale": dataset_scale,
        "num_rows": num_rows,
        "num_stages": len(stage_metrics_list),
        "E2E_time": e2e_time,
        "E2E_throughput": num_rows / e2e_time if e2e_time > 0 else 0,
        "remark": remark
    }
    
    for i, stage_row in enumerate(stage_metrics_list):
        stage_duration_ms = stage_row.stageDuration if hasattr(stage_row, 'stageDuration') else 0
        stage_duration_s = stage_duration_ms / 1000.0
        records_read = stage_row.recordsRead if hasattr(stage_row, 'recordsRead') else 0
        
        results[f"stage{i}_time"] = stage_duration_s
        results[f"stage{i}_throughput"] = records_read / stage_duration_s if stage_duration_s > 0 else 0
        results[f"stage{i}_executorRunTime"] = stage_row.executorRunTime if hasattr(stage_row, 'executorRunTime') else 0
        results[f"stage{i}_executorCpuTime"] = stage_row.executorCpuTime if hasattr(stage_row, 'executorCpuTime') else 0
        results[f"stage{i}_jvmGCTime"] = stage_row.jvmGCTime if hasattr(stage_row, 'jvmGCTime') else 0
        results[f"stage{i}_recordsRead"] = records_read
        results[f"stage{i}_bytesRead"] = stage_row.bytesRead if hasattr(stage_row, 'bytesRead') else 0
    
    fieldnames = ["num_workers", "mem_per_worker", "cores_per_worker", "dataset_scale", "num_rows", "num_stages", "remark"]
    
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
    
    with open(results_csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        writer.writerow(results)
    
    results["results_file"] = results_csv_path
    
    return results

    
    