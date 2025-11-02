import os

SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST", "local[*]")
DATASET_PATH = os.getenv("DATASET_PATH", "./train.csv")
DRIVER_MEMORY = os.getenv("DRIVER_MEMORY", "16g")
# NUM_STAGES = int(os.getenv("NUM_STAGES", "3"))  
