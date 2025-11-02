from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any
import uvicorn
from data_science.main import bench_pipeline

app = FastAPI(title="Spark Pipeline Benchmark API", version="1.0.0")


class BenchmarkRequest(BaseModel):
    """Request model for benchmark API"""
    num_workers: int = Field(..., ge=1, description="Number of worker nodes")
    mem_per_worker: int = Field(..., ge=1, description="Memory per worker in GB")
    cores_per_worker: int = Field(..., ge=1, description="Number of cores per worker")
    dataset_scale: float = Field(..., ge=0.0, le=1.0, description="Dataset scale factor (0 to 1)")
    log_dir: str = Field(..., description="Directory to save benchmark results")


class BenchmarkResponse(BaseModel):
    """Response model for benchmark API"""
    success: bool
    E2E_time: float
    E2E_throughput: float
    num_rows: int
    num_stages: int
    results_file: str
    message: str


@app.get("/")
def root():
    """Root endpoint"""
    return {
        "message": "Spark Pipeline Benchmark API",
        "endpoints": {
            "/benchmark": "POST - Run a benchmark with specified configuration",
            "/health": "GET - Health check"
        }
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.post("/benchmark", response_model=BenchmarkResponse)
def run_benchmark(request: BenchmarkRequest) -> Dict[str, Any]:
    """
    Run a benchmark with the specified configuration.
    
    Args:
        request: BenchmarkRequest containing configuration parameters
    
    Returns:
        BenchmarkResponse with E2E metrics and results file location
    """
    try:
        # Run the benchmark pipeline
        results = bench_pipeline(
            num_workers=request.num_workers,
            mem_per_worker=request.mem_per_worker,
            cores_per_worker=request.cores_per_worker,
            dataset_scale=request.dataset_scale,
            log_dir=request.log_dir
        )
        
        return {
            "success": True,
            "E2E_time": results["E2E_time"],
            "E2E_throughput": results["E2E_throughput"],
            "num_rows": results["num_rows"],
            "num_stages": results["num_stages"],
            "results_file": results["results_file"],
            "message": "Benchmark completed successfully"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Benchmark failed: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

