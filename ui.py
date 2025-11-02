import gradio as gr
import requests
import pandas as pd
import os
from typing import List, Tuple
import glob
import time


API_URL = "http://localhost:8000"


def run_benchmark_ui(num_workers: int, mem_per_worker: int, cores_per_worker: int, dataset_scale: float, log_dir: str, progress=gr.Progress()) -> Tuple[str, str]:
    """
    Run benchmark through the API and return results.
    
    Args:
        num_workers: Number of worker nodes
        mem_per_worker: Memory per worker in GB
        cores_per_worker: Number of cores per worker
        dataset_scale: Dataset scale factor (0 to 1)
        log_dir: Directory to save results
        progress: Gradio progress tracker
    
    Returns:
        Tuple of (status message, results dataframe as markdown)
    """
    start_time = time.time()
    
    try:
        # Show initial progress
        progress(0, desc="🚀 Initializing benchmark...")
        
        # Prepare configuration message
        config_msg = f"""
⏳ **Benchmark Running...**

**Configuration:**
- Workers: {num_workers}
- Memory per Worker: {mem_per_worker} GB
- Cores per Worker: {cores_per_worker}
- Dataset Scale: {dataset_scale}
- Log Directory: {log_dir}

**Status:** Sending request to API...
"""
        
        progress(0.2, desc="📡 Sending request to API...")
        time.sleep(0.5)  # Brief pause for visual feedback
        
        # Make API request
        progress(0.3, desc="⚙️ Spark session initializing...")
        response = requests.post(
            f"{API_URL}/benchmark",
            json={
                "num_workers": num_workers,
                "mem_per_worker": mem_per_worker,
                "cores_per_worker": cores_per_worker,
                "dataset_scale": dataset_scale,
                "log_dir": log_dir
            },
            timeout=300  # 5 minutes timeout
        )
        
        progress(0.7, desc="📊 Processing benchmark results...")
        
        if response.status_code == 200:
            progress(0.9, desc="✅ Finalizing...")
            
            data = response.json()
            elapsed_time = time.time() - start_time
            
            # Format the response
            message = f"""
✅ **Benchmark Completed Successfully!**

**Configuration:**
- Workers: {num_workers}
- Memory per Worker: {mem_per_worker} GB
- Cores per Worker: {cores_per_worker}
- Dataset Scale: {dataset_scale}

**Results:**
- Rows Processed: {data['num_rows']:,}
- Number of Stages: {data['num_stages']}
- E2E Time: {data['E2E_time']:.2f} seconds
- E2E Throughput: {data['E2E_throughput']:.2f} rows/sec
- Total Runtime (including API overhead): {elapsed_time:.2f} seconds
- Results File: {data['results_file']}
"""
            
            # Load and return the results CSV
            progress(1.0, desc="✅ Complete!")
            results_df = load_results_from_file(data['results_file'])
            
            return message, results_df
        else:
            error_msg = f"❌ **Benchmark Failed**\n\nStatus Code: {response.status_code}\n\n{response.text}"
            return error_msg, ""
    
    except requests.exceptions.Timeout:
        return "❌ **Benchmark Failed**\n\nRequest timed out after 5 minutes.", ""
    except Exception as e:
        return f"❌ **Benchmark Failed**\n\n{str(e)}", ""


def load_results_from_file(file_path: str) -> str:
    """
    Load results from CSV file and return as formatted markdown table.
    
    Args:
        file_path: Path to the results CSV file
    
    Returns:
        Formatted markdown table
    """
    try:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            return df.to_markdown(index=False)
        else:
            return "Results file not found."
    except Exception as e:
        return f"Error loading results: {str(e)}"


def load_all_results(log_dir_pattern: str = "./logs/*") -> str:
    """
    Load all results from log directories matching the pattern.
    
    Args:
        log_dir_pattern: Glob pattern for log directories
    
    Returns:
        Combined results as markdown table
    """
    try:
        # Find all results.csv files
        results_files = glob.glob(f"{log_dir_pattern}/results.csv")
        
        if not results_files:
            return "No results found. Run a benchmark first!"
        
        # Load and combine all results
        all_dfs = []
        for file_path in results_files:
            try:
                df = pd.read_csv(file_path)
                df['log_dir'] = os.path.dirname(file_path)
                all_dfs.append(df)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
        
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            # Sort by E2E_time
            combined_df = combined_df.sort_values('E2E_time')
            return combined_df.to_markdown(index=False)
        else:
            return "No valid results found."
    
    except Exception as e:
        return f"Error loading results: {str(e)}"


def check_api_health() -> str:
    """Check if the API is running."""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        if response.status_code == 200:
            return "✅ API is running and healthy"
        else:
            return f"⚠️ API responded with status code: {response.status_code}"
    except requests.exceptions.ConnectionError:
        return "❌ Cannot connect to API. Please ensure the FastAPI server is running on port 8000."
    except Exception as e:
        return f"❌ Error checking API health: {str(e)}"


def update_status_running(num_workers, mem_per_worker, cores_per_worker, dataset_scale, log_dir):
    """Update status to show benchmark is running."""
    timestamp = time.strftime("%H:%M:%S")
    return f"🔄 [{timestamp}] Benchmark RUNNING - Workers: {num_workers}, Memory: {mem_per_worker}GB, Cores: {cores_per_worker}, Scale: {dataset_scale}"


def update_status_complete():
    """Update status to show benchmark is complete."""
    timestamp = time.strftime("%H:%M:%S")
    return f"✅ [{timestamp}] Benchmark COMPLETED successfully!"


def update_status_ready():
    """Reset status to ready."""
    return "Ready to run benchmark"


# Create Gradio interface
with gr.Blocks(title="Spark Pipeline Benchmark", theme=gr.themes.Soft()) as demo:
    gr.Markdown(
        """
        # 🚀 Spark Pipeline Benchmark Tool
        
        Configure and run benchmarks for your Spark data processing pipeline.
        Monitor performance metrics and compare different configurations.
        """
    )
    
    # API Health Status
    with gr.Row():
        api_status = gr.Textbox(label="API Status", value=check_api_health(), interactive=False)
        refresh_btn = gr.Button("🔄 Refresh Status", size="sm")
    
    refresh_btn.click(fn=check_api_health, outputs=api_status)
    
    gr.Markdown("---")
    
    # Benchmark Configuration
    gr.Markdown("## ⚙️ Benchmark Configuration")
    
    with gr.Row():
        with gr.Column():
            num_workers = gr.Slider(
                minimum=1,
                maximum=10,
                value=2,
                step=1,
                label="Number of Workers",
                info="Number of worker nodes in the Spark cluster"
            )
            
            mem_per_worker = gr.Slider(
                minimum=1,
                maximum=32,
                value=10,
                step=1,
                label="Memory per Worker (GB)",
                info="Memory allocated to each worker"
            )
            
            cores_per_worker = gr.Slider(
                minimum=1,
                maximum=16,
                value=4,
                step=1,
                label="Cores per Worker",
                info="Number of CPU cores per worker"
            )
        
        with gr.Column():
            dataset_scale = gr.Slider(
                minimum=0.0,
                maximum=1.0,
                value=1.0,
                step=0.1,
                label="Dataset Scale",
                info="Scale factor for dataset size (0 = 0%, 1 = 100%)"
            )
            
            log_dir = gr.Textbox(
                value="./logs/benchmark_1",
                label="Log Directory",
                info="Directory to save benchmark results"
            )
    
    # Run Benchmark Button
    run_btn = gr.Button("▶️ Run Benchmark", variant="primary", size="lg")
    
    # Status indicator
    with gr.Row():
        benchmark_status = gr.Textbox(
            label="Benchmark Status",
            value="Ready to run benchmark",
            interactive=False,
            lines=2
        )
    
    # Results Section
    gr.Markdown("---")
    gr.Markdown("## 📊 Benchmark Results")
    
    with gr.Tabs():
        with gr.Tab("Current Run"):
            status_output = gr.Markdown(label="Status")
            results_output = gr.Markdown(label="Results")
        
        with gr.Tab("All Results"):
            gr.Markdown("View all benchmark results from all log directories")
            log_pattern = gr.Textbox(
                value="./logs/*",
                label="Log Directory Pattern",
                info="Glob pattern to search for results (e.g., ./logs/*)"
            )
            load_all_btn = gr.Button("📂 Load All Results", size="sm")
            all_results_output = gr.Markdown(label="All Results")
    
    # Connect the buttons
    run_btn.click(
        fn=update_status_running,
        inputs=[num_workers, mem_per_worker, cores_per_worker, dataset_scale, log_dir],
        outputs=[benchmark_status]
    ).then(
        fn=run_benchmark_ui,
        inputs=[num_workers, mem_per_worker, cores_per_worker, dataset_scale, log_dir],
        outputs=[status_output, results_output]
    ).then(
        fn=update_status_complete,
        outputs=[benchmark_status]
    )
    
    load_all_btn.click(
        fn=load_all_results,
        inputs=[log_pattern],
        outputs=[all_results_output]
    )
    
    # Footer
    gr.Markdown(
        """
        ---
        **Note:** Make sure the FastAPI server is running before running benchmarks.
        Start the server with: `python main.py`
        """
    )


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)
