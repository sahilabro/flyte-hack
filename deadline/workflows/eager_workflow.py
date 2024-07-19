from flytekit import task, workflow, dynamic
from flytekit.experimental import eager
from typing import List, Dict, Any
import random

# Simulating the DeadlinePlugin functionality
def submit_job(job_info: Dict[str, Any]) -> str:
    return f"job_id_{random.randint(1000, 9999)}"

def poll_job(job_id: str) -> None:
    print(f"Polling job {job_id}")

def get_job_output(job_id: str) -> Dict[str, Any]:
    return {"job_id": job_id, "status": "completed"}

job_info_input = {
    "name": "test_job",
    "plugin": "Blender",
    "frames": "1-100"
}

@task
def submit_deadline_job(job_info: Dict[str, Any]) -> str:
    job_id = submit_job(job_info)
    poll_job(job_id)
    return job_id

@task
def process_job_output(job_id: str) -> Dict[str, Any]:
    job_output = get_job_output(job_id)
    print(f"Job Output for job_id {job_id}: {job_output}")
    return job_output

@task
def finalize_job_outputs(job_outputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    print(f"Final job outputs: {job_outputs}")
    return job_outputs

@eager
def submit_and_process_jobs() -> List[Dict[str, Any]]:
    job_infos = [job_info_input] * random.randint(1, 5)
    job_outputs = []
    for job_info in job_infos:
        job_id = submit_deadline_job(job_info)
        job_output = process_job_output(job_id)
        job_outputs.append(job_output)
    return job_outputs

@workflow
def deadline_fanout_workflow() -> List[Dict[str, Any]]:
    job_outputs = submit_and_process_jobs()
    return finalize_job_outputs(job_outputs=job_outputs)

if __name__ == "__main__":
    print("Running deadline_fanout_workflow locally")
    result = deadline_fanout_workflow()
    print(f"Workflow execution completed. Result: {result}")