import os
os.environ["PREFECT_API_URL"] = "https://prefect.mariothedev.com/api"
os.environ["PREFECT_API_AUTH_STRING"] = "Pk@123@fl"

from prefect import flow

flow.from_source(
    source="https://github.com/mariothedev/prefect-flows.git",
    entrypoint="hello.py:hello_pipeline",
).deploy(name="hello", work_pool_name="my-pool", cron="* * * * *")

# Daily at 03:00 UTC. The worker needs requirements.txt installed, MLflow + MinIO
# credentials in its environment, and git push access (the flow commits + tags data.dvc).
flow.from_source(
    source="https://github.com/mariothedev/prefect-flows.git",
    entrypoint="training_data.py:training_data_pipeline",
).deploy(name="training-data", work_pool_name="my-pool", cron="0 3 * * *")
