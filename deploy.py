import os
os.environ["PREFECT_API_URL"] = "https://prefect.mariothedev.com/api"
os.environ["PREFECT_API_AUTH_STRING"] = "Pk@123@fl"

from prefect import flow

flow.from_source(
    source="https://github.com/mariothedev/prefect-flows.git",
    entrypoint="hello.py:hello_pipeline",
).deploy(name="hello", work_pool_name="my-pool", cron="* * * * *")
