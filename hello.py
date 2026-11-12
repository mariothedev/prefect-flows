from prefect import flow, task

@task(retries=2)
def extract():
    return ["hello", "world"]

@task
def transform(words):
    return " ".join(w.upper() for w in words)

@task
def load(text):
    print(text)

@flow(log_prints=True)
def hello_pipeline():
    load(transform(extract()))
