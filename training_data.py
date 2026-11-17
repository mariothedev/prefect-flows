"""Training data pipeline: MLflow traces -> versioned training datasets.

Steps (one Prefect task each):
  1. sync      reset data/ to the last published version (dvc pull)
  2. extract   fetch traces newer than the last extraction from MLflow
  3. flatten   one flat row per trace -> data/raw/traces_<ts>.parquet
  4. transform dbt build in transform/: clean, filter, dedupe, split + data tests
  5. export    data/datasets/{train,eval}.jsonl in chat fine-tuning format
  6. version   dvc add + dvc push, git commit data.dvc, git tag dataset-vN
  7. register  log the version to the MLflow "foliolake-training-data" experiment

Configuration (environment):
  MLFLOW_TRACKING_URI, MLFLOW_TRACKING_USERNAME, MLFLOW_TRACKING_PASSWORD
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  (MinIO keys; or set them in .dvc/config.local)
  The machine running the flow also needs git push access to this repo.

Get a dataset version back:
  git checkout dataset-v3 && dvc pull
  or in Python: dvc.api.open("data/datasets/train.jsonl", rev="dataset-v3")
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import mlflow
import mlflow.data
import yaml
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

DEFAULT_TRACKING_URI = "https://mlflow.mariothedev.com"

REPO = Path(__file__).resolve().parent
DATA = REPO / "data"
RAW = DATA / "raw"
CURATED = DATA / "curated"
DATASETS = DATA / "datasets"
TRANSFORM = REPO / "transform"
DVC_FILE = REPO / "data.dvc"
TAG_PREFIX = "dataset-v"

# Schema of the raw layer. Parquet stores these types, so every later read agrees on them.
RAW_SCHEMA = {
    "trace_id": "VARCHAR",
    "request_time_ms": "BIGINT",
    "state": "VARCHAR",
    "trace_name": "VARCHAR",
    "session_id": "VARCHAR",
    "question": "VARCHAR",
    "history_json": "VARCHAR",
    "answer": "VARCHAR",  # what the LLM generated
    "reply": "VARCHAR",  # what the app returned (answer + appended sources)
    "context": "VARCHAR",  # retrieved text the LLM saw (tool results)
    "route": "VARCHAR",
    "guardrail": "VARCHAR",
    "hhem_score": "DOUBLE",
    "faithfulness": "DOUBLE",
    "answer_relevancy": "DOUBLE",
    "context_utilization": "DOUBLE",
    "model": "VARCHAR",
    "messages_json": "VARCHAR",  # full conversation of the final LLM call, OpenAI format
    "tools_json": "VARCHAR",  # tool definitions offered to the LLM
    "extracted_at": "TIMESTAMP",
}
MESSAGE_KEYS = ("role", "content", "tool_calls", "tool_call_id", "name")


def run(*cmd: str) -> str:
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"`{' '.join(cmd)}` failed:\n{proc.stderr or proc.stdout}")
    return proc.stdout.strip()


def dvc(*args: str) -> str:
    return run(sys.executable, "-m", "dvc", *args)


def git(*args: str) -> str:
    return run("git", *args)


def sql_str(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def scalar(query: str):
    return duckdb.sql(query).fetchall()[0][0]


# ---------------------------------------------------------------------------- flatten


def clean_message(message: dict) -> dict:
    return {k: message[k] for k in MESSAGE_KEYS if message.get(k) is not None}


def flatten_trace(trace, extracted_at: datetime) -> dict:
    """Turn one nested MLflow trace into one flat row matching RAW_SCHEMA."""
    info, spans = trace.info, trace.data.spans
    root = next((s for s in spans if s.parent_id is None), None)
    inputs = root.inputs if root and isinstance(root.inputs, dict) else {}
    outputs = root.outputs if root and isinstance(root.outputs, dict) else {}

    row = dict.fromkeys(RAW_SCHEMA)
    row.update(
        trace_id=info.trace_id,
        request_time_ms=info.request_time,
        state=getattr(info.state, "value", info.state),
        trace_name=info.tags.get("mlflow.traceName") or (root.name if root else None),
        session_id=info.trace_metadata.get("mlflow.trace.session"),
        question=inputs.get("message"),
        history_json=json.dumps(inputs["history"], ensure_ascii=False) if "history" in inputs else None,
        reply=outputs.get("reply"),
        route=outputs.get("route"),
        guardrail=info.tags.get("guardrail"),
        hhem_score=outputs.get("hhem_score"),
        extracted_at=extracted_at,
    )

    # Judge scores (RAGAS) are assessments on the trace; keep the latest valid one per name.
    for a in sorted(info.assessments or [], key=lambda a: a.last_update_time_ms or 0):
        value = a.feedback.value if a.feedback is not None and a.valid is not False else None
        if a.name in row and isinstance(value, (int, float)):
            row[a.name] = float(value)

    # The last LLM call holds the exact prompt the model saw and its final answer.
    llm_calls = sorted((s for s in spans if s.span_type == "CHAT_MODEL"), key=lambda s: s.start_time_ns)
    if llm_calls:
        last = llm_calls[-1]
        prompt = (last.inputs or {}).get("messages") or []
        choices = (last.outputs or {}).get("choices") or []
        if prompt and choices and choices[0].get("message"):
            messages = [clean_message(m) for m in [*prompt, choices[0]["message"]]]
            tools = last.attributes.get("mlflow.chat.tools")
            row.update(
                answer=messages[-1].get("content"),
                context="\n\n".join(m["content"] for m in messages if m["role"] == "tool" and m.get("content")),
                model=last.attributes.get("mlflow.llm.model"),
                messages_json=json.dumps(messages, ensure_ascii=False),
                tools_json=json.dumps(tools, ensure_ascii=False) if tools else None,
            )
    return row


def last_extracted_ms() -> int:
    if not any(RAW.glob("*.parquet")):
        return 0
    return scalar(f"select max(request_time_ms) from read_parquet({sql_str(RAW / '*.parquet')})") or 0


# ---------------------------------------------------------------------------- tasks


@task(cache_policy=NO_CACHE)
def sync_data() -> None:
    """Start from the last published version. Leftovers of a failed run are discarded."""
    shutil.rmtree(DATA, ignore_errors=True)
    if DVC_FILE.exists():
        dvc("pull", "data.dvc")
        print(f"Pulled data for {DVC_FILE.name}")


@task(retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
def extract_traces(experiment: str, settle_minutes: int) -> tuple[list, int, int]:
    """Traces newer than the last extraction, but at least `settle_minutes` old.

    The wait lets the LLM judge attach its scores before we read a trace; once the
    watermark passes a trace it is never fetched again.
    """
    since = last_extracted_ms()
    until = int(time.time() * 1000) - settle_minutes * 60_000
    exp = mlflow.get_experiment_by_name(experiment)
    if exp is None:
        raise ValueError(f"MLflow experiment {experiment!r} not found")
    traces = mlflow.search_traces(
        locations=[exp.experiment_id],
        filter_string=f"trace.timestamp_ms > {since} AND trace.timestamp_ms <= {until}",
        return_type="list",
    )
    print(f"Found {len(traces)} new traces in ({since}, {until}]")
    return traces, since, until


@task(cache_policy=NO_CACHE)
def write_raw(traces: list) -> Path:
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    rows = [flatten_trace(t, now) for t in traces]
    RAW.mkdir(parents=True, exist_ok=True)
    path = RAW / f"traces_{now:%Y%m%dT%H%M%SZ}.parquet"

    con = duckdb.connect()
    con.execute(f"create table raw ({', '.join(f'{c} {t}' for c, t in RAW_SCHEMA.items())})")
    placeholders = ", ".join(["?"] * len(RAW_SCHEMA))
    con.executemany(f"insert into raw values ({placeholders})", [tuple(r[c] for c in RAW_SCHEMA) for r in rows])
    con.execute(f"copy (select * from raw order by request_time_ms, trace_id) to {sql_str(path)} (format parquet)")
    print(f"Wrote {len(rows)} rows to {path.relative_to(REPO)}")
    return path


@task(cache_policy=NO_CACHE)
def transform(faithfulness_min: float, answer_relevancy_min: float, eval_pct: int) -> None:
    from dbt.cli.main import dbtRunner

    (TRANSFORM / "target").mkdir(exist_ok=True)
    CURATED.mkdir(parents=True, exist_ok=True)
    os.environ["DBT_DUCKDB_PATH"] = str(TRANSFORM / "target" / "warehouse.duckdb")
    dbt_vars = {
        "raw_glob": str(RAW / "*.parquet"),
        "curated_dir": str(CURATED),
        "faithfulness_min": faithfulness_min,
        "answer_relevancy_min": answer_relevancy_min,
        "eval_pct": eval_pct,
    }
    args = ["build", "--project-dir", str(TRANSFORM), "--profiles-dir", str(TRANSFORM), "--vars", json.dumps(dbt_vars)]
    if not dbtRunner().invoke(args).success:
        raise RuntimeError("dbt build failed (a model or data test failed); nothing was versioned")


@task(cache_policy=NO_CACHE)
def export_jsonl() -> dict[str, int]:
    """Write train/eval JSONL: one {"messages": [...], "tools": [...]} object per line."""
    DATASETS.mkdir(parents=True, exist_ok=True)
    query = f"""
        select split, messages_json, tools_json
        from read_parquet({sql_str(CURATED / 'training_examples.parquet')})
        order by trace_id
    """
    rows = duckdb.sql(query).fetchall()
    counts = {"raw_traces": scalar(f"select count(*) from read_parquet({sql_str(RAW / '*.parquet')})")}
    for split in ("train", "eval"):
        lines = []
        for row_split, messages_json, tools_json in rows:
            if row_split == split:
                example = {"messages": json.loads(messages_json)}
                if tools_json:
                    example["tools"] = json.loads(tools_json)
                lines.append(json.dumps(example, ensure_ascii=False) + "\n")
        (DATASETS / f"{split}.jsonl").write_text("".join(lines), encoding="utf-8")
        counts[f"{split}_rows"] = len(lines)
    print(f"Exported {counts}")
    return counts


def next_tag() -> str:
    numbers = [int(t[len(TAG_PREFIX):]) for t in git("tag", "--list", f"{TAG_PREFIX}*").split() if re.fullmatch(rf"{TAG_PREFIX}\d+", t)]
    return f"{TAG_PREFIX}{max(numbers, default=0) + 1}"


@task(cache_policy=NO_CACHE)
def version_data(push_git: bool) -> dict | None:
    """Snapshot data/ with DVC, upload it, then commit + tag the pointer file in git."""
    dvc("add", "data")
    if not git("status", "--porcelain", "--", DVC_FILE.name):
        return None  # byte-for-byte identical to the last version

    dvc("push", DVC_FILE.name)  # data first, so a published tag always has its data
    if push_git:
        git("fetch", "--tags", "origin")  # see tags created by other machines
    tag = next_tag()
    git("add", DVC_FILE.name)
    git("commit", "-m", f"data: {tag}", "--", DVC_FILE.name)
    git("tag", "-a", tag, "-m", f"Training dataset {tag}")
    if push_git:
        git("push", "origin", "HEAD")
        git("push", "origin", tag)

    remote = dvc("remote", "default")
    version = {
        "tag": tag,
        "commit": git("rev-parse", "HEAD"),
        "dvc_md5": yaml.safe_load(DVC_FILE.read_text())["outs"][0]["md5"],
        "dvc_remote_url": dvc("config", f"remote.{remote}.url"),
    }
    print(f"Versioned {version}")
    return version


@task(cache_policy=NO_CACHE)
def register(version: dict, counts: dict, params: dict, experiment: str, tracking_uri: str | None) -> str:
    """One MLflow run per dataset version: the registry entry future training runs point to."""
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)
    df = duckdb.sql(f"select * from read_parquet({sql_str(CURATED / 'training_examples.parquet')})").df()
    dataset = mlflow.data.from_pandas(df, source=version["dvc_remote_url"], name="foliolake-rag-training-examples")

    with mlflow.start_run(run_name=version["tag"]) as run:
        mlflow.set_tags({
            "dataset.version": version["tag"],
            "git.commit": version["commit"],
            "dvc.md5": version["dvc_md5"],
            "dvc.remote_url": version["dvc_remote_url"],
        })
        mlflow.log_params(params)
        mlflow.log_metrics(counts)
        mlflow.log_input(dataset, context="training", tags={"version": version["tag"]})
        mlflow.log_artifact(str(DVC_FILE))
    print(f"Registered {version['tag']} as MLflow run {run.info.run_id}")
    return run.info.run_id


# ---------------------------------------------------------------------------- flow


@flow(name="training-data-pipeline", log_prints=True)
def training_data_pipeline(
    source_experiment: str = "foliolake-rag",
    registry_experiment: str = "foliolake-training-data",
    registry_tracking_uri: str | None = None,
    settle_minutes: int = 60,
    faithfulness_min: float = 0.8,
    answer_relevancy_min: float = 0.5,
    eval_pct: int = 20,
    push_git: bool = True,
) -> str | None:
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI))

    sync_data()
    traces, since, until = extract_traces(source_experiment, settle_minutes)
    if not traces:
        print("No new traces; nothing to version.")
        return None

    write_raw(traces)
    transform(faithfulness_min, answer_relevancy_min, eval_pct)
    counts = export_jsonl() | {"new_traces": len(traces)}

    version = version_data(push_git)
    if version is None:
        print("Dataset unchanged; no new version.")
        return None

    params = {
        "source_experiment": source_experiment,
        "extract_since_ms": since,
        "extract_until_ms": until,
        "settle_minutes": settle_minutes,
        "faithfulness_min": faithfulness_min,
        "answer_relevancy_min": answer_relevancy_min,
        "eval_pct": eval_pct,
    }
    register(version, counts, params, registry_experiment, registry_tracking_uri)
    return version["tag"]


if __name__ == "__main__":
    training_data_pipeline()
