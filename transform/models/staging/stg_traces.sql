-- One row per MLflow trace, exactly as extracted (data/raw/*.parquet), plus a real timestamp.
select
    trace_id,
    epoch_ms(request_time_ms) as requested_at,
    request_time_ms,
    state,
    trace_name,
    session_id,
    question,
    history_json,
    answer,
    reply,
    context,
    route,
    guardrail,
    hhem_score,
    faithfulness,
    answer_relevancy,
    context_utilization,
    model,
    messages_json,
    tools_json,
    extracted_at
from read_parquet('{{ var("raw_glob") }}', union_by_name = true)
