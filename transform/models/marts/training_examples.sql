-- The training set: good chat answers only, one per question+context, split into train/eval.
-- Written to Parquet (data/curated/training_examples.parquet) so it is versioned with DVC.
{{ config(
    materialized = 'external',
    location = var('curated_dir') ~ '/training_examples.parquet'
) }}

with chats as (
    -- Chat turns that finished and produced an answer (skips uploads and failed runs).
    select *
    from {{ ref('stg_traces') }}
    where trace_name = 'chat'
      and state = 'OK'
      and messages_json is not null
      and nullif(trim(answer), '') is not null
),

good as (
    -- Quality gate: guardrail passed and the RAGAS judge scores clear the thresholds.
    -- Traces the judge hasn't scored yet (null scores) are left out.
    select *
    from chats
    where guardrail = 'pass'
      and faithfulness >= {{ var('faithfulness_min') }}
      and answer_relevancy >= {{ var('answer_relevancy_min') }}
),

deduped as (
    -- Same question over the same retrieved context: keep the most recent answer.
    select *
    from good
    qualify row_number() over (
        partition by md5(lower(trim(question)) || coalesce(context, ''))
        order by request_time_ms desc
    ) = 1
)

select
    trace_id,
    requested_at,
    session_id,
    question,
    context,
    answer,
    model,
    faithfulness,
    answer_relevancy,
    context_utilization,
    hhem_score,
    messages_json,
    tools_json,
    -- md5 of the trace id gives a stable split: a trace never moves between train and eval.
    case when md5_number(trace_id) % 100 < {{ var('eval_pct') }} then 'eval' else 'train' end as split
from deduped
order by trace_id
