-- Judge scores are fractions; anything outside [0, 1] means extraction picked up the wrong value.
select trace_id
from {{ ref('training_examples') }}
where faithfulness not between 0 and 1
   or answer_relevancy not between 0 and 1
   or (context_utilization is not null and context_utilization not between 0 and 1)
