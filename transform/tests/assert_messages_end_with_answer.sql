-- Every training conversation must end with the assistant's answer, or the example teaches nothing.
select trace_id
from {{ ref('training_examples') }}
where json_extract_string(messages_json, '$[#-1].role') is distinct from 'assistant'
   or json_extract_string(messages_json, '$[#-1].content') is distinct from answer
