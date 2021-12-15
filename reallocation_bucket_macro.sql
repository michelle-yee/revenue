--this dbt macro recreates our reallocation buckets on a daily grain

{% set buckets = ["lead", "trial", "client"] %}

select
    DATE_TRUNC('d', conversion_date) as conversion_date,
    {% for conversion_type in buckets %}
    sum(case when conversion_type = '{{buckets}}' then channel_credit ELSE 0 end) as {{conversion_type}}_reallocation_bucket,
    {% endfor %}
from {{ ref('reallocation_bucket') }}
group by 1
