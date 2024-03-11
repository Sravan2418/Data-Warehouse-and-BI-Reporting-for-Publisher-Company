with stg_publishers as (select * from {{ source("pubs", "Publishers") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_publishers.pub_id"]) }} as publisherskey,
    stg_publishers.*
from stg_publishers
