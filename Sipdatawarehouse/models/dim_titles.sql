with stg_titles as (select * from {{ source("pubs", "Titles") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_titles.title_id"]) }} as titlekey,
    stg_titles.title_id as titleid,
    stg_titles.title as titlename,
    stg_titles.type as titletype,
    stg_titles.notes as titlenotes,
    stg_titles.advance as advance,
    stg_titles.royalty as royalty
from stg_titles
