with
    stg_title_author as (
        select
            {{ dbt_utils.generate_surrogate_key(["au_id"]) }} as authorkey,
            {{ dbt_utils.generate_surrogate_key(["title_id"]) }} as titlekey,
            *
        from {{ source("pubs", "Titleauthor") }}
    ),
    stg_title as (
        select
            {{ dbt_utils.generate_surrogate_key(["title_id"]) }} as titlekey,
            {{ dbt_utils.generate_surrogate_key(["pub_id"]) }} as publisherskey,
            replace(to_date(pubdate)::varchar, '-', '')::int as pubdatekey,
            *
        from {{ source("pubs", "Titles") }}
    )
select
    a.authorkey,
    a.titlekey,
    t.publisherskey,
    a.au_ord,
    a.title_id,
    a.royaltyper,
    t.price,
    t.ytd_sales,
    (t.price * t.ytd_sales) as totalsalesrevenue,
    ((a.royaltyper / 100) * t.price * t.ytd_sales) as effectiveroyaltyearned,
    ((t.royalty * t.ytd_sales / 100) - t.advance) as netearnings,
    t.pubdatekey
from stg_title_author a
join stg_title t on a.titlekey = t.titlekey
