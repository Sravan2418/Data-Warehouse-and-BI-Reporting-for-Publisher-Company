with
    stg_sales as (
        select
            ord_num as orderid,
            title_id,
            stor_id,
            {{ dbt_utils.generate_surrogate_key(["title_id"]) }} as titlekey,
            {{ dbt_utils.generate_surrogate_key(["stor_id"]) }} as storekey,
            qty as quantity,
            ord_date,
            payterms
        from {{ source("pubs", "Sales") }}
    ),
    stg_titles_more as (select title_id, price, pub_id,
    {{ dbt_utils.generate_surrogate_key(["pub_id"]) }} as publisherkey
    from 
    {{ source("pubs", "Titles") }}),
    stg_discounts as (select * from {{ source("pubs", "Discounts") }})
select
    s.orderid,
    s.titlekey,
    s.storekey,
    t.publisherkey,
    s.ord_date,
    s.payterms,
    s.quantity,
    case when d.discount is not null then d.discount else 0 end as discount,
    s.quantity * t.price as income,
    case
        when d.discount is not null
        then s.quantity * (t.price * (1 - (d.discount / 100)))
        else s.quantity * t.price
    end as discountedincome
from stg_sales s
left join stg_titles_more t on s.title_id = t.title_id
left join stg_discounts d on s.stor_id = d.stor_id
