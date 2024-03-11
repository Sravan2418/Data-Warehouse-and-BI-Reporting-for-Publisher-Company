with stg_stores as (select * from {{ source("pubs", "Stores") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_stores.stor_id"]) }} as storekey,
    stg_stores.stor_id as storeid,
    stg_stores.stor_name as storename,
    stg_stores.stor_address as storeaddress,
    stg_stores.city as storecity,
    stg_stores.state as storestate,
    stg_stores.zip as storezip
from stg_stores
