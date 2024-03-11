with stg_authors as (select * from {{ source("pubs", "Authors") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_authors.au_id"]) }} as authorkey,
    stg_authors.au_id as authorid,
    stg_authors.au_lname as authorlastname,
    stg_authors.au_fname as authorfirstname,
    stg_authors.phone as authorphone,
    stg_authors.address as authoraddress,
    stg_authors.city as authorcity,
    stg_authors.state as authorstate,
    stg_authors.zip as authorzip,
    stg_authors.contract as authorcontract
from stg_authors
