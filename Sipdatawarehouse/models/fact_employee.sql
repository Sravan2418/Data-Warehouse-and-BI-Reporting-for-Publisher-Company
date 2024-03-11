with stg_employee as (
    select *, {{ dbt_utils.generate_surrogate_key(['pub_id']) }} as publisherkey  from {{source('pubs','Employee')}}
),
stg_jobs as (
    select job_id, job_desc from {{source('pubs','Jobs')}}
)
select 
    e.emp_id as Employeeid,
    e.publisherkey,
    e.fname as firstname,
    e.lname as lastname,
    j.job_desc as jobdesc,
    e.job_lvl as joblevel,
    1 as empflg,
    e.hire_date as hiredate
from stg_employee e
left join stg_jobs j on e.job_id = j.job_id
