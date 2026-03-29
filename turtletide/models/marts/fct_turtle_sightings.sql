with stg as (
    select * from {{ ref('stg_obis_occurrences') }}
),

with_basin as (
    select
        *,
        case
            when longitude between -180 and -100 then 'North Pacific'
            when longitude between -100 and  -60
             and latitude  between   0  and   90 then 'North Atlantic'
            when longitude between -100 and  -60
             and latitude  between  -90 and    0 then 'South Atlantic'
            when longitude between  -60 and   20 then 'South Atlantic'
            when longitude between   20 and  147 then 'Indian Ocean'
            when longitude between  147 and  180 then 'South Pacific'
            else 'Other'
        end as ocean_basin
    from stg
    where latitude  between -90 and 90
      and longitude between -180 and 180
      and year is not null
)

select * from with_basin