with sightings as (
    select * from {{ ref('fct_turtle_sightings') }}
),

aggregated as (
    select
        ocean_basin,
        scientific_name,
        year,
        month,
        count(*)                        as sighting_count,
        avg(sea_surface_temp)           as avg_sst,
        avg(sea_surface_salinity)       as avg_sss,
        avg(depth_m)                    as avg_depth
    from sightings
    where ocean_basin != 'Other'
      and year is not null
      and month is not null
    group by
        ocean_basin,
        scientific_name,
        year,
        month
)

select * from aggregated
order by ocean_basin, scientific_name, year, month