with source as (
    select * from {{ source('turtletide_silver', 'occurrences') }}
),

enriched as (
    select
        occurrence_id,
        scientific_name,
        species,
        latitude,
        longitude,
        safe.parse_date('%Y-%m-%d', event_date)    as event_date,
        extract(year from safe.parse_date('%Y-%m-%d', event_date))  as year,
        extract(month from safe.parse_date('%Y-%m-%d', event_date)) as month,
        depth                                       as depth_m,
        life_stage,
        basis_of_record,
        sea_surface_temp,
        sea_surface_salinity,
        dataset_id
    from source
    where latitude is not null
      and longitude is not null
)

select * from enriched