select
    -- identifiers (FHV nie ma vendorid ani ratecodeid, więc wstawiamy NULL / 0)
    cast(null as int) as vendor_id,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp)  as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info (FHV nie ma tych samych pól co yellow/green)
    cast(null as varchar) as store_and_fwd_flag,
    cast(0 as int)        as passenger_count,      -- brak w FHV, więc 0
    cast(0 as int)        as trip_type,           -- brak w FHV, więc 0

    -- payment info – FHV nie ma tych kolumn, więc wszystko na 0
    cast(0 as numeric(10,2)) as fare_amount,
    cast(0 as numeric(10,2)) as extra,
    cast(0 as numeric(10,2)) as mta_tax,
    cast(0 as numeric(10,2)) as tip_amount,
    cast(0 as numeric(10,2)) as tolls_amount,
    0.000000000000001        as ehail_fee,        -- jak chciałaś: bardzo mała wartość
    cast(0 as numeric(10,2)) as improvement_surcharge,
    cast(0 as numeric(10,2)) as total_amount,
    cast(0 as int)           as payment_type,

    -- źródło usługi
    'FHV' as service_type
from {{ source('raw_data', 'fhv_tripdata') }}
where dispatching_base_num is not null

    