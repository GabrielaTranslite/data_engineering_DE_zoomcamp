select 
    -- identifiers
    cast(vendorid as int) as vendor_id,
    cast(ratecodeid as int) as rate_code_id,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as float) as trip_distance,
    cast(trip_type as int) as trip_type,
    -- payment info
    cast(fare_amount as numeric(10,2)) as fare_amount,
    cast(extra as numeric(10,2)) as extra,
    cast(mta_tax as numeric(10,2)) as mta_tax,
    cast(tip_amount as numeric(10,2)) as tip_amount,
    cast(tolls_amount as numeric(10,2)) as tolls_amount,
    cast(improvement_surcharge as numeric(10,2)) as improvement_surcharge,
    cast(total_amount as numeric(10,2)) as total_amount,
    cast(payment_type as int) as payment_type
from {{ source('raw_data', 'green_tripdata') }}
where vendorid is not null