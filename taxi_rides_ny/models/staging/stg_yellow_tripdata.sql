select
  -- identifiers
    cast(vendorid as int) as vendor_id,
    cast(ratecodeid as int) as rate_code_id,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as numeric(10,2)) as trip_distance,
    1 as trip_type, -- yellow taxi can only be street-hail, so we can hardcode this value
    -- payment info
    cast(fare_amount as numeric(10,2)) as fare_amount,
    cast(extra as numeric(10,2)) as extra,
    cast(mta_tax as numeric(10,2)) as mta_tax,
    cast(tip_amount as numeric(10,2)) as tip_amount,
    cast(tolls_amount as numeric(10,2)) as tolls_amount,
    0 as ehail_fee, -- yellow taxi does not have this fee, so we can hardcode this value
    cast(improvement_surcharge as numeric(10,2)) as improvement_surcharge,
    cast(total_amount as numeric(10,2)) as total_amount,
    cast(payment_type as int) as payment_type,
    -- NEW
    'Yellow' as service_type
from {{ source('raw_data', 'yellow_tripdata') }}
where vendorid is not null
