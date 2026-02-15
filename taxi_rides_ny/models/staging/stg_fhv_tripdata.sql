select
  -- identifiers
    cast(vendorid as int) as vendor_id,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    -- payment info
    cast(fare_amount as numeric(10,2)) as fare_amount,
    cast(extra as numeric(10,2)) as extra,
    cast(mta_tax as numeric(10,2)) as mta_tax,
    cast(tip_amount as numeric(10,2)) as tip_amount,
    cast(tolls_amount as numeric(10,2)) as tolls_amount,
    0.000000000000001 AS ehail_fee, -- fhv does not have this fee, so we can hardcode this value
    cast(improvement_surcharge as numeric(10,2)) as improvement_surcharge,
    cast(total_amount as numeric(10,2)) as total_amount,
    cast(payment_type as int) as payment_type,
    -- NEW
    'FHV' AS service_type
    where dispatching_base_num IS NULL
    from {{ source('raw_data', 'fhv_tripdata') }}
    