-- mart.cryptoscan_snap_futures_futurespricecollector_flat_v2_2_mv определение

CREATE TABLE mart.cryptoscan_snap_futures_futurespricecollector_flat_v2_2_mv
(

    `id` UUID,

    `ts` DateTime,

    `datetime` DateTime64(9),

    `exchange` String,

    `fundingDatetime` DateTime64(9),

    `fundingRate` Float64,

    `fundingTimestamp` DateTime,

    `indexPrice` Float64,

    `interval` UInt8,

    `markPrice` Float64,

    `symbol` String,

    `timestamp` DateTime
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 8192;