-- mart.cryptoscan_snap_tickers_futurespricecollector_flat_v2_2_mv определение

CREATE TABLE mart.cryptoscan_snap_tickers_futurespricecollector_flat_v2_2_mv
(

    `id` UUID,

    `ts` DateTime,

    `datetime` DateTime64(9),

    `exchange` String,

    `average` Float64,

    `baseVolume` Float64,

    `change` Float64,

    `close` Float64,

    `high` Float64,

    `last` Float64,

    `low` Float64,

    `open` Float64,

    `percentage` Float64,

    `quoteVolume` Float64,

    `vwap` Float64,

    `symbol` String,

    `bid` Float64,

    `bidVolume` Float64,

    `ask` Float64,

    `askVolume` Float64,

    `previousClose` Float64,

    `markPrice` Float64,

    `indexPrice` Float64,

    `timestamp` DateTime
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 8192;