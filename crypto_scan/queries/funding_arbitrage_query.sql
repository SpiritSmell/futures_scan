-- SQL запрос для поиска 10 самых выгодных арбитражных возможностей по фандингу за последние 10 минут
WITH latest_data AS (
    SELECT 
        f.symbol,
        f.exchange,
        f.ts,
        f.fundingRate,
        f.markPrice as futures_mark_price,
        f.indexPrice as futures_index_price,
        t.markPrice as ticker_mark_price,
        t.indexPrice as ticker_index_price,
        t.last as spot_price,
        t.bid,
        t.ask,
        -- Вычисляем спред между фьючерсом и спотом
        (f.markPrice - t.last) / t.last * 100 as price_spread_pct,
        -- Вычисляем потенциальную прибыль от арбитража
        -- Если фандинг положительный и спред отрицательный - можно заработать на конвергенции
        CASE 
            WHEN f.fundingRate > 0 AND (f.markPrice - t.last) / t.last < -0.001 
            THEN ABS(f.fundingRate * 100) + ABS((f.markPrice - t.last) / t.last * 100)
            WHEN f.fundingRate < 0 AND (f.markPrice - t.last) / t.last > 0.001 
            THEN ABS(f.fundingRate * 100) + ABS((f.markPrice - t.last) / t.last * 100)
            ELSE 0
        END as arbitrage_profit_potential,
        -- Определяем тип арбитража
        CASE 
            WHEN f.fundingRate > 0 AND (f.markPrice - t.last) / t.last < -0.001 
            THEN 'LONG_FUTURES_SHORT_SPOT'
            WHEN f.fundingRate < 0 AND (f.markPrice - t.last) / t.last > 0.001 
            THEN 'SHORT_FUTURES_LONG_SPOT'
            ELSE 'NO_ARBITRAGE'
        END as arbitrage_type
    FROM mart.cryptoscan_snap_futures_futurespricecollector_flat_v2_2_mv f
    INNER JOIN mart.cryptoscan_snap_tickers_futurespricecollector_flat_v2_2_mv t
        ON f.symbol = t.symbol 
        AND f.exchange = t.exchange
        AND ABS(toUnixTimestamp(f.ts) - toUnixTimestamp(t.ts)) <= 30  -- Данные должны быть синхронизированы в пределах 30 секунд
    WHERE f.ts >= now() - INTERVAL 10 MINUTE
        AND t.ts >= now() - INTERVAL 10 MINUTE
        AND f.fundingRate IS NOT NULL
        AND f.markPrice > 0
        AND t.last > 0
        AND t.bid > 0
        AND t.ask > 0
)
SELECT 
    symbol,
    exchange,
    ts,
    fundingRate,
    futures_mark_price,
    spot_price,
    price_spread_pct,
    arbitrage_profit_potential,
    arbitrage_type,
    -- Дополнительные метрики для анализа
    bid,
    ask,
    (ask - bid) / bid * 100 as bid_ask_spread_pct,
    -- Оценка ликвидности (чем меньше спред, тем лучше ликвидность)
    CASE 
        WHEN (ask - bid) / bid <= 0.001 THEN 'HIGH_LIQUIDITY'
        WHEN (ask - bid) / bid <= 0.005 THEN 'MEDIUM_LIQUIDITY'
        ELSE 'LOW_LIQUIDITY'
    END as liquidity_assessment
FROM latest_data
WHERE arbitrage_profit_potential > 0
    AND arbitrage_type != 'NO_ARBITRAGE'
    -- Фильтруем только инструменты с разумной ликвидностью
    AND (ask - bid) / bid <= 0.01  -- Спред не более 1%
ORDER BY arbitrage_profit_potential DESC
LIMIT 10;
