-- SQL запрос для поиска арбитражных возможностей: фандинг + межбиржевой арбитраж
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
        -- Вычисляем потенциальную прибыль от фандинг арбитража
        CASE 
            WHEN f.fundingRate > 0 AND (f.markPrice - t.last) / t.last < -0.001 
            THEN ABS(f.fundingRate * 100) + ABS((f.markPrice - t.last) / t.last * 100)
            WHEN f.fundingRate < 0 AND (f.markPrice - t.last) / t.last > 0.001 
            THEN ABS(f.fundingRate * 100) + ABS((f.markPrice - t.last) / t.last * 100)
            ELSE 0
        END as funding_arbitrage_profit,
        -- Определяем тип фандинг арбитража
        CASE 
            WHEN f.fundingRate > 0 AND (f.markPrice - t.last) / t.last < -0.001 
            THEN 'LONG_FUTURES_SHORT_SPOT'
            WHEN f.fundingRate < 0 AND (f.markPrice - t.last) / t.last > 0.001 
            THEN 'SHORT_FUTURES_LONG_SPOT'
            ELSE 'NO_FUNDING_ARBITRAGE'
        END as funding_arbitrage_type
    FROM mart.cryptoscan_snap_futures_futurespricecollector_flat_v2_2_mv f
    INNER JOIN mart.cryptoscan_snap_tickers_futurespricecollector_flat_v2_2_mv t
        ON f.symbol = t.symbol 
        AND f.exchange = t.exchange
        AND ABS(toUnixTimestamp(f.ts) - toUnixTimestamp(t.ts)) <= 30
    WHERE f.ts >= now() - INTERVAL 10 MINUTE
        AND t.ts >= now() - INTERVAL 10 MINUTE
        AND f.fundingRate IS NOT NULL
        AND f.markPrice > 0
        AND t.last > 0
        AND t.bid > 0
        AND t.ask > 0
),
-- Межбиржевой арбитраж для спотовых цен
cross_exchange_spot AS (
    SELECT 
        t1.symbol,
        t1.exchange as exchange_1,
        t2.exchange as exchange_2,
        t1.ts as ts_1,
        t2.ts as ts_2,
        t1.last as price_1,
        t2.last as price_2,
        t1.bid as bid_1,
        t1.ask as ask_1,
        t2.bid as bid_2,
        t2.ask as ask_2,
        -- Потенциальная прибыль от межбиржевого арбитража (покупаем дешевле, продаем дороже)
        GREATEST(
            (t1.bid - t2.ask) / t2.ask * 100,  -- Покупаем на бирже 2, продаем на бирже 1
            (t2.bid - t1.ask) / t1.ask * 100   -- Покупаем на бирже 1, продаем на бирже 2
        ) as cross_exchange_profit,
        -- Определяем направление арбитража
        CASE 
            WHEN (t1.bid - t2.ask) / t2.ask > (t2.bid - t1.ask) / t1.ask 
            THEN CONCAT('BUY_', t2.exchange, '_SELL_', t1.exchange)
            ELSE CONCAT('BUY_', t1.exchange, '_SELL_', t2.exchange)
        END as cross_exchange_direction,
        -- Оценка ликвидности для арбитража
        LEAST(
            (t1.ask - t1.bid) / t1.bid * 100,
            (t2.ask - t2.bid) / t2.bid * 100
        ) as min_spread_pct
    FROM mart.cryptoscan_snap_tickers_futurespricecollector_flat_v2_2_mv t1
    INNER JOIN mart.cryptoscan_snap_tickers_futurespricecollector_flat_v2_2_mv t2
        ON t1.symbol = t2.symbol
        AND t1.exchange != t2.exchange
        AND ABS(toUnixTimestamp(t1.ts) - toUnixTimestamp(t2.ts)) <= 30
    WHERE t1.ts >= now() - INTERVAL 10 MINUTE
        AND t2.ts >= now() - INTERVAL 10 MINUTE
        AND t1.last > 0 AND t2.last > 0
        AND t1.bid > 0 AND t1.ask > 0
        AND t2.bid > 0 AND t2.ask > 0
        -- Фильтры для исключения аномалий
        AND t1.last BETWEEN 0.000001 AND 1000000  -- Разумный диапазон цен
        AND t2.last BETWEEN 0.000001 AND 1000000
        AND ABS(t1.last - t2.last) / GREATEST(t1.last, t2.last) <= 0.25  -- Максимум 25% разницы в ценах
        AND (t1.ask - t1.bid) / t1.bid <= 0.1  -- Спред не более 10%
        AND (t2.ask - t2.bid) / t2.bid <= 0.1
        -- Фильтруем только прибыльные возможности
        AND GREATEST(
            (t1.bid - t2.ask) / t2.ask * 100,
            (t2.bid - t1.ask) / t1.ask * 100
        ) > 0.1  -- Минимум 0.1% прибыли
        AND GREATEST(
            (t1.bid - t2.ask) / t2.ask * 100,
            (t2.bid - t1.ask) / t1.ask * 100
        ) <= 25  -- Максимум 25% прибыли (исключаем аномалии)
),
-- Межбиржевой арбитраж для фьючерсов
cross_exchange_futures AS (
    SELECT 
        f1.symbol,
        f1.exchange as exchange_1,
        f2.exchange as exchange_2,
        f1.ts as ts_1,
        f2.ts as ts_2,
        f1.markPrice as futures_price_1,
        f2.markPrice as futures_price_2,
        f1.fundingRate as funding_1,
        f2.fundingRate as funding_2,
        -- Прибыль от ценовой разницы
        ABS(f1.markPrice - f2.markPrice) / LEAST(f1.markPrice, f2.markPrice) * 100 as price_diff_pct,
        -- Дополнительная прибыль от разницы в фандинге
        ABS(f1.fundingRate - f2.fundingRate) * 100 as funding_diff_pct,
        -- Общая потенциальная прибыль
        ABS(f1.markPrice - f2.markPrice) / LEAST(f1.markPrice, f2.markPrice) * 100 + 
        ABS(f1.fundingRate - f2.fundingRate) * 100 as total_futures_arbitrage_profit,
        -- Направление арбитража
        CASE 
            WHEN f1.markPrice > f2.markPrice 
            THEN CONCAT('SHORT_', f1.exchange, '_LONG_', f2.exchange)
            ELSE CONCAT('SHORT_', f2.exchange, '_LONG_', f1.exchange)
        END as futures_arbitrage_direction
    FROM mart.cryptoscan_snap_futures_futurespricecollector_flat_v2_2_mv f1
    INNER JOIN mart.cryptoscan_snap_futures_futurespricecollector_flat_v2_2_mv f2
        ON f1.symbol = f2.symbol
        AND f1.exchange != f2.exchange
        AND ABS(toUnixTimestamp(f1.ts) - toUnixTimestamp(f2.ts)) <= 30
    WHERE f1.ts >= now() - INTERVAL 10 MINUTE
        AND f2.ts >= now() - INTERVAL 10 MINUTE
        AND f1.markPrice > 0 AND f2.markPrice > 0
        AND f1.fundingRate IS NOT NULL AND f2.fundingRate IS NOT NULL
        -- Фильтры для исключения аномалий в фьючерсах
        AND f1.markPrice BETWEEN 0.000001 AND 1000000
        AND f2.markPrice BETWEEN 0.000001 AND 1000000
        AND ABS(f1.markPrice - f2.markPrice) / GREATEST(f1.markPrice, f2.markPrice) <= 0.3  -- Максимум 30% разницы
        AND ABS(f1.fundingRate) <= 0.01  -- Фандинг не более 1%
        AND ABS(f2.fundingRate) <= 0.01
        -- Фильтруем только значимые возможности
        AND (ABS(f1.markPrice - f2.markPrice) / LEAST(f1.markPrice, f2.markPrice) * 100 + 
             ABS(f1.fundingRate - f2.fundingRate) * 100) > 0.1
        AND (ABS(f1.markPrice - f2.markPrice) / LEAST(f1.markPrice, f2.markPrice) * 100 + 
             ABS(f1.fundingRate - f2.fundingRate) * 100) <= 50  -- Максимум 50% общей прибыли
),
-- Объединяем все типы арбитража
combined_opportunities AS (
    -- Фандинг арбитраж
    SELECT 
        symbol,
        exchange as primary_exchange,
        '' as secondary_exchange,
        'FUNDING_ARBITRAGE' as arbitrage_type,
        funding_arbitrage_profit as profit_potential,
        funding_arbitrage_type as strategy,
        ts,
        futures_mark_price as price_1,
        spot_price as price_2,
        fundingRate as additional_info
    FROM latest_data
    WHERE funding_arbitrage_profit > 0
        AND funding_arbitrage_type != 'NO_FUNDING_ARBITRAGE'
        AND (ask - bid) / bid <= 0.01
        -- Фильтр на высоколиквидные токены
        AND symbol IN (
            'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'XRP/USDT', 'ADA/USDT',
            'DOGE/USDT', 'SOL/USDT', 'TRX/USDT', 'DOT/USDT', 'MATIC/USDT',
            'LTC/USDT', 'SHIB/USDT', 'AVAX/USDT', 'UNI/USDT', 'ATOM/USDT',
            'LINK/USDT', 'ETC/USDT', 'XLM/USDT', 'BCH/USDT', 'FIL/USDT',
            'NEAR/USDT', 'ALGO/USDT', 'VET/USDT', 'ICP/USDT', 'FTM/USDT',
            'HBAR/USDT', 'FLOW/USDT', 'EGLD/USDT', 'XTZ/USDT', 'THETA/USDT'
        )
    
    UNION ALL
    
    -- Межбиржевой спотовый арбитраж
    SELECT 
        symbol,
        exchange_1 as primary_exchange,
        exchange_2 as secondary_exchange,
        'CROSS_EXCHANGE_SPOT' as arbitrage_type,
        cross_exchange_profit as profit_potential,
        cross_exchange_direction as strategy,
        ts_1 as ts,
        price_1,
        price_2,
        min_spread_pct as additional_info
    FROM cross_exchange_spot
    WHERE cross_exchange_profit > 0.1
        AND cross_exchange_profit <= 25  -- Исключаем аномально высокие значения
        AND min_spread_pct <= 1.0  -- Максимальный спред 1%
        AND price_1 > 0.000001 AND price_2 > 0.000001  -- Минимальные цены
        AND price_1 <= 1000000 AND price_2 <= 1000000  -- Максимальные цены
        -- Фильтр на высоколиквидные токены
        AND symbol IN (
            'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'XRP/USDT', 'ADA/USDT',
            'DOGE/USDT', 'SOL/USDT', 'TRX/USDT', 'DOT/USDT', 'MATIC/USDT',
            'LTC/USDT', 'SHIB/USDT', 'AVAX/USDT', 'UNI/USDT', 'ATOM/USDT',
            'LINK/USDT', 'ETC/USDT', 'XLM/USDT', 'BCH/USDT', 'FIL/USDT',
            'NEAR/USDT', 'ALGO/USDT', 'VET/USDT', 'ICP/USDT', 'FTM/USDT',
            'HBAR/USDT', 'FLOW/USDT', 'EGLD/USDT', 'XTZ/USDT', 'THETA/USDT'
        )
    
    UNION ALL
    
    -- Межбиржевой фьючерсный арбитраж
    SELECT 
        symbol,
        exchange_1 as primary_exchange,
        exchange_2 as secondary_exchange,
        'CROSS_EXCHANGE_FUTURES' as arbitrage_type,
        total_futures_arbitrage_profit as profit_potential,
        futures_arbitrage_direction as strategy,
        ts_1 as ts,
        futures_price_1 as price_1,
        futures_price_2 as price_2,
        funding_diff_pct as additional_info
    FROM cross_exchange_futures
    WHERE total_futures_arbitrage_profit > 0.1
        AND total_futures_arbitrage_profit <= 50  -- Исключаем аномально высокие значения
        AND futures_price_1 > 0.000001 AND futures_price_2 > 0.000001
        AND futures_price_1 <= 1000000 AND futures_price_2 <= 1000000
        -- Фильтр на высоколиквидные токены
        AND symbol IN (
            'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'XRP/USDT', 'ADA/USDT',
            'DOGE/USDT', 'SOL/USDT', 'TRX/USDT', 'DOT/USDT', 'MATIC/USDT',
            'LTC/USDT', 'SHIB/USDT', 'AVAX/USDT', 'UNI/USDT', 'ATOM/USDT',
            'LINK/USDT', 'ETC/USDT', 'XLM/USDT', 'BCH/USDT', 'FIL/USDT',
            'NEAR/USDT', 'ALGO/USDT', 'VET/USDT', 'ICP/USDT', 'FTM/USDT',
            'HBAR/USDT', 'FLOW/USDT', 'EGLD/USDT', 'XTZ/USDT', 'THETA/USDT'
        )
)
-- Финальная выборка топ-15 возможностей
SELECT 
    symbol,
    primary_exchange,
    secondary_exchange,
    arbitrage_type,
    profit_potential,
    strategy,
    ts,
    price_1,
    price_2,
    additional_info,
    -- Категоризация по прибыльности
    CASE 
        WHEN profit_potential >= 1.0 THEN 'HIGH_PROFIT'
        WHEN profit_potential >= 0.5 THEN 'MEDIUM_PROFIT'
        ELSE 'LOW_PROFIT'
    END as profit_category,
    -- Оценка сложности исполнения
    CASE 
        WHEN arbitrage_type = 'FUNDING_ARBITRAGE' THEN 'MEDIUM'
        WHEN arbitrage_type = 'CROSS_EXCHANGE_SPOT' THEN 'LOW'
        WHEN arbitrage_type = 'CROSS_EXCHANGE_FUTURES' THEN 'HIGH'
    END as execution_complexity
FROM combined_opportunities
ORDER BY profit_potential DESC
LIMIT 15;
