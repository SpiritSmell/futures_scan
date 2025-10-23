-- SQL запрос для межбиржевого арбитража фандингов и фьючерсов за последние 10 минут
WITH futures_data AS (
    SELECT 
        f.symbol,
        f.exchange,
        f.ts,
        f.fundingRate,
        f.markPrice as futures_mark_price,
        f.indexPrice as futures_index_price
    FROM mart.cryptoscan_snap_futures_futurespricecollector_flat_v2_2_mv f
    WHERE f.ts >= now() - INTERVAL 10 MINUTE
        AND f.fundingRate IS NOT NULL
        AND f.markPrice > 0
),
cross_exchange_funding_arbitrage AS (
    SELECT 
        f1.symbol,
        f1.exchange as exchange_1,
        f2.exchange as exchange_2,
        f1.ts as ts_1,
        f2.ts as ts_2,
        f1.fundingRate as funding_rate_1,
        f2.fundingRate as funding_rate_2,
        f1.futures_mark_price as futures_price_1,
        f2.futures_mark_price as futures_price_2,
        f1.futures_index_price as index_price_1,
        f2.futures_index_price as index_price_2,
        
        -- Вычисляем разницу в фандинг рейтах (в процентах)
        (f1.fundingRate - f2.fundingRate) * 100 as funding_rate_diff_pct,
        
        -- Вычисляем разницу в ценах фьючерсов (в процентах)
        (f1.futures_mark_price - f2.futures_mark_price) / f2.futures_mark_price * 100 as price_diff_pct,
        
        -- Потенциальная прибыль от фандинг арбитража
        -- Получаем фандинг на бирже с более высоким рейтом, платим на бирже с низким рейтом
        ABS(f1.fundingRate - f2.fundingRate) * 100 as funding_arbitrage_profit,
        
        -- Потенциальная прибыль от ценового арбитража фьючерсов
        ABS(f1.futures_mark_price - f2.futures_mark_price) / LEAST(f1.futures_mark_price, f2.futures_mark_price) * 100 as price_arbitrage_profit,
        
        -- Общая потенциальная прибыль
        ABS(f1.fundingRate - f2.fundingRate) * 100 + 
        ABS(f1.futures_mark_price - f2.futures_mark_price) / LEAST(f1.futures_mark_price, f2.futures_mark_price) * 100 as total_arbitrage_profit,
        
        -- Определяем стратегию фандинг арбитража
        CASE 
            WHEN f1.fundingRate > f2.fundingRate 
            THEN CONCAT('SHORT_', f1.exchange, '_LONG_', f2.exchange, '_COLLECT_FUNDING')
            ELSE CONCAT('SHORT_', f2.exchange, '_LONG_', f1.exchange, '_COLLECT_FUNDING')
        END as funding_strategy,
        
        -- Определяем стратегию ценового арбитража
        CASE 
            WHEN f1.futures_mark_price > f2.futures_mark_price 
            THEN CONCAT('SHORT_', f1.exchange, '_LONG_', f2.exchange, '_PRICE_CONV')
            ELSE CONCAT('SHORT_', f2.exchange, '_LONG_', f1.exchange, '_PRICE_CONV')
        END as price_strategy,
        
        -- Комбинированная стратегия
        CASE 
            WHEN f1.fundingRate > f2.fundingRate AND f1.futures_mark_price > f2.futures_mark_price
            THEN CONCAT('DOUBLE_SHORT_', f1.exchange, '_LONG_', f2.exchange)
            WHEN f1.fundingRate > f2.fundingRate AND f1.futures_mark_price < f2.futures_mark_price
            THEN CONCAT('MIXED_FUNDING_', f1.exchange, '_PRICE_', f2.exchange)
            WHEN f1.fundingRate < f2.fundingRate AND f1.futures_mark_price > f2.futures_mark_price
            THEN CONCAT('MIXED_FUNDING_', f2.exchange, '_PRICE_', f1.exchange)
            ELSE CONCAT('DOUBLE_SHORT_', f2.exchange, '_LONG_', f1.exchange)
        END as combined_strategy,
        
        -- Оценка качества арбитража
        CASE 
            WHEN ABS(f1.fundingRate - f2.fundingRate) * 100 > 0.1 AND 
                 ABS(f1.futures_mark_price - f2.futures_mark_price) / LEAST(f1.futures_mark_price, f2.futures_mark_price) * 100 > 0.05
            THEN 'HIGH_QUALITY'
            WHEN ABS(f1.fundingRate - f2.fundingRate) * 100 > 0.05 OR 
                 ABS(f1.futures_mark_price - f2.futures_mark_price) / LEAST(f1.futures_mark_price, f2.futures_mark_price) * 100 > 0.02
            THEN 'MEDIUM_QUALITY'
            ELSE 'LOW_QUALITY'
        END as arbitrage_quality
        
    FROM futures_data f1
    INNER JOIN futures_data f2
        ON f1.symbol = f2.symbol
        AND f1.exchange != f2.exchange
        AND ABS(toUnixTimestamp(f1.ts) - toUnixTimestamp(f2.ts)) <= 30  -- Синхронизация данных
    WHERE 
        -- Фильтры для исключения аномалий
        f1.futures_mark_price BETWEEN 0.000001 AND 1000000
        AND f2.futures_mark_price BETWEEN 0.000001 AND 1000000
        AND ABS(f1.fundingRate) <= 0.01  -- Фандинг не более 1%
        AND ABS(f2.fundingRate) <= 0.01
        AND ABS(f1.futures_mark_price - f2.futures_mark_price) / GREATEST(f1.futures_mark_price, f2.futures_mark_price) <= 0.05  -- Максимум 5% разницы в ценах
        
        -- Минимальные пороги для арбитража
        AND (ABS(f1.fundingRate - f2.fundingRate) * 100 > 0.01 OR  -- Минимум 0.01% разницы в фандинге
             ABS(f1.futures_mark_price - f2.futures_mark_price) / LEAST(f1.futures_mark_price, f2.futures_mark_price) * 100 > 0.01)  -- Или 0.01% в цене
)
SELECT 
    symbol,
    exchange_1,
    exchange_2,
    ts_1,
    funding_rate_1,
    funding_rate_2,
    funding_rate_diff_pct,
    futures_price_1,
    futures_price_2,
    price_diff_pct,
    funding_arbitrage_profit,
    price_arbitrage_profit,
    total_arbitrage_profit,
    funding_strategy,
    price_strategy,
    combined_strategy,
    arbitrage_quality,
    
    -- Дополнительные метрики для анализа
    CASE 
        WHEN total_arbitrage_profit >= 0.5 THEN 'HIGH_PROFIT'
        WHEN total_arbitrage_profit >= 0.1 THEN 'MEDIUM_PROFIT'
        ELSE 'LOW_PROFIT'
    END as profit_category,
    
    -- Оценка сложности исполнения
    CASE 
        WHEN funding_rate_diff_pct * price_diff_pct > 0 THEN 'SIMPLE_SAME_DIRECTION'
        WHEN funding_rate_diff_pct * price_diff_pct < 0 THEN 'COMPLEX_OPPOSITE_DIRECTION'
        ELSE 'NEUTRAL'
    END as execution_complexity,
    
    -- Рекомендация по исполнению
    CASE 
        WHEN total_arbitrage_profit > 0.2 AND arbitrage_quality = 'HIGH_QUALITY' 
        THEN 'RECOMMENDED'
        WHEN total_arbitrage_profit > 0.1 AND arbitrage_quality IN ('HIGH_QUALITY', 'MEDIUM_QUALITY')
        THEN 'CONSIDER'
        ELSE 'AVOID'
    END as recommendation

FROM cross_exchange_funding_arbitrage
WHERE total_arbitrage_profit > 0.01  -- Минимум 0.01% общей прибыли
ORDER BY total_arbitrage_profit DESC, arbitrage_quality DESC
LIMIT 15;
