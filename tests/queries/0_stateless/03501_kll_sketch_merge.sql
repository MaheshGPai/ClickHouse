-- Test mergeSerializedKLL with distributed sketches

DROP TABLE IF EXISTS kll_test_sketches;

CREATE TABLE kll_test_sketches (
    service String,
    hour DateTime,
    sketch String
) ENGINE = Memory;

-- Insert hourly sketches for multiple services
INSERT INTO kll_test_sketches
SELECT 
    'service_' || toString(intDiv(number, 100)) AS service,
    toDateTime('2024-01-01 00:00:00') + toIntervalHour(intDiv(number % 100, 10)) AS hour,
    serializedKLL(rand64() % 1000) AS sketch
FROM numbers(1000);

-- Merge sketches by service
SELECT 
    service,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.5) AS p50,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.95) AS p95
FROM kll_test_sketches
GROUP BY service
ORDER BY service;

-- Merge all sketches globally
SELECT 
    percentileFromKLL(mergeSerializedKLL(sketch), 0.5) AS global_p50,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.95) AS global_p95
FROM kll_test_sketches;

-- Merge sketches by hour
SELECT 
    hour,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.5) AS p50
FROM kll_test_sketches
GROUP BY hour
ORDER BY hour;

-- Test merging with base64 encoding parameter (should default to raw binary)
SELECT 
    percentileFromKLL(mergeSerializedKLL(sketch), 0.5) AS p50_default
FROM kll_test_sketches;

DROP TABLE kll_test_sketches;
