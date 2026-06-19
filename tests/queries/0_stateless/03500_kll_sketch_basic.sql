-- Test basic serializedKLL and percentileFromKLL functions

-- Test with simple numeric data
SELECT percentileFromKLL(serializedKLL(number), 0.5) AS median
FROM numbers(100);

SELECT percentileFromKLL(serializedKLL(number), 0.95) AS p95
FROM numbers(100);

SELECT percentileFromKLL(serializedKLL(number), 0.99) AS p99
FROM numbers(100);

-- Test with grouped data
SELECT 
    intDiv(number, 10) AS group_id,
    percentileFromKLL(serializedKLL(number), 0.5) AS median
FROM numbers(100)
GROUP BY group_id
ORDER BY group_id;

-- Test mergeSerializedKLL
WITH sketches AS (
    SELECT 
        intDiv(number, 10) AS group_id,
        serializedKLL(number) AS sketch
    FROM numbers(100)
    GROUP BY group_id
)
SELECT 
    percentileFromKLL(mergeSerializedKLL(sketch), 0.5) AS overall_median,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.95) AS overall_p95,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.99) AS overall_p99
FROM sketches;

-- Test with Float64 values
SELECT percentileFromKLL(serializedKLL(rand64() / 1000000000000.0), 0.5) AS median_float
FROM numbers(1000);

-- Test with negative values
SELECT percentileFromKLL(serializedKLL(number - 50), 0.5) AS median_negative
FROM numbers(100);

-- Test edge cases
SELECT percentileFromKLL(serializedKLL(number), 0.0) AS p0
FROM numbers(100);

SELECT percentileFromKLL(serializedKLL(number), 1.0) AS p100
FROM numbers(100);

-- Test with single value
SELECT percentileFromKLL(serializedKLL(42), 0.5) AS single_value;

-- Test with empty sketch (should return NaN)
SELECT percentileFromKLL('', 0.5) AS empty_sketch;
