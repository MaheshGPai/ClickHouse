---
description: 'Creates a serialized KLL quantiles sketch for percentile estimation'
slug: /sql-reference/aggregate-functions/reference/serializedkll
title: 'serializedKLL'
doc_type: 'reference'
---

# serializedKLL

Creates a serialized binary representation of a KLL (Kolmogorov-Lerch-Lifschitz) quantiles sketch for approximate percentile and quantile calculations. KLL is a modern probabilistic data structure that provides better space efficiency (35-60% smaller) than classic quantiles sketches while maintaining formal accuracy guarantees.

The resulting sketch can be stored, transmitted over network, merged with other sketches using [mergeSerializedKLL](/docs/en/sql-reference/aggregate-functions/reference/mergeserializedkll), or queried for percentiles using [percentileFromKLL](/docs/en/sql-reference/functions/percentilefromkll).

## Syntax

```sql
serializedKLL(column)
```

## Arguments

- `column` — Numeric column (Int8/16/32/64, UInt8/16/32/64, Float32/64) to create the sketch from.

## Returned Value

- Serialized binary KLL sketch. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Implementation Details

- Uses Apache DataSketches KLL algorithm with default K=200
- Provides ~1.65% rank error at 99% confidence
- 35-60% smaller than classic quantiles sketches for same accuracy
- Better accuracy for central quantiles (p50-p95)
- Proven optimal compactness guarantees
- Compatible with Apache DataSketches format (Java, Python, Go implementations)
- Sketch size is independent of data size (~2-3KB for K=200)

## Usage

### Create Sketch from Raw Values

```sql
SELECT serializedKLL(response_time_ms) AS latency_sketch
FROM requests
WHERE service = 'api' AND date = today();
```

### Store Sketches in Table

```sql
CREATE TABLE hourly_latency_sketches (
    service String,
    hour DateTime,
    sketch String
) ENGINE = MergeTree() 
ORDER BY (service, hour);

INSERT INTO hourly_latency_sketches
SELECT 
    service,
    toStartOfHour(timestamp) AS hour,
    serializedKLL(latency_ms) AS sketch
FROM requests
GROUP BY service, hour;
```

## Examples

### Example 1: Basic Sketch Creation

```sql
-- Create sketch and immediately extract percentiles
SELECT 
    percentileFromKLL(serializedKLL(response_time_ms), 0.50) AS p50,
    percentileFromKLL(serializedKLL(response_time_ms), 0.95) AS p95,
    percentileFromKLL(serializedKLL(response_time_ms), 0.99) AS p99
FROM requests
WHERE date = today();
```

### Example 2: Time-Series Sketch Storage

```sql
-- Create hourly sketches for efficient percentile queries
SELECT 
    toStartOfHour(timestamp) AS hour,
    serializedKLL(query_duration_ms) AS sketch
FROM query_log
WHERE timestamp >= now() - INTERVAL 7 DAY
GROUP BY hour;
```

### Example 3: Per-Service Sketches

```sql
-- Track latency distribution per service
SELECT 
    service,
    region,
    serializedKLL(latency_ms) AS sketch
FROM requests
WHERE date = today()
GROUP BY service, region;
```

### Example 4: Kafka/Streaming Integration

```sql
-- Materialize sketches for Kafka export
CREATE MATERIALIZED VIEW kafka_latency_sketches
ENGINE = Kafka(...)
AS SELECT 
    service,
    toStartOfMinute(timestamp) AS minute,
    serializedKLL(latency_ms) AS kll_sketch_bytes
FROM requests
GROUP BY service, minute;
```

### Example 5: Comparing Distributions

```sql
-- Create sketches for before/after comparison
WITH 
    before AS (
        SELECT serializedKLL(latency_ms) AS sketch
        FROM requests WHERE date = '2024-01-01'
    ),
    after AS (
        SELECT serializedKLL(latency_ms) AS sketch
        FROM requests WHERE date = '2024-01-02'
    )
SELECT 
    percentileFromKLL((SELECT sketch FROM before), 0.95) AS before_p95,
    percentileFromKLL((SELECT sketch FROM after), 0.95) AS after_p95;
```

## Performance Notes

- Memory: ~2-3KB per sketch (independent of data size)
- Speed: Fast insertion and sketching, suitable for real-time analytics
- Compression: Sketch size does not grow with data volume
- Storage: 35-60% smaller than classic quantiles for same accuracy
- Network: Compact format for transmission between services

## Advantages Over Classic Quantiles

1. **Space Efficiency**: 35-60% smaller for same accuracy
2. **Better Central Percentiles**: More accurate for p50-p95
3. **Formal Guarantees**: Proven optimal compactness
4. **Modern Algorithm**: Actively maintained and improved
5. **Cross-Platform**: Apache DataSketches standard format

## Use Cases

- Computing percentiles (p50, p95, p99) for latency monitoring
- Tracking distribution of values over time
- Distributed quantile estimation across multiple nodes
- Cross-service sketch interoperability (Go → ClickHouse → Java)
- Pre-computing sketches for fast percentile queries
- Reducing data retention costs (store sketches instead of raw values)

## See Also

- [mergeSerializedKLL](/docs/en/sql-reference/aggregate-functions/reference/mergeserializedkll) — Merge multiple KLL sketches
- [percentileFromKLL](/docs/en/sql-reference/functions/percentilefromkll) — Extract percentile from sketch
- [quantile](/docs/en/sql-reference/aggregate-functions/reference/quantile) — Direct percentile calculation (non-sketched)
