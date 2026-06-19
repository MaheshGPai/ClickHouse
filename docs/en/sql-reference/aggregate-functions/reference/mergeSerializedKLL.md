---
description: 'Merges multiple serialized KLL quantiles sketches into a unified sketch'
slug: /sql-reference/aggregate-functions/reference/mergeserializedkll
title: 'mergeSerializedKLL'
doc_type: 'reference'
---

# mergeSerializedKLL

Merges multiple serialized KLL (Kolmogorov-Lerch-Lifschitz) quantiles sketches into a single unified sketch. This enables distributed percentile/quantile estimation where sketches are computed on different nodes, time periods, or services and then merged together to get accurate percentiles across the entire dataset.

KLL sketches provide better space efficiency (35-60% smaller) than classic quantiles sketches while maintaining formal accuracy guarantees, making them ideal for cross-service data pipelines and distributed analytics.

## Syntax

```sql
mergeSerializedKLL([base64_encoded])(sketch_column)
```

## Parameters (optional)

- `base64_encoded` — UInt8 (0 or 1, default: 0)
  - 0 (false): Data is raw binary, skips base64 decoding (fastest, recommended for ClickHouse-generated data)
  - 1 (true): Data may be base64 encoded, checks and decodes if detected (for CSV, JSON, external data)

## Arguments

- `sketch_column` — Column containing serialized KLL sketches (from [serializedKLL](/docs/en/sql-reference/aggregate-functions/reference/serializedkll) or previous merges). Type: [String](/docs/en/sql-reference/data-types/string.md).

## Returned Value

- Merged serialized KLL sketch. Type: [String](/docs/en/sql-reference/data-types/string.md).
- Can be further merged or passed to [percentileFromKLL](/docs/en/sql-reference/functions/percentilefromkll).

## Implementation Details

- Uses Apache DataSketches KLL merging algorithm
- Efficient merging: O(k) where k is sketch size (typically ~800 items for K=200)
- Merging is associative and commutative
- Merged sketch maintains same accuracy guarantees as input sketches
- 35-60% smaller than classic quantiles sketches
- Compatible with sketches from Java, Python, Go implementations

## Usage

### Basic Sketch Merging

```sql
SELECT mergeSerializedKLL(sketch) AS merged_sketch
FROM hourly_latency_sketches
WHERE hour >= now() - INTERVAL 24 HOUR;
```

### Extract Percentiles from Merged Sketch

```sql
SELECT 
    percentileFromKLL(mergeSerializedKLL(sketch), 0.50) AS p50,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.95) AS p95,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.99) AS p99
FROM hourly_latency_sketches;
```

## Examples

### Example 1: Daily Rollup from Hourly Sketches

```sql
SELECT 
    date,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.5) AS p50_latency,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.95) AS p95_latency,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.99) AS p99_latency
FROM hourly_latency_sketches
GROUP BY toDate(hour) AS date;
```

### Example 2: Cross-Service Aggregation from Kafka

```sql
-- Merge KLL sketches from Kafka messages (raw binary from Go service)
SELECT 
    service,
    percentileFromKLL(mergeSerializedKLL(kll_sketch_bytes), 0.95) AS p95_latency
FROM kafka_edge_metrics
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY service;
```

### Example 3: Multi-Dimensional Aggregation

```sql
-- Merge across multiple dimensions
SELECT 
    service,
    toStartOfWeek(hour) AS week,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.95) AS weekly_p95
FROM hourly_latency_sketches
WHERE hour >= now() - INTERVAL 90 DAY
GROUP BY service, week
ORDER BY service, week;
```

### Example 4: Global Percentiles Across Regions

```sql
WITH regional_sketches AS (
    SELECT 
        region,
        serializedKLL(response_time_ms) AS sketch
    FROM requests
    WHERE date = today()
    GROUP BY region
)
SELECT 
    percentileFromKLL(mergeSerializedKLL(sketch), 0.50) AS global_median,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.95) AS global_p95,
    percentileFromKLL(mergeSerializedKLL(sketch), 0.99) AS global_p99
FROM regional_sketches;
```

### Example 5: Hierarchical Time Aggregation

```sql
-- Minute → Hour → Day rollups
CREATE TABLE minute_sketches (
    service String,
    minute DateTime,
    sketch String
) ENGINE = MergeTree() ORDER BY (service, minute);

CREATE TABLE hour_sketches (
    service String,
    hour DateTime,
    sketch String
) ENGINE = MergeTree() ORDER BY (service, hour);

-- Roll up minutes to hours
INSERT INTO hour_sketches
SELECT 
    service,
    toStartOfHour(minute) AS hour,
    mergeSerializedKLL(sketch) AS sketch
FROM minute_sketches
WHERE minute >= now() - INTERVAL 1 HOUR
GROUP BY service, hour;
```

### Example 6: Enable Base64 Decoding for External Data

```sql
-- For external data that may be base64 encoded
SELECT 
    mergeSerializedKLL(1)(sketch) AS merged
FROM imported_sketches;
```

## Performance Notes

- **Merging Speed**: O(k) where k is sketch size (~800 items for K=200)
- **Memory**: Only holds merged sketch in memory
- **Optimization**: Use `base64_encoded=0` (default) for best performance with binary data
- **Efficiency**: Significantly faster than re-computing percentiles from raw data
- **Network**: 35-60% smaller than classic quantiles for data transmission

## Comparison with Classic Quantiles

| Metric | KLL Sketch | Classic Quantiles |
|--------|------------|-------------------|
| Size | 2-3KB | 4-6KB |
| Accuracy (p50-p95) | Better | Good |
| Merge Speed | Fast (O(k)) | Fast (O(k)) |
| Format | Apache DataSketches | ClickHouse-specific |
| Cross-Platform | Yes | No |

## Use Cases

1. **Time-Series Rollup**: Aggregate minute → hour → day → month
2. **Distributed Analytics**: Combine sketches from multiple shards or services
3. **Cross-Service Pipelines**: Merge sketches from Kafka, upstream services, or different systems
4. **Cost Optimization**: Pre-compute sketches instead of storing raw data
5. **Global Metrics**: Aggregate regional or datacenter-level sketches
6. **Data Lake Integration**: Apache DataSketches format for Spark, Flink, etc.

## Error Handling

- Empty sketches are ignored during merge
- Corrupted sketches are skipped (graceful degradation)
- At least one valid sketch is required for non-empty result

## See Also

- [serializedKLL](/docs/en/sql-reference/aggregate-functions/reference/serializedkll) — Create KLL sketches
- [percentileFromKLL](/docs/en/sql-reference/functions/percentilefromkll) — Extract percentile from merged sketch
- [quantileMerge](/docs/en/sql-reference/aggregate-functions/reference/quantilemerge) — Alternative percentile aggregation
