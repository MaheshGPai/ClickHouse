---
description: 'Creates a TDigest sketch and returns centroids as a JSON string for percentile estimation'
slug: /sql-reference/aggregate-functions/reference/serializedTDigest
title: 'serializedTDigest'
doc_type: 'reference'
---

# serializedTDigest

Creates a TDigest sketch from numeric values and returns the centroids as a JSON string. TDigest is an algorithm for accurate estimation of percentiles, particularly at the extremes (p99, p99.9, etc.).

## Syntax

```sql
serializedTDigest(expression)
```

## Arguments

- `expression` — Numeric expression. Supported types: [Int](/docs/en/sql-reference/data-types/int-uint.md), [UInt](/docs/en/sql-reference/data-types/int-uint.md), [Float](/docs/en/sql-reference/data-types/float.md).

## Returned Value

- JSON string representing centroids where keys are centroid means and values are centroid weights. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Examples

```sql
SELECT serializedTDigest(number) AS tdigest_centroids
FROM numbers(1000);
```

## See Also

- [quantileTDigest](/docs/en/sql-reference/aggregate-functions/reference/quantiletdigest) — TDigest-based percentile function
- [serializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeddoublesketch) — Alternative quantiles algorithm
