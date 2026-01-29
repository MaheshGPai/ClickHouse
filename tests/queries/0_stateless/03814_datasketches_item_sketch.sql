-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

SELECT 'Test 1: Basic estimate exactness (small domain)';
WITH s AS (
    SELECT serializedItemSketch(concat('item_', toString(number % 10))) AS sk
    FROM numbers(100)
)
SELECT
    estimateFromItemSketch((SELECT sk FROM s), 'item_0') = 10 AS ok0,
    estimateFromItemSketch((SELECT sk FROM s), 'item_5') = 10 AS ok5,
    estimateFromItemSketch((SELECT sk FROM s), 'item_9') = 10 AS ok9;

SELECT 'Test 2: MergeSerializedItemSketch merges partitions';
WITH parts AS (
    SELECT
        if(number < 50, 'a', 'b') AS part,
        concat('item_', toString(number % 10)) AS item
    FROM numbers(100)
),
sketches AS (
    SELECT part, serializedItemSketch(item) AS sk
    FROM parts
    GROUP BY part
),
merged AS (
    SELECT mergeSerializedItemSketch(sk) AS sk
    FROM sketches
)
SELECT
    estimateFromItemSketch((SELECT sk FROM merged), 'item_0') = 10 AS ok0,
    estimateFromItemSketch((SELECT sk FROM merged), 'item_7') = 10 AS ok7;

SELECT 'Test 3: Weights are respected';
WITH s AS (
    SELECT serializedItemSketch(concat('k', toString(number % 3)), toUInt64(2)) AS sk
    FROM numbers(30)
)
SELECT
    estimateFromItemSketch((SELECT sk FROM s), 'k0') = 20 AS k0_ok,
    estimateFromItemSketch((SELECT sk FROM s), 'k1') = 20 AS k1_ok,
    estimateFromItemSketch((SELECT sk FROM s), 'k2') = 20 AS k2_ok;

SELECT 'Test 4: base64_encoded parameter equivalence';
WITH s AS (
    SELECT serializedItemSketch(concat('item_', toString(number % 10))) AS sk
    FROM numbers(100)
),
m_default AS (SELECT mergeSerializedItemSketch((SELECT sk FROM s)) AS m),
m_explicit_0 AS (SELECT mergeSerializedItemSketch(0)((SELECT sk FROM s)) AS m),
m_explicit_1 AS (SELECT mergeSerializedItemSketch(1)((SELECT sk FROM s)) AS m)
SELECT
    estimateFromItemSketch((SELECT m FROM m_default), 'item_0') =
        estimateFromItemSketch((SELECT m FROM m_explicit_0), 'item_0') AS default_eq_0,
    estimateFromItemSketch((SELECT m FROM m_default), 'item_0') =
        estimateFromItemSketch((SELECT m FROM m_explicit_1), 'item_0') AS default_eq_1;

SELECT 'Test 5: Invalid input returns 0';
SELECT estimateFromItemSketch('not a sketch', 'x') = 0;

SELECT 'Test 6: Top N items';
WITH s AS (
    SELECT serializedItemSketch(concat('k', toString(number % 3))) AS sk
    FROM numbers(30)
)
SELECT
    length(topItemsFromItemSketch((SELECT sk FROM s), 2)) = 2 AS size_ok,
    arrayStringConcat(arrayMap(x -> x.1, topItemsFromItemSketch((SELECT sk FROM s), 2)), ',') AS items,
    arrayStringConcat(arrayMap(x -> toString(x.2), topItemsFromItemSketch((SELECT sk FROM s), 2)), ',') AS estimates;

