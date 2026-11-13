# PySpark interview challenge — solutions

Only open this after you've tried the challenge. Your answers may be different and still correct: check the results, not the exact code.

## Task 1 — Schemas

```python
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType,
)

customers_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("segment", StringType()),
    StructField("signup_date", DateType()),
])

products_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("unit_price", DoubleType()),
])

orders_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("order_ts", TimestampType()),
    StructField("status", StringType()),
])

events_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
])

customers = spark.read.csv("/data/customers.csv", header=True, schema=customers_schema)
products = spark.read.csv("/data/products.csv", header=True, schema=products_schema)
orders = spark.read.csv("/data/orders.csv", header=True, schema=orders_schema)
events = spark.read.json("/data/web_events.jsonl", schema=events_schema)

for df in (customers, products, orders, events):
    df.printSchema()
```

Note: `web_events.jsonl` has `event_id` too. Reading with an explicit schema ignores fields you don't list.

## Task 2 — Top 5 customers by revenue

```python
delivered = (
    orders.filter(F.col("status") == "delivered")
    .join(products, "product_id")
    .withColumn("amount", F.col("quantity") * F.col("unit_price"))
)

(
    delivered.groupBy("customer_id")
    .agg(
        F.round(F.sum("amount"), 2).alias("revenue"),
        F.count("*").alias("order_count"),
    )
    .join(customers.select("customer_id", "name", "city"), "customer_id")
    .select("customer_id", "name", "city", "revenue", "order_count")
    .orderBy(F.desc("revenue"))
    .limit(5)
    .show()
)
```

Each row of `orders` is one order with one product, so after the product join `count("*")` is the number of orders. If an order could have several products, you'd need to count distinct `order_id` instead.

## Task 3 — Monthly revenue in 2024

```python
(
    delivered.filter(F.year("order_ts") == 2024)
    .withColumn("month", F.date_format("order_ts", "yyyy-MM"))
    .groupBy("month")
    .agg(F.round(F.sum("amount"), 2).alias("revenue"))
    .orderBy("month")
    .show()
)
```

Don't use `substring` on the timestamp string. It breaks if the format changes.

## Task 4 — Customers who never bought

```python
(
    customers.join(orders, "customer_id", "left_anti")
    .select("customer_id", "name", "segment")
    .show()
)
```

`left_anti` keeps the rows from the left side that have no match on the right. It's the standard answer for "rows with no match". A `left` join with a `null` filter also works, but it's longer.

## Task 5 — Parse the customer number

```python
customers_num = customers.withColumn(
    "customer_number",
    F.split(F.col("name"), "_").getItem(1).cast("int"),
)

customers_num.filter(F.col("customer_number") % 100 == 0).count()
```

Use `F.split` with `getItem`, not Python string methods, so the work stays distributed.

## Task 6 — Conversion rate per product

```python
stats = (
    events.groupBy("product_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
    )
    .filter(F.col("views") >= 10)
    .withColumn("conversion_rate", F.round(F.col("purchases") / F.col("views"), 3))
)

(
    stats.join(products.select("product_id", "product_name"), "product_id")
    .select("product_name", "views", "purchases", "conversion_rate")
    .orderBy(F.desc("conversion_rate"))
    .limit(5)
    .show()
)
```

The conditional sum is the usual pattern for counting several event types in one pass. Filtering on `views` after the aggregation is correct, because it's a per-product count.

## Task 7 — Best product per category

```python
by_category = Window.partitionBy("category").orderBy(F.desc("revenue"))

(
    delivered.groupBy("category", "product_name")
    .agg(F.round(F.sum("amount"), 2).alias("revenue"))
    .withColumn("rank", F.row_number().over(by_category))
    .filter(F.col("rank") == 1)
    .select("category", "product_name", "revenue")
    .show()
)
```

Use `row_number` when you want exactly one row per category. Use `rank` or `dense_rank` if ties should all appear.

## Task 8 — Performance question

```python
joined = orders.join(products, "product_id")
joined.explain()

from pyspark.sql.functions import broadcast

joined_broadcast = orders.join(broadcast(products), "product_id")
joined_broadcast.explain()
```

1. **Strategy:** with default settings, Spark uses `SortMergeJoin` for two large tables. For a small table like `products`, Spark can choose `BroadcastHashJoin` automatically, if the table is below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default).
2. **Avoiding the shuffle of `orders`:** broadcast the small table with `broadcast(products)`. The plan then shows `BroadcastHashJoin` and no `Exchange` on the `orders` side.
3. **Why broadcast can fail:** the small table is copied to every executor's memory. If it's too big, executors run out of memory or the broadcast times out. `spark.sql.autoBroadcastJoinThreshold` sets the size limit for automatic broadcasts. Set it to `-1` to turn automatic broadcast off.

## How to score it

| Area | What a good answer does |
|---|---|
| Schemas | Explicit types, dates and timestamps typed correctly |
| Joins | `left_anti` for missing rows, the right join key, no duplicate counts |
| Aggregations | Conditional sums or grouped counts, filter applied at the right step |
| Windows | `Window.partitionBy`, correct ordering, `row_number` for one per group |
| Performance | Reads the plan, uses `broadcast` on small tables, knows the thresholds |

Good answers also check their own results. For example, the number of customers with no orders plus customers with orders should equal the total number of customers.
