
# Energy Trend Data Analysis

## A) Query Examples for Analysis

### Year-to-Year Data Trend Analysis:
- **How did Indigenous Production, Import, and Export Products change between 1999 and 2000?**: Analyze the differences in production, imports, and exports between these years.
- **Year-over-Year Changes in Indigenous Production (1999–2003)**: Track how Indigenous production fluctuated year-over-year.

### Aggregation Product-wise and Sub-product-wise:
- **Total Production in the 4th Quarter of 2001**: Calculate the total production across all sub-products for this period.
- **Crude Oil & NGLs vs Feedstocks Production (1999–2003)**: Compare production trends for Crude Oil, NGLs, and Feedstocks over the specified time frame.

### Filtering by Sub-products:
- **Total Crude Oil & NGLs Production Across All Quarters**: Aggregate the total production for Crude Oil and NGLs for all available quarters.
- **Quarterly Crude Oil & NGLs Production (1999–2003)**: Display the production data for these products by quarter over the years.

### Time-Range Queries:
- **Total Indigenous Production (Q1 1999 – Q2 2003)**: Filter and sum Indigenous production data over this time range.
- **Crude Oil & NGLs Production in 2000**: Show production data for Crude Oil and NGLs for the year 2000.

## B) Serializable Isolation in Databricks
Databricks implements serializable isolation as the highest level of isolation in ACID transactions. It ensures that transactions are executed in a way that is equivalent to serial execution, even when running concurrently. This is achieved through Optimized Writes, Delta Lake, and upsert operations using MERGE.

### Key Features:
- **Optimized Writes**: Reduce the overhead of small files and improve write throughput.
- **Upserts Using MERGE**: Efficiently handle updates to existing records.

## C) Handling Concurrency, Deduplication, and Upserts

### Concurrency:
- Leverage Delta Lake’s ACID Transactions for data consistency.
- Use Optimized Writes and Batch Writes to improve performance under high-throughput workloads.
- Partitioning and Auto-compaction help handle high-frequency writes and improve query performance.

### Deduplication and Upserts:
- **MERGE** operation ensures that duplicate rows are not written by using a unique identifier.
- The **MERGE** command efficiently manages both inserts and updates, ensuring that rows are either updated or inserted as needed.
