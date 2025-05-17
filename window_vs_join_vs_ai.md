# When AI Gives Bad Database Advice: Solving Billion-Row Query Problems



I recently faced a challenging data performance (Athena/Trino) problem : efficiently merging a 2.2 billion historic row table (538 GB) with a 28 million row updated table from a mongo database. 

What seemed like a standard ETL task turned into an unexpected lesson in the gap between theoretical and practical query optimization.



## The Problem: Two Approaches, Opposite Results



Our traditional approach used a common efficient pattern:



staging_table =>  updated_mongo_table

main_table => updated_mongo_table + historical_table



### Query 1:



```sql

WITH initial AS (

    SELECT * FROM staging_table

    UNION ALL

    SELECT * FROM main_table

),

numbered AS (

    SELECT *,

        row_number() OVER (PARTITION BY id ORDER BY last_modified DESC) AS row_number

    FROM initial

)

SELECT * FROM numbered WHERE row_number = 1;

```



This query started failing with "Query exhausted resources" errors as our data grew.



I hypothesized a different approach using three specific joins instead:



### Query 2:



```sql

WITH

new_records AS (

    SELECT stg.* FROM staging_table stg

    LEFT JOIN main_table snap ON stg.id = snap.id

    WHERE snap.id IS NULL

),

records_to_update AS (

    SELECT stg.* FROM staging_table stg

    JOIN main_table snap ON stg.id = snap.id

),

existing_records AS (

    SELECT snap.* FROM main_table snap

    LEFT JOIN staging_table stg ON snap.id = stg.id

    WHERE stg.id IS NULL

)

SELECT * FROM new_records UNION ALL

SELECT * FROM records_to_update UNION ALL

SELECT * FROM existing_records;

```



## AI Said I Was Wrong (But I Wasn't)



I asked an AI assistant to analyze both approaches. It confidently declared:



"The first (original) query is likely to be more efficient at this scale because it performs fewer table scans, has a simpler execution plan, and follows a well-established pattern for incremental updates."



However, when I tested both queries with a simple LIMIT 10. (Since we are joining the full the dataset, we can use limit 10 the engine will need to do the full calculation before providing me with 10 rows)



**Query 1 (Window Function):** Failed with "Query exhausted resources"

**Query 2 (My Join Approach):** Completed in 31.3 seconds, scanning ~48GB  (The 538GB above is the total data without compression)



Surprisingly, my supposedly "inefficient" approach succeeded where the theoretically optimal one failed!



## The Reality: What AI Missed



The window function approach required Athena to:

1. Load 2.27 billion combined rows into memory - From googke: "Apache Spark and Amazon Athena both support window functions, but they differ in how they handle disk spills and overall processing. Spark, with its in-memory processing, can spill to disk when memory is insufficient, while Athena, a serverless query engine, relies on a distributed SQL engine and typically doesn't spill to disk for most common use cases."



2. Partition all data by ID

3. Sort each partition by timestamp

4. Then calculate row numbers



By contrast, my join approach:

1. Processed each join separately

2. Never load the full dataset in memory

3. Leveraged Athena's ability to optimize individual join operations



## The Lesson: Trust But Verify



This experience taught me several valuable lessons:



1. **AI is a tool, not an oracle**: It excels at theoretical analysis but can miss critical real-world execution factors.



2. **Map Reduce engines have personalities**: Athena has specific limitations that theory doesn't account for.



3. **I miss spark**: More on this on my next post.



We will implement my solution with no additional partitioning, reducing processing time by 76% (before "Query exhausted resources" issue) and eliminating resource errors.



## Final Thought



Sometimes, challenging conventional wisdom—even when endorsed by AI—leads to the breakthrough solution. As data engineers, our experience and critical thinking remain irreplaceable, especially at billion-row scale.



---



*Have you found that a theoretically "inefficient" approach actually performed better in practice? Share your experiences!*



*#DataEngineering #BigData #AWS #Athena #DatabaseOptimization*

