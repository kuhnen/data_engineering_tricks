# Spark vs Athena: Window Functions Performance Analysis

## Executive Summary

Our performance analysis evaluated Apache Spark and Amazon Athena/Trino processing window functions on a 2.4 billion row dataset. The results revealed fundamental architectural differences that significantly impact performance:

- **Spark Window Function**: Completed in ~5 minutes (wall clock time) / ~25 minutes (CPU time)
- **Spark JOIN Approach**: Completed in ~11 minutes (wall clock) / ~55.5 minutes (CPU time)
- **Trino Window Function**: Failed after 4+ hours (280+ minutes)
- **Trino JOIN Approach**: Completed in ~17 minutes (wall clock) / ~60 minutes (CPU time)

The key finding: Spark's memory management architecture and spill-to-disk strategy enabled it to process window functions efficiently where Trino failed completely with identical hardware resources. CPU utilization patterns show Spark's efficient execution as sharp, well-defined spikes, while Trino exhibited sustained high utilization before failing.

## Test Environment

- **Dataset**: 2.4 billion rows (35M staging + 2.37B main table)
- **Infrastructure**: EMR cluster with 1 core node and 1 task node
- **Node Configuration**: 4 CPUs and 15GB memory per node
- **Query**: Window function using `row_number()` to find most recent record per ID

**Note on Row Processing**: Trino actually processed 3.2 billion rows versus the actual 2.4 billion rows. This discrepancy occurs because Trino performs internal retries, resulting in some rows being processed multiple times, adding to its computational burden.

## Key Performance Insights

### 1. Memory Management Differences

| Aspect | Spark | Athena/Trino |
|--------|-------|--------------|
| **Architecture** | Project Tungsten with off-heap memory management | JVM-based memory management |
| **Spill Strategy** | Aggressively spilled 246.6 GiB to disk | Limited spill capability for window functions |
| **Memory Utilization** | Efficient with 15GB per node + disk spill | Hit memory ceiling at only 1GB usage |
| **Peak Memory** | 319.7 GiB distributed across cluster | Failed despite available memory |
| **Memory Release** | Clear pattern of efficient release after task completion | Sustained high memory utilization |

Spark's ability to spill operations to disk when memory is constrained was the critical factor in its success. The execution plan shows Spark successfully managed memory by spilling 246.6 GiB to disk during processing.

### 2. Execution Model Differences

| Aspect | Spark | Athena/Trino |
|--------|-------|--------------|
| **Model** | Stage-based with clear boundaries | Pipeline-based with continuous flow |
| **Window Implementation** | Dedicated Window operator with separate Sort | TopNRanking operator requiring full partition state |
| **Processing Pattern** | Clear phases with efficient resource release | Sustained high resource utilization |
| **Adaptability** | Dynamic adaptation during execution | Fixed execution plan |
| **CPU Utilization** | Sharp spikes reaching 95% with clear completion | Sustained high utilization (95%) for extended periods |

Spark's stage-based execution model with clear boundaries between processing phases allowed it to manage resources more effectively than Trino's pipeline-based approach. The CPU utilization graphs show Spark's execution as distinct, well-defined spikes with clear completion, while Trino maintained high CPU utilization that eventually led to failure.

### 3. Resource Utilization

| Metric | Spark Window | Spark JOIN | Trino Window | Trino JOIN |
|--------|-------------|------------|--------------|------------|
| **Wall Clock Time** | ~5 minutes | ~11 minutes | Failed after 4+ hours | ~17 minutes |
| **CPU Time** | ~25 minutes | ~55.5 minutes | 280+ minutes before failure | ~60 minutes |
| **Memory Peak** | 319.7 GiB across cluster | 210 GiB | Failed at 1GB ceiling | ~120 GiB estimated |
| **Data Spilled** | 246.6 GiB | Minimal | Unknown (limited capability) | Minimal |
| **Shuffle Data** | 65 GB | 129 GB | 228 GB | 120 GB |
| **Disk Usage** | 40-45% | 30-35% | 50-55% | 40-45% |

The monitoring data showed Spark efficiently using resources with sharp, well-defined CPU utilization followed by effective resource release, while Trino exhibited sustained high resource utilization that eventually led to failure. The amount of shuffle data needed by Trino (228 GB) was significantly higher than Spark's window approach (65 GB), further contributing to resource pressure.

## Visualizing Performance Differences

The CPU utilization patterns clearly illustrate the fundamental differences between Spark and Trino:

- **Spark Window Execution**: 
  - Sharp spike reaching 95% CPU at the 5-minute mark
  - Clear execution phases visible in utilization graph
  - Efficient completion with CPU dropping to 0% by 30 minutes
  - Memory usage mirrors CPU pattern with efficient release after task
  
- **Spark JOIN Execution**:
  - More sustained utilization around 80-85% 
  - Gradual decline over 55 minutes of processing
  - Less pronounced but still defined execution boundaries
  - Higher shuffle requirements than window approach

- **Trino Window Execution**:
  - Sustained high CPU utilization (95%) for over 4 hours
  - No clear execution boundaries or efficient release pattern
  - Memory utilization approaching 100% throughout execution
  - Eventually failed without completing the query
  
- **Trino JOIN Execution**:
  - Similar pattern to Spark JOIN but extended over longer period
  - Successfully completed without window function memory demands
  - Higher network traffic with distinct protocol signatures
  - Required different query pattern to succeed

The performance visualizations confirm that Spark's execution model creates distinct processing phases with clear resource allocation and release, while Trino maintains sustained high resource utilization without the efficient phases seen in Spark.

### CPU Utilization Over Time

| Time | Spark Window | Spark JOIN | Trino Window | Trino JOIN |
|------|-------------|------------|--------------|------------|
| 5min | 95% | 85% | 60% | 60% |
| 10min | 85% | 80% | 90% | 85% |
| 15min | 90% | 85% | 95% | 90% |
| 20min | 40% | 80% | 95% | 95% |
| 25min | 5% | 75% | 95% | 90% |
| 30min | 0% | 70% | 95% | 90% |
| 40min | 0% | 60% | 95% | 80% |
| 50min | 0% | 30% | 95% | 70% |
| 55min | 0% | 5% | 95% | 50% |
| 60min | 0% | 0% | 95% | 20% |
| 120min | 0% | 0% | 95% | 0% |
| 240min | 0% | 0% | 75% | 0% |
| 280min | 0% | 0% | 0% | 0% |

### 4. Data Processing Efficiency

| Metric | Spark | Trino |
|--------|-------|-------|
| **Rows Processed** | 2.47 billion | 3.22 billion (attempted) |
| **Input Size** | 148 MiB + 32.9 GiB | 148 MiB + 49.7 GiB |
| **Output Records** | 35.4 million | 35.4 million (JOIN approach) |
| **Data Reduction Ratio** | 68:1 | 68:1 (successful approaches) |
| **Number of Shuffles** | 1 (Window), 3 (JOIN) | 1 (Window), Multiple (JOIN) |

Interestingly, Trino processed more rows (3.22B) than actually existed in the dataset (2.4B) due to internal retries, further increasing its workload. Despite this difference in processing, all successful approaches achieved the same 68:1 data reduction ratio from input to output rows.

## Alternative Approach: JOIN Strategy

When window functions failed in Trino, a JOIN-based approach succeeded:

```sql
-- Alternative approach avoiding window functions
WITH grouped_max AS (
  SELECT id, MAX(last_modified) as max_modified
  FROM (
    SELECT * FROM inventory_snapshot_staging.discount
    UNION ALL
    SELECT * FROM inventory_snapshot.discount
  ) combined
  GROUP BY id
),
joined AS (
  SELECT a.*
  FROM (
    SELECT * FROM inventory_snapshot_staging.discount
    UNION ALL
    SELECT * FROM inventory_snapshot.discount
  ) a
  JOIN grouped_max b ON a.id = b.id AND a.last_modified = b.max_modified
)
SELECT * FROM joined LIMIT 10;
```

This approach worked because:
1. The GROUP BY operation immediately reduced data volume
2. The aggregation operations are more memory-efficient than window functions
3. It avoided maintaining ordering information for all records within each partition
4. It broke the problem into discrete stages with manageable memory footprints

## Implications for Amazon Athena

While the test used Trino on EMR for visibility into metrics, the findings directly apply to Amazon Athena since it uses Trino (formerly PrestoSQL) as its execution engine. Additional considerations for Athena include:

### Observability Challenges in Serverless Athena

1. **Black Box Nature**: Serverless Athena provides almost no visibility into:
   - CPU utilization 
   - Memory consumption
   - Resource allocation per query
   - Detailed operator metrics

2. **Limited Metrics**: Athena only provides:
   - Query duration
   - Data scanned
   - Success/failure status
   - Basic query stage information

3. **No Access to Execution Nodes**: With Athena, you cannot:
   - Monitor memory pressure in real-time
   - Observe CPU spikes
   - See disk spill operations
   - Track network traffic patterns

4. **Troubleshooting Limitations**: Without the detailed metrics available in EMR monitoring:
   - Failed queries appear simply as timeouts or errors
   - Root cause analysis is extremely difficult
   - Performance optimization is largely trial-and-error

5. **Default Query Timeout**: Athena's 30-minute default timeout would have terminated the window function query before completion

6. **Cost Implications**: Athena charges by data scanned ($5.00 per TB), so inefficient query patterns increase costs

## Conclusion

The performance analysis reveals fundamental architectural differences between Spark and Athena/Trino that make Spark significantly more efficient for window functions on large datasets. Understanding these differences allows organizations to select the appropriate engine for specific workloads and optimize query patterns accordingly.

Key takeaways from this analysis:

1. **Architectural Differences Are Fundamental**: Spark's stage-based execution and aggressive spill-to-disk strategy enabled it to succeed where Trino failed with identical hardware. These are not minor implementation differences but fundamental architectural approaches.

2. **Resource Utilization Efficiency Varies Dramatically**: Spark completed window functions in ~25 minutes CPU time while Trino failed after 280+ minutes, demonstrating a massive efficiency gap.

3. **Memory Requirements Are Asymmetric**: Based on Spark's 246.6 GiB spill to disk, Trino would likely need 50-60GB minimum memory to complete the same operation, confirming our estimate that Trino needs 3-4x more memory than Spark.

4. **Workload-Specific Engine Selection Is Essential**: For large-scale window functions, Spark demonstrates clear superiority. For interactive and federated queries, Trino/Athena may still be appropriate with optimized query patterns.

5. **Different Query Patterns Are Required**: Organizations using both engines should implement engine-specific SQL patterns, using window functions for Spark and JOIN approaches for Athena/Trino.

For organizations requiring both engines, a hybrid approach leveraging each system's strengths appears optimal: use Spark for heavy processing with window functions, then use Athena/Trino for interactive queries on pre-processed data.
