## LinkedIn Post 1: Window Functions on 2.4B Rows - Spark Success, Trino Failure

📊 **Window Functions at Scale: Spark's Advantage** 📊

After weeks of troubleshooting and performance analysis, I've uncovered something critical for anyone working with window functions at scale: 🔍

• Spark processed a 2.4 billion row window function in just ~5 minutes (wall clock) ⚡️
• Trino gave up completely after 4+ hours on identical hardware ⏱️
• Root cause: Spark spilled 246GB to disk when memory constrained, Trino couldn't 💾

🧠 The execution plans tell the full story. Check out this visualization comparing CPU utilization patterns:

[CPU utilization chart showing Spark's clear spike and completion vs Trino's sustained high utilization]

An interesting technical detail: For window functions ONLY, Trino processed 3.2B rows vs our actual 2.4B dataset. This happens because Trino performs internal retries with window functions, processing some rows multiple times. Notably, this doesn't happen with Trino's JOIN approach! 🤯

⚠️ Important methodology note: I deliberately used EMR Trino rather than serverless Amazon Athena for this analysis. Why? Because Athena (which uses Trino engine underneath) is a complete black box that provides zero visibility into CPU, memory, and disk metrics. With Athena, you get a timeout message or the dreaded "Query exhausted resources at this scale factor" error after waiting 30+ minutes, with no clue about the root cause or how to fix it. 🤷‍♂️

EMR Trino allowed me to capture detailed metrics like CPU spikes, memory ceiling effects, and disk utilization - critical data for understanding why the queries failed. These insights apply directly to Athena since it uses the same Trino engine, just without the observability. 📊

For production implementations with window functions:
• Configure adequate disk space for Spark (~250GB in our case) 💽
• Set sufficient shuffle partitions (1,000 worked well for 3B rows) 🔄
• If using Athena/Trino, avoid window functions on large datasets completely 🚫

I've documented the complete technical breakdown with benchmarks, execution plans, and recommended configurations - link in comments. 📝

Remember: Sometimes the ability to gracefully spill to disk, even if it might seem slower in theory, is what makes the difference between a query that completes and one that fails completely. In big data, finishing slower is infinitely better than not finishing at all! ⏱️💾

#DataEngineering #ApacheSpark #Trino #Athena #AWS #BigData
