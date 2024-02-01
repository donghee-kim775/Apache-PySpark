import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) !=2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
    
    mnm_file = sys.argv[1]

    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())
    
    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file))
    
    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending = False))
    
    count_mnm_df.show(n=60, truncate=False)

    count_mnm_df.explain(True)