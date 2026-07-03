# LinkedIn Job Market Analysis — PySpark + HDFS version (real cluster, no Docker)
# Run this directly on your WSL2 machine with Hadoop + Spark installed locally.

import matplotlib
matplotlib.use("Agg")  # headless backend — saves files instead of opening a window,
                        # required since a plain terminal has no display server

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, trim, explode, split, desc, count, isnan
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# -----------------------------------------------------------------------
# STEP 1: Start Spark session
# enableHiveSupport() works here WITHOUT a separate Hive server — Spark
# uses a built-in embedded Derby metastore. This is what lets us use
# spark.sql(...) and saveAsTable(...) just like the original notebook.
# -----------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("LinkedIn Job Analysis")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.sql.warehouse.dir", "/home/codebind/spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark version:", spark.version)

# -----------------------------------------------------------------------
# STEP 2: Load data FROM HDFS (not local disk)
# Before running this, the CSVs must already be uploaded to HDFS —
# see README step "Upload data to HDFS".
# -----------------------------------------------------------------------

HDFS_BASE = "hdfs://localhost:9000/user/codebind/linkedin"

job_skills = spark.read.csv(f"{HDFS_BASE}/job_skills.csv", header=True, inferSchema=True)
job_summary = spark.read.option("multiLine", "true").option("escape", '"').csv(
    f"{HDFS_BASE}/job_summary.csv", header=True, inferSchema=True
)
job_listings = spark.read.csv(f"{HDFS_BASE}/linkedin_job_postings.csv", header=True, inferSchema=True)

print("Job Skills rows:", job_skills.count())
print("Job Summary rows:", job_summary.count())
print("Job Listings rows:", job_listings.count())

job_skills.printSchema()
job_skills.show(5)

# -----------------------------------------------------------------------
# STEP 3: Data cleaning
# -----------------------------------------------------------------------

def check_null_values(df, dataset_name):
    null_counts = df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    )
    print(f"\nNull values in {dataset_name}:")
    null_counts.toPandas().T.rename(columns={0: "null_count"}).pipe(print)

check_null_values(job_skills, "Job Skills")
check_null_values(job_summary, "Job Summary")
check_null_values(job_listings, "Job Listings")

def clean_dataset(df):
    for c in df.columns:
        df = df.withColumn(c, when(col(c).isNotNull(), col(c)).otherwise("Unknown"))
    df = df.dropDuplicates()
    df = df.select([trim(col(c)).alias(c.strip().lower()) for c in df.columns])
    return df

job_skills = clean_dataset(job_skills)
job_summary = clean_dataset(job_summary)
job_listings = clean_dataset(job_listings)

# -----------------------------------------------------------------------
# STEP 4: Merge datasets (SQL-style inner joins)
# -----------------------------------------------------------------------

merged_data = job_skills.join(job_summary, on="job_link", how="inner")
merged_data = merged_data.join(job_listings, on="job_link", how="inner")

print("\nMerged dataset:", merged_data.count(), "rows,", len(merged_data.columns), "columns")
merged_data.show(5)

valid_job_types = ["Remote", "Onsite", "Hybrid"]
merged_data = merged_data.filter(col("job_type").isin(valid_job_types))

# -----------------------------------------------------------------------
# STEP 5: Save as a Hive table so we can run spark.sql() queries
# -----------------------------------------------------------------------

merged_data.write.mode("overwrite").saveAsTable("linkedin_job_postings_2024")
spark.sql("SELECT * FROM linkedin_job_postings_2024").show(5)

# -----------------------------------------------------------------------
# STEP 6: Analysis — Top companies by job postings
# -----------------------------------------------------------------------

top_companies = spark.sql("""
    SELECT company, COUNT(*) AS count
    FROM linkedin_job_postings_2024
    GROUP BY company
    ORDER BY count DESC
    LIMIT 10
""").toPandas()

plt.figure(figsize=(12, 6))
sns.barplot(x="count", y="company", data=top_companies, palette="viridis")
plt.title("Top 10 Companies by Job Postings")
plt.tight_layout()
plt.savefig("/home/codebind/output_top_companies.png")
plt.close()
print("Saved: ~/output_top_companies.png")

# -----------------------------------------------------------------------
# STEP 7: Analysis — Top skills (explode + split pattern)
# -----------------------------------------------------------------------

top_skills = spark.sql("""
    SELECT skill, COUNT(*) AS count
    FROM (
        SELECT explode(split(job_skills, ',')) AS skill
        FROM linkedin_job_postings_2024
    ) skills
    GROUP BY skill
    ORDER BY count DESC
    LIMIT 10
""").toPandas()

plt.figure(figsize=(12, 6))
sns.barplot(x="count", y="skill", data=top_skills, palette="cubehelix")
plt.title("Top 10 Required Skills")
plt.tight_layout()
plt.savefig("/home/codebind/output_top_skills.png")
plt.close()
print("Saved: ~/output_top_skills.png")

# -----------------------------------------------------------------------
# STEP 7b: Additional exports for a richer Power BI dashboard
# -----------------------------------------------------------------------

# Job type distribution (Remote / Onsite / Hybrid)
job_type_dist = spark.sql("""
    SELECT job_type, COUNT(*) AS count
    FROM linkedin_job_postings_2024
    GROUP BY job_type
    ORDER BY count DESC
""").toPandas()

# Job level distribution (Entry, Mid senior, Associate, etc.)
job_level_dist = spark.sql("""
    SELECT job_level, COUNT(*) AS count
    FROM linkedin_job_postings_2024
    GROUP BY job_level
    ORDER BY count DESC
""").toPandas()

# Top 15 job locations
top_locations = spark.sql("""
    SELECT job_location, COUNT(*) AS count
    FROM linkedin_job_postings_2024
    GROUP BY job_location
    ORDER BY count DESC
    LIMIT 15
""").toPandas()

# Top 10 job titles
top_titles = spark.sql("""
    SELECT job_title, COUNT(*) AS count
    FROM linkedin_job_postings_2024
    GROUP BY job_title
    ORDER BY count DESC
    LIMIT 10
""").toPandas()

# Skill demand by job level (which skills matter most at Entry vs Senior etc.)
skills_by_level = spark.sql("""
    SELECT job_level, skill, COUNT(*) AS count
    FROM (
        SELECT job_level, explode(split(job_skills, ',')) AS skill
        FROM linkedin_job_postings_2024
    ) t
    GROUP BY job_level, skill
    ORDER BY job_level, count DESC
""").toPandas()

# -----------------------------------------------------------------------
# STEP 8: Export results for Power BI
# -----------------------------------------------------------------------

top_companies.to_csv("/home/codebind/output_top_companies.csv", index=False)
top_skills.to_csv("/home/codebind/output_top_skills.csv", index=False)
job_type_dist.to_csv("/home/codebind/output_job_type_distribution.csv", index=False)
job_level_dist.to_csv("/home/codebind/output_job_level_distribution.csv", index=False)
top_locations.to_csv("/home/codebind/output_top_locations.csv", index=False)
top_titles.to_csv("/home/codebind/output_top_titles.csv", index=False)
skills_by_level.to_csv("/home/codebind/output_skills_by_level.csv", index=False)

print("\nDone. 7 CSV files exported to your home directory (~) for Power BI.")

spark.stop()
