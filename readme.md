# LinkedIn Job Market Analysis — Spark + HDFS

Big data analysis of 1.3M LinkedIn job postings using a real local Hadoop
(HDFS) + Apache Spark cluster, built and debugged from scratch on WSL2.

**Data source**: [1.3M LinkedIn Jobs and Skills 2024](https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024) (Kaggle)

## What this project does

Analyzes LinkedIn job postings to answer:
- What are the most in-demand skills across job postings?
- Which companies post the most jobs?
- How do skill requirements vary by company and job level?
- What's the geographic distribution of postings?

Three raw datasets (`job_skills.csv`, `job_summary.csv`,
`linkedin_job_postings.csv`) are cleaned, joined on `job_link`, stored in
HDFS, processed with Spark DataFrames/SQL, and visualized with
matplotlib/seaborn. Final aggregates are exported for a Power BI dashboard.

## Tech stack
- **Apache Hadoop 3.3.6** — HDFS for distributed file storage
- **Apache Spark 3.5.0** (PySpark) — distributed data processing, with
  Hive support enabled via Spark's built-in embedded Derby metastore
  (no separate Hive server needed)
- **Python** — pandas, matplotlib, seaborn for cleaning/visualization
- **Power BI** — final interactive dashboard
- Environment: WSL2 (Ubuntu 22.04) on Windows

---

## Setup — from a clean environment to a working cluster

This section documents the actual setup process, including the real
issues hit along the way (kept in for anyone reproducing this).

### 1. Prerequisites
- Java 8 (`JAVA_HOME` must point to Java 8 specifically — Hadoop 3.x
  does not run reliably on newer JDKs)
- Hadoop 3.3.6, extracted to `~/hadoop`
- SSH server running locally (Hadoop's start scripts use SSH even for
  single-node/pseudo-distributed setups)

### 2. Configuring HDFS (single-node / pseudo-distributed)

`~/hadoop/etc/hadoop/core-site.xml`:
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

`~/hadoop/etc/hadoop/hdfs-site.xml` — **explicit storage directories are
important here**. Using Hadoop's default `/tmp/hadoop-<user>/...` paths
caused a `NameNode`/`DataNode` clusterID mismatch after a reformat
(since `/tmp` isn't guaranteed persistent), so storage is pointed at a
permanent location instead:
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/codebind/hadoopdata/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/codebind/hadoopdata/datanode</value>
  </property>
</configuration>
```

### 3. Starting the cluster
```bash
sudo service ssh start      # SSH must be running before HDFS starts
hdfs namenode -format        # first time only — wipes any existing HDFS data
start-dfs.sh
jps                           # should show NameNode, DataNode, SecondaryNameNode
```
Note: HDFS does **not** persist across a fresh WSL session automatically.
`sudo service ssh start` and `start-dfs.sh` need to be re-run every time
you start a new terminal session for this project.

Verify the cluster is healthy:
```bash
hdfs dfsadmin -report
```
Look for `Live datanodes (1)` in the output.

### 4. Installing Spark
```bash
cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
mv spark-3.5.0-bin-hadoop3 spark
```
Added to `~/.bashrc`:
```bash
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

### 5. Python dependencies
```bash
pip3 install pyspark==3.5.0 pandas matplotlib seaborn plotly wordcloud
```

---

## Data preparation — issues hit and how they were resolved

Working with a real, messy dataset surfaced a few problems that don't
show up with toy CSVs:

**1. `job_summary.csv` contains embedded newlines inside quoted fields**
(multi-line job description text). Naive line-based tools (`cut`,
`awk`, `sort`) treat every physical line as a separate record, which
silently corrupts the file's row structure.

**2. `linkedin_job_postings.csv` quotes every field, including the
`job_link` URL itself**, while the other two files leave it unquoted.
This meant a naive string comparison across files never matched, even
for genuinely identical links.

**Fix**: switched from shell text tools to Python's built-in `csv`
module (`csv.reader`/`csv.writer`), which correctly handles quoting and
embedded newlines. See `find_common_links.py` and `filter_sample.py`.

**3. Sampling had to be done by finding `job_link`s common to all three
files first**, then filtering each file down to that same set — simply
taking the first N rows of each file independently (e.g. `head -n
200000`) produced almost no overlap after the join, since the files
aren't ordered identically.

**4. Spark's default CSV reader also splits on embedded newlines**
unless told otherwise. Fixed by reading `job_summary.csv` with:
```python
spark.read.option("multiLine", "true").option("escape", '"').csv(...)
```

---

## Running the analysis

```bash
# 1. Start the cluster (see "Starting the cluster" above)

# 2. Build a sample with consistent job_links across all 3 files
cd ~/linkedin_data
python3 find_common_links.py
python3 filter_sample.py

# 3. Upload sample data to HDFS
hdfs dfs -mkdir -p /user/codebind/linkedin
hdfs dfs -put ~/linkedin_data/job_skills_sample.csv /user/codebind/linkedin/job_skills.csv
hdfs dfs -put ~/linkedin_data/job_summary_sample.csv /user/codebind/linkedin/job_summary.csv
hdfs dfs -put ~/linkedin_data/linkedin_job_postings_sample.csv /user/codebind/linkedin/linkedin_job_postings.csv

# 4. Run the Spark analysis
cd ~/linkedin_project
python3 pyspark_analysis.py
```

This reads the three files from HDFS, cleans them (null handling,
deduplication, column standardization), joins them into a single
50,000-row dataset, saves it as a Spark-managed table, and runs SQL
queries against it to produce:
- Top 10 companies by job posting volume
- Top 10 most-requested skills (via `explode(split(job_skills, ','))`
  to turn comma-separated skill lists into individual rows)

Results are saved as `output_top_companies.png` / `.csv` and
`output_top_skills.png` / `.csv`.

## Bringing results into Power BI
The exported CSVs (`output_top_companies.csv`, `output_top_skills.csv`)
are imported into Power BI Desktop (Get Data > Text/CSV) to build the
final interactive dashboard.

---

## Repo structure
```
.
├── README.md
├── find_common_links.py      # finds job_links present in all 3 raw CSVs
├── filter_sample.py           # samples + filters the 3 CSVs consistently
├── pyspark_analysis.py        # main Spark job: clean, join, analyze
├── output_top_companies.csv
├── output_top_companies.png
├── output_top_skills.csv
└── output_top_skills.png
```

## What I'd do differently at larger scale
- Use a real multi-node HDFS cluster (or cloud equivalent like S3 +
  EMR) rather than single-node, since the whole point of HDFS is
  distributing storage/compute across machines
- Use a proper Hive metastore shared across jobs/users instead of
  Spark's embedded single-user Derby metastore
- Automate the sampling/upload steps into a single script rather than
  running them manually
