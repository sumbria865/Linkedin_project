"""
Filter each of the 3 CSVs down to only rows whose job_link is in
sample_links.txt, writing proper (quoted, comma-correct) CSV output.
Streaming -- safe for large files.
"""
import csv
import sys
import random

csv.field_size_limit(sys.maxsize)

random.seed(42)

# Load the common links and take a random sample
with open("common_links.txt") as f:
    all_common = [line.strip() for line in f if line.strip()]

SAMPLE_SIZE = 50000
sample = set(random.sample(all_common, min(SAMPLE_SIZE, len(all_common))))
print(f"Sampled {len(sample)} links out of {len(all_common)} common links")

with open("sample_links.txt", "w") as f:
    for link in sample:
        f.write(link + "\n")

def filter_csv(in_path, out_path):
    kept = 0
    with open(in_path, newline='', encoding='utf-8', errors='replace') as fin, \
         open(out_path, "w", newline='', encoding='utf-8') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        header = next(reader)
        writer.writerow(header)
        for row in reader:
            if row and row[0].strip() in sample:
                writer.writerow(row)
                kept += 1
    print(f"{in_path} -> {out_path}: {kept} rows kept")

filter_csv("job_skills.csv", "job_skills_sample.csv")
filter_csv("job_summary.csv", "job_summary_sample.csv")
filter_csv("linkedin_job_postings.csv", "linkedin_job_postings_sample.csv")

print("\nDone.")
