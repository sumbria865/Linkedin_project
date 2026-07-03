"""
Extract common job_links across all 3 LinkedIn CSVs, properly handling:
- quoted fields (so URLs wrapped in quotes match unquoted ones)
- embedded newlines inside quoted fields (e.g. multi-line job descriptions)

Uses Python's csv module in streaming mode -- never loads a whole file into
memory, so this is safe even for the 4.8GB job_summary.csv.
"""
import csv
import sys

csv.field_size_limit(sys.maxsize)  # some job_summary fields are very long

def get_links(path):
    links = set()
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if row:  # skip any blank rows
                links.add(row[0].strip())
    return links

print("Reading job_skills.csv ...")
skills_links = get_links("job_skills.csv")
print(f"  {len(skills_links)} links")

print("Reading linkedin_job_postings.csv ...")
listings_links = get_links("linkedin_job_postings.csv")
print(f"  {len(listings_links)} links")

print("Reading job_summary.csv (this is the big one, may take a minute) ...")
summary_links = get_links("job_summary.csv")
print(f"  {len(summary_links)} links")

common = skills_links & listings_links & summary_links
print(f"\nCommon links across all 3 files: {len(common)}")

with open("common_links.txt", "w") as f:
    for link in common:
        f.write(link + "\n")

print("Written to common_links.txt")
