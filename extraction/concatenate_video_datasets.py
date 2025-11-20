#!/usr/bin/env python3

import csv
import os

FILES = [
    "data_0-99.csv",
    "data_100-999.csv",
    "data_1000-9999.csv",
    "data_10000-99999.csv",
    "data_100000-999999.csv",
    "data_1000000-10000000.csv",
    "data_10000000-50000000.csv",
    "data_50000000-100000000.csv",
    "data_100000000+.csv",
]

OUTPUT = "final_dataset2.csv"

def main():
    rows = []
    header = None

    for fname in FILES:
        if not os.path.exists(fname):
            print(f"Skipping missing file: {fname}")
            continue

        print(f"Reading: {fname}")

        with open(fname, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # use header of the first existing file
            if header is None:
                header = reader.fieldnames

            for row in reader:
                rows.append(row)

    if header is None:
        print("No CSV files found. Exiting.")
        return

    print(f"Writing {len(rows)} rows → {OUTPUT}")

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    print("Done.")

if __name__ == "__main__":
    main()
