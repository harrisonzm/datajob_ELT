#!/usr/bin/env python3

from extraction.extraction import load_optimized_fast , execute_extraction

def main():
    csv_path = "data_jobs.csv"
    execute_extraction(csv_path)

if __name__ == "__main__":
    main()