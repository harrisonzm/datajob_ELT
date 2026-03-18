#!/usr/bin/env python3

from extraction.extraction import load_optimized_fast, execute_extraction, execute_extraction_with_transforms

def main():
    csv_path = "data_jobs.csv"
    
    # Opción 1: Carga rápida solo a job_posting (sin transformaciones)
    # execute_extraction(csv_path)
    
    # Opción 2: Carga con todas las transformaciones (reemplaza dbt)
    execute_extraction_with_transforms(csv_path)

if __name__ == "__main__":
    main()