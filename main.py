#!/usr/bin/env python3

import logging
import sys
from datetime import datetime
from extraction.extraction import execute_extraction

# Configurar logging principal
def setup_main_logging():
    """Configura el sistema de logging principal"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/pipeline.log', encoding='utf-8')
        ]
    )

def main():
    setup_main_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("="*70)
    logger.info(" "*20 + "INICIO DEL PIPELINE ETL")
    logger.info("="*70)
    logger.info(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    csv_path = "data_jobs.csv"
    
    try:
        logger.info("Iniciando proceso de extracción...")
        # Usar método estándar (más robusto) por defecto
        # Para método ultra-rápido COPY, cambiar a: use_copy=True
        execute_extraction(csv_path, use_copy=True)
        logger.info("")
        logger.info("="*70)
        logger.info(" "*20 + "PIPELINE COMPLETADO")
        logger.info("="*70)
        
    except Exception as e:
        logger.error("="*70)
        logger.error(" "*20 + "ERROR EN PIPELINE")
        logger.error("="*70)
        logger.error(f"Error: {str(e)}")
        logger.exception("Traceback completo:")
        sys.exit(1)

if __name__ == "__main__":
    main()