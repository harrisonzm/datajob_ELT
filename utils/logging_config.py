"""
Configuración centralizada de logging para el pipeline ETL
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

# Crear carpeta de logs si no existe
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Niveles de logging
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

def setup_logger(
    name: str,
    log_file: str = None,
    level: str = 'INFO',
    console_output: bool = True
) -> logging.Logger:
    """
    Configura y retorna un logger con handlers para consola y archivo
    
    Args:
        name: Nombre del logger (usualmente __name__)
        log_file: Nombre del archivo de log (opcional)
        level: Nivel de logging ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
        console_output: Si True, también muestra logs en consola
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVELS.get(level, logging.INFO))
    
    # Evitar duplicación de handlers
    if logger.handlers:
        return logger
    
    # Formato detallado para logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para archivo
    if log_file:
        file_handler = logging.FileHandler(
            LOGS_DIR / log_file,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Handler para consola
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(LOG_LEVELS.get(level, logging.INFO))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger

def log_execution_summary(logger: logging.Logger, start_time: float, end_time: float, success: bool):
    """
    Registra un resumen de la ejecución
    
    Args:
        logger: Logger a usar
        start_time: Tiempo de inicio (time.time())
        end_time: Tiempo de fin (time.time())
        success: Si la ejecución fue exitosa
    """
    duration = end_time - start_time
    
    logger.info("="*70)
    if success:
        logger.info("✓ EJECUCIÓN COMPLETADA EXITOSAMENTE")
    else:
        logger.error("✗ EJECUCIÓN FALLÓ")
    
    logger.info(f"Duración total: {duration:.2f} segundos ({duration/60:.2f} minutos)")
    logger.info(f"Hora de finalización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)

def log_step(logger: logging.Logger, step_number: int, step_name: str):
    """
    Registra el inicio de un paso del pipeline
    
    Args:
        logger: Logger a usar
        step_number: Número del paso
        step_name: Nombre descriptivo del paso
    """
    logger.info("")
    logger.info(f"{'='*70}")
    logger.info(f"PASO {step_number}: {step_name}")
    logger.info(f"{'='*70}")
