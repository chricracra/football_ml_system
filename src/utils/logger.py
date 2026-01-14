# src/utils/logger.py
import logging
import sys
from pathlib import Path

def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Configura e restituisce un logger.
    """
    # Crea il logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Evita di aggiungere handler multipli
    if not logger.handlers:
        # Formattatore per i messaggi di log
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Handler per output a console (stderr)
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # (Opzionale) Aggiungi qui un handler per scrivere su file
        # file_handler = logging.FileHandler('logs/app.log')
        # file_handler.setFormatter(formatter)
        # logger.addHandler(file_handler)

    return logger
