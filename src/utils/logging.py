"""
Logging Utilities Module

This module provides helper classes and functions for configuring logging.
"""

import logging
import logging.config
import os
from datetime import datetime

class LoggingManager:
    """Manages logging setup for the application"""
    
    @staticmethod
    def setup_logging(config=None, log_file=None, level=None, add_timestamp=True):
        """
        Configure logging for the application
        
        Args:
            config (dict, optional): Logging configuration
            log_file (str, optional): Log file path. If None, uses config or default.
            level (str, optional): Logging level. If None, uses config or default.
            add_timestamp (bool): Whether to add timestamp to log filename.
            
        Returns:
            logger: Configured logger
        """
        # Use provided values or fall back to config
        config = config or {}
        level = level or config.get('level', 'INFO')
        log_file = log_file or config.get('file')
        log_format = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Convert string level to logging level
        numeric_level = getattr(logging, level.upper(), logging.INFO)
        
        # Add timestamp to log file if requested
        if add_timestamp and log_file:
            filename, ext = os.path.splitext(log_file)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = f"{filename}_{timestamp}{ext}"
        
        # Configure logging
        log_config = {
            'version': 1,
            'formatters': {
                'standard': {
                    'format': log_format
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': numeric_level,
                    'formatter': 'standard',
                },
            },
            'loggers': {
                '': {  # Root logger
                    'handlers': ['console'],
                    'level': numeric_level,
                    'propagate': True
                }
            }
        }
        
        # Add file handler if log file is specified
        if log_file:
            # Ensure log directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
                
            log_config['handlers']['file'] = {
                'class': 'logging.FileHandler',
                'level': numeric_level,
                'formatter': 'standard',
                'filename': log_file,
            }
            log_config['loggers']['']['handlers'].append('file')
        
        # Apply configuration
        logging.config.dictConfig(log_config)
        
        # Create a logger for the caller
        logger = logging.getLogger('pipeline')
        logger.info(f"Logging initialized at level {level}")
        if log_file:
            logger.info(f"Logging to file: {log_file}")
        
        return logger
    
    @staticmethod
    def get_logger(name):
        """Get a named logger"""
        return logging.getLogger(name)
        
    @staticmethod
    def log_execution_time(func):
        """
        Decorator to log function execution time
        
        Args:
            func: Function to decorate
        """
        import time
        import functools
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            start_time = time.time()
            logger.info(f"Starting {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"Finished {func.__name__} in {execution_time:.2f} seconds")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"Failed {func.__name__} after {execution_time:.2f} seconds: {str(e)}")
                raise
        
        return wrapper
    
    @staticmethod
    def log_step(description):
        """
        Decorator to log pipeline step with description
        
        Args:
            description (str): Step description
        """
        def decorator(func):
            import functools
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                logger = logging.getLogger(func.__module__)
                logger.info(f"STEP: {description}")
                return func(*args, **kwargs)
            
            return wrapper
        
        return decorator