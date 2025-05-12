"""
Base Pipeline Component Module

This module provides the base class for pipeline components.
"""

import logging
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class PipelineComponent(ABC):
    """Abstract base class for pipeline components"""
    
    def __init__(self, name):
        """
        Initialize the pipeline component
        
        Args:
            name (str): Component name
        """
        self.name = name
    
    def process(self, entity_name, entity_data=None, **kwargs):
        """
        Process an entity
        
        Args:
            entity_name (str): Entity name
            entity_data (dict, optional): Entity data from previous steps
            **kwargs: Additional arguments
            
        Returns:
            dict: Processing results
        """
        logger.info(f"Processing entity '{entity_name}' with component '{self.name}'")
        
        start_time = time.time()
        result = {
            'component': self.name,
            'entity': entity_name,
            'success': False,
            'time': 0,
            'message': ''
        }
        
        try:
            # Call the implemented process method
            process_result = self.process_entity(entity_name, entity_data, **kwargs)
            
            # Update result
            result.update(process_result)
            
            # Calculate time
            execution_time = time.time() - start_time
            result['time'] = f"{execution_time:.2f}s"
            
            if result.get('success', False):
                logger.info(f"Component '{self.name}' processed '{entity_name}' successfully in {result['time']}")
            else:
                logger.warning(f"Component '{self.name}' failed to process '{entity_name}' in {result['time']}")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in component '{self.name}' processing '{entity_name}': {str(e)}")
            
            # Update result
            result.update({
                'success': False,
                'time': f"{execution_time:.2f}s",
                'message': f"Error: {str(e)}"
            })
            
            return result
    
    @abstractmethod
    def process_entity(self, entity_name, entity_data=None, **kwargs):
        """
        Process an entity (to be implemented by subclasses)
        
        Args:
            entity_name (str): Entity name
            entity_data (dict, optional): Entity data from previous steps
            **kwargs: Additional arguments
            
        Returns:
            dict: Processing results
        """
        pass