"""
Full-Text Search Generator Module

This module provides functionality for generating full-text search vectors.
"""

import logging
from .base import PipelineComponent

logger = logging.getLogger(__name__)

class FTSGenerator(PipelineComponent):
    """Generates full-text search vectors for entities"""
    
    def __init__(self, db_client, fts_manager):
        """
        Initialize the FTS generator
        
        Args:
            db_client (DBClient): Database client
            fts_manager (FTSManager): FTS manager
        """
        super().__init__("FTSGenerator")
        self.db_client = db_client
        self.fts_manager = fts_manager
    
    def process_entity(self, entity_name, entity_data=None, **kwargs):
        """
        Generate FTS vectors for an entity
        
        Args:
            entity_name (str): Entity name
            entity_data (dict, optional): Entity data from previous steps
            **kwargs: Additional arguments
            
        Returns:
            dict: Processing results with FTS configuration
        """
        try:
            # Create FTS table
            fts_config = self.fts_manager.create_fts_table(entity_name)
            
            # Generate FTS vectors
            success = self.fts_manager.generate_fts_vectors(entity_name, fts_config)
            
            if success:
                return {
                    'success': True,
                    'message': f"Generated FTS vectors for {entity_name}",
                    'fts_config': fts_config
                }
            else:
                return {
                    'success': False,
                    'message': f"Failed to generate FTS vectors for {entity_name}"
                }
                
        except Exception as e:
            logger.error(f"Error generating FTS vectors: {str(e)}")
            return {
                'success': False,
                'message': f"Error: {str(e)}"
            }