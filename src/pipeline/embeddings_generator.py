"""
Embeddings Generator Module

This module provides functionality for generating embeddings for entities.
"""

import logging
from .base import PipelineComponent

logger = logging.getLogger(__name__)

class EmbeddingsGenerator(PipelineComponent):
    """Generates embeddings for entities"""
    
    def __init__(self, db_client, embeddings_manager):
        """
        Initialize the embeddings generator
        
        Args:
            db_client (DBClient): Database client
            embeddings_manager (EmbeddingsManager): Embeddings manager
        """
        super().__init__("EmbeddingsGenerator")
        self.db_client = db_client
        self.embeddings_manager = embeddings_manager
    
    def process_entity(self, entity_name, entity_data=None, **kwargs):
        """
        Generate embeddings for an entity
        
        Args:
            entity_name (str): Entity name
            entity_data (dict, optional): Entity data from previous steps
            **kwargs: Additional arguments
            
        Returns:
            dict: Processing results with embeddings configuration
        """
        try:
            # Create embeddings table
            embeddings_config = self.embeddings_manager.create_embeddings_table(entity_name)
            
            # Get text columns
            text_columns = self.db_client.get_text_columns(entity_name)
            
            if not text_columns:
                return {
                    'success': False,
                    'message': f"No text columns found for {entity_name}"
                }
            
            # Get the data
            columns_str = ', '.join(['id'] + text_columns)
            query = f"""
            SELECT {columns_str}
            FROM {entity_name};
            """
            
            rows = self.db_client.execute_query(query)
            
            if not rows:
                return {
                    'success': False,
                    'message': f"No data found in {entity_name}"
                }
            
            logger.info(f"Generating embeddings for {len(rows)} rows in {entity_name}")
            
            # Prepare text data
            texts = []
            ids = []
            for row in rows:
                id_val = row[0]
                # Combine text from all columns
                text_parts = []
                for i, col in enumerate(text_columns):
                    val = row[i+1]
                    if val is not None:
                        text_parts.append(str(val))
                
                combined_text = ' '.join(text_parts).strip()
                if combined_text:
                    texts.append(combined_text)
                    ids.append(id_val)
            
            # Generate embeddings
            batch_size = 32
            all_embeddings = []
            
            # Process in batches to avoid memory issues
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i:i+batch_size]
                batch_embeddings = self.embeddings_manager.generate_embeddings(batch_texts)
                all_embeddings.extend(batch_embeddings)
            
            # Store embeddings
            success = self.embeddings_manager.store_embeddings(
                entity_name, 
                embeddings_config, 
                ids, 
                all_embeddings
            )
            
            if success:
                return {
                    'success': True,
                    'message': f"Generated embeddings for {len(ids)} rows in {entity_name}",
                    'embeddings_config': embeddings_config
                }
            else:
                return {
                    'success': False,
                    'message': f"Failed to store embeddings for {entity_name}"
                }
                
        except Exception as e:
            logger.error(f"Error generating embeddings: {str(e)}")
            return {
                'success': False,
                'message': f"Error: {str(e)}"
            }