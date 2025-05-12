"""
Full-Text Search Manager Module

This module provides functionality for managing full-text search in PostgreSQL.
"""

import logging

logger = logging.getLogger(__name__)

class FTSManager:
    """Manages full-text search operations in PostgreSQL"""
    
    def __init__(self, db_client):
        """
        Initialize the FTS manager
        
        Args:
            db_client (DBClient): Database client
        """
        self.db_client = db_client
    
    def create_fts_table(self, entity_name):
        """
        Create the full-text search table for an entity
        
        Args:
            entity_name (str): Entity name
            
        Returns:
            dict: FTS table configuration
        """
        conn = self.db_client.get_connection()
        cursor = conn.cursor()
        
        try:
            logger.info(f"Creating FTS table for {entity_name}")
            
            fts_table = f"{entity_name}_fts"
            
            # Create the FTS table
            drop_table_sql = f"DROP TABLE IF EXISTS {fts_table};"
            create_table_sql = f"""
            CREATE TABLE {fts_table} (
                id NUMERIC(38,0) PRIMARY KEY REFERENCES {entity_name}(id),
                tsv tsvector
            );
            """
            
            cursor.execute(drop_table_sql)
            cursor.execute(create_table_sql)
            
            # Create GIN index for fast text search
            create_index_sql = f"""
            CREATE INDEX idx_{fts_table}_tsv ON {fts_table} USING GIN(tsv);
            """
            
            cursor.execute(create_index_sql)
            conn.commit()
            
            logger.info(f"Created {fts_table} table and GIN index")
            
            return {
                'table': fts_table
            }
        finally:
            cursor.close()
            conn.close()
    
    def generate_fts_vectors(self, entity_name, fts_config):
        """
        Generate full-text search vectors for an entity
        
        Args:
            entity_name (str): Entity name
            fts_config (dict): FTS table configuration
            
        Returns:
            bool: Success status
        """
        # Get text columns
        text_columns = self.db_client.get_text_columns(entity_name)
        
        if not text_columns:
            logger.warning(f"No text columns found for {entity_name}")
            return False
        
        # Generate tsvectors
        fts_table = fts_config['table']
        
        # Construct concatenation of text columns
        concat_expr = " || ' ' || ".join([f"COALESCE({col}, '')" for col in text_columns])
        
        # Insert tsvectors
        insert_query = f"""
        INSERT INTO {fts_table} (id, tsv)
        SELECT id, to_tsvector('english', {concat_expr})
        FROM {entity_name};
        """
        
        try:
            self.db_client.execute_query(insert_query)
            
            # Verify
            count_query = f"SELECT COUNT(*) FROM {fts_table}"
            result = self.db_client.execute_query(count_query, fetchall=False)
            count = result[0] if result else 0
            
            logger.info(f"Generated FTS vectors for {count} rows in {entity_name}")
            return True
        except Exception as e:
            logger.error(f"Error generating FTS vectors: {str(e)}")
            return False
    
    def search(self, entity_name, query, limit=10):
        """
        Search entities using full-text search
        
        Args:
            entity_name (str): Entity name
            query (str): Search query
            limit (int): Maximum number of results
            
        Returns:
            list: Search results
        """
        fts_table = f"{entity_name}_fts"
        
        # Check if FTS table exists
        if not self.db_client.table_exists(fts_table):
            logger.error(f"FTS table {fts_table} does not exist")
            return []
        
        # Construct query
        search_query = f"""
        SELECT e.*, ts_rank(f.tsv, plainto_tsquery('english', %s)) AS rank
        FROM {entity_name} e
        JOIN {fts_table} f ON e.id = f.id
        WHERE f.tsv @@ plainto_tsquery('english', %s)
        ORDER BY rank DESC
        LIMIT %s;
        """
        
        try:
            results = self.db_client.execute_query(search_query, (query, query, limit))
            logger.info(f"Found {len(results)} results for query '{query}'")
            return results
        except Exception as e:
            logger.error(f"Error in FTS search: {str(e)}")
            return []