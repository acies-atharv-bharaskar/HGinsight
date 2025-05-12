"""
Embeddings Manager Module

This module provides functionality for managing embeddings in PostgreSQL.
"""

import logging
import numpy as np
import psycopg2

# Import sentence-transformers for embeddings
try:
    from sentence_transformers import SentenceTransformer
    HAVE_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAVE_SENTENCE_TRANSFORMERS = False
    logging.warning("Warning: sentence-transformers not installed. Embeddings will be simulated.")

logger = logging.getLogger(__name__)

class EmbeddingsManager:
    """Manages embedding operations in PostgreSQL"""
    
    def __init__(self, db_client, model_name='all-MiniLM-L6-v2'):
        """
        Initialize the embeddings manager
        
        Args:
            db_client (DBClient): Database client
            model_name (str): Embedding model name
        """
        self.db_client = db_client
        self.model_name = model_name
        self.model = None
        self.embedding_size = 768  # Default size
        
    def load_model(self):
        """Load the embedding model"""
        if not HAVE_SENTENCE_TRANSFORMERS:
            logger.warning("sentence-transformers not installed, using random embeddings")
            return None
            
        if self.model is None:
            logger.info(f"Loading embedding model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            self.embedding_size = self.model.get_sentence_embedding_dimension()
            logger.info(f"Model loaded with embedding size: {self.embedding_size}")
        
        return self.model
    
    def get_embedding_size(self):
        """Get the embedding size"""
        if HAVE_SENTENCE_TRANSFORMERS and self.model is None:
            self.load_model()
        return self.embedding_size
    
    def generate_embeddings(self, texts):
        """
        Generate embeddings for a list of texts
        
        Args:
            texts (list): List of text strings
            
        Returns:
            list: List of embeddings
        """
        if not texts:
            return []
            
        if HAVE_SENTENCE_TRANSFORMERS:
            model = self.load_model()
            embeddings = model.encode(texts)
            return embeddings
        else:
            # Generate random embeddings as fallback
            return [self.generate_random_embedding(text) for text in texts]
    
    def generate_random_embedding(self, text):
        """
        Generate a random embedding for a text (for testing)
        
        Args:
            text (str): Input text
            
        Returns:
            numpy.ndarray: Random embedding vector
        """
        # Use text hash as seed for reproducibility
        np.random.seed(hash(text) % 2**32)
        # Generate random vector
        embedding = np.random.normal(0, 1, self.embedding_size)
        # Normalize to unit length
        embedding = embedding / np.linalg.norm(embedding)
        return embedding
    
    def create_embeddings_table(self, entity_name):
        """
        Create the embeddings table for an entity
        
        Args:
            entity_name (str): Entity name
            
        Returns:
            dict: Table configuration
        """
        conn = self.db_client.get_connection()
        cursor = conn.cursor()
        
        try:
            logger.info(f"Creating embeddings table for {entity_name}")
            
            embeddings_table = f"{entity_name}_embeddings"
            
            # First, attempt to create pgvector extension if it's not already installed
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                conn.commit()
                logger.info("pgvector extension installed/verified")
                
                # Get embedding dimension
                embedding_size = self.get_embedding_size()
                
                # Create the embeddings table with vector type
                drop_table_sql = f"DROP TABLE IF EXISTS {embeddings_table};"
                create_table_sql = f"""
                CREATE TABLE {embeddings_table} (
                    id NUMERIC(38,0) PRIMARY KEY REFERENCES {entity_name}(id),
                    embedding vector({embedding_size})
                );
                """
                
                cursor.execute(drop_table_sql)
                cursor.execute(create_table_sql)
                conn.commit()
                
                logger.info(f"Created {embeddings_table} table with vector type")
                
                # Return configuration showing we're using pgvector
                return {
                    'table': embeddings_table,
                    'has_pgvector': True,
                    'embedding_size': embedding_size
                }
                
            except Exception as e:
                logger.warning(f"Could not create pgvector table: {str(e)}")
                logger.warning("Falling back to BYTEA storage for embeddings")
                
                # Create a table using BYTEA for storing embeddings
                drop_table_sql = f"DROP TABLE IF EXISTS {embeddings_table};"
                create_table_sql = f"""
                CREATE TABLE {embeddings_table} (
                    id NUMERIC(38,0) PRIMARY KEY REFERENCES {entity_name}(id),
                    embedding BYTEA
                );
                """
                
                cursor.execute(drop_table_sql)
                cursor.execute(create_table_sql)
                conn.commit()
                
                logger.info(f"Created {embeddings_table} table with BYTEA type")
                
                # Return configuration showing we're using BYTEA
                return {
                    'table': embeddings_table,
                    'has_pgvector': False,
                    'embedding_size': self.embedding_size
                }
        finally:
            cursor.close()
            conn.close()
    
    def store_embeddings(self, entity_name, embeddings_config, ids, embeddings, batch_size=32):
        """
        Store embeddings in the database
        
        Args:
            entity_name (str): Entity name
            embeddings_config (dict): Embeddings table configuration
            ids (list): List of entity IDs
            embeddings (list): List of embedding vectors
            batch_size (int): Batch size for inserts
            
        Returns:
            bool: Success status
        """
        if not ids or not embeddings or len(ids) != len(embeddings):
            logger.error("Invalid IDs or embeddings data")
            return False
        
        conn = self.db_client.get_connection()
        cursor = conn.cursor()
        
        try:
            embeddings_table = embeddings_config['table']
            has_pgvector = embeddings_config['has_pgvector']
            
            for i in range(0, len(ids), batch_size):
                batch_ids = ids[i:i+batch_size]
                batch_embeddings = embeddings[i:i+batch_size]
                
                logger.info(f"Storing embeddings batch {i//batch_size + 1}/{len(ids)//batch_size + 1}")
                
                if has_pgvector:
                    # Using pgvector extension
                    for j, embedding in enumerate(batch_embeddings):
                        cursor.execute(
                            f"INSERT INTO {embeddings_table} (id, embedding) VALUES (%s, %s) "
                            f"ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding",
                            (batch_ids[j], embedding.tolist() if hasattr(embedding, 'tolist') else embedding)
                        )
                else:
                    # Using BYTEA storage
                    for j, embedding in enumerate(batch_embeddings):
                        # Convert to binary
                        embedding_array = np.array(embedding, dtype=np.float32)
                        embedding_binary = embedding_array.tobytes()
                        
                        cursor.execute(
                            f"INSERT INTO {embeddings_table} (id, embedding) VALUES (%s, %s) "
                            f"ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding",
                            (batch_ids[j], psycopg2.Binary(embedding_binary))
                        )
            
            conn.commit()
            
            # Create vector similarity index if using pgvector
            if has_pgvector:
                try:
                    cursor.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{embeddings_table}_vector "
                        f"ON {embeddings_table} USING ivfflat (embedding vector_cosine_ops);"
                    )
                    conn.commit()
                    logger.info(f"Created vector similarity index for {embeddings_table}")
                except Exception as e:
                    logger.warning(f"Could not create vector index: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing embeddings: {str(e)}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()