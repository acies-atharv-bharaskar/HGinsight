"""
Database client module for PostgreSQL operations.
"""
import psycopg2
import psycopg2.extras
import logging
import urllib.parse
from sqlalchemy import create_engine
import traceback

logger = logging.getLogger(__name__)

class DBClient:
    """Handles database connections and operations"""
    
    def __init__(self, config):
        """
        Initialize with database config
        """
        self.config = config
        # Try connecting once to validate config
        try:
            conn = self.get_connection()
            conn.close()
            logger.debug("Initial DB connection successful")
        except Exception as e:
            # Just log the error, don't raise - let later operations handle connection issues
            logger.warning(f"Initial DB connection failed: {e}")
    
    def get_connection(self):
        """Gets a new database connection"""
        # TODO: Add connection pooling in the future
        try:
            conn = psycopg2.connect(
                host=self.config.get('host', 'localhost'),
                database=self.config.get('database', 'postgres'),
                user=self.config.get('user', 'postgres'),
                password=self.config.get('password', 'Acies@123'),  # FIXME: Don't hardcode default password
                port=self.config.get('port', '5432')
            )
            return conn
        except Exception as e:
            logger.error(f"DB connection error: {e}")
            # Include stack trace for connection errors - helpful for debugging
            logger.debug(traceback.format_exc())
            raise
    
    # Quick helper to get SQL Alchemy engine for pandas operations
    def get_sqlalchemy_engine(self):
        pw = self.config.get('password', 'Acies@123')
        pw_encoded = urllib.parse.quote_plus(pw)
        user = self.config.get('user', 'postgres')
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', '5432')
        db = self.config.get('database', 'postgres')
        
        conn_str = f"postgresql://{user}:{pw_encoded}@{host}:{port}/{db}"
        
        try:
            return create_engine(conn_str)
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {e}")
            raise
    
    def execute_query(self, query, params=None, fetchall=True):
        """Run a query and get results"""
        conn = None
        cursor = None
        
        # Simple check if query is SELECT or not
        is_select = query.strip().upper().startswith(('SELECT', 'SHOW', 'WITH'))
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(query, params)
            
            if is_select:
                if fetchall:
                    return cursor.fetchall()
                else:
                    return cursor.fetchone()
            else:
                conn.commit()
                return True
        except Exception as e:
            if conn:
                conn.rollback()
            # Added query text for easier debugging
            logger.error(f"Query failed: {e}")
            logger.error(f"Query was: {query}")
            if params:
                logger.error(f"Params: {params}")
            raise
        finally:
            # Always clean up resources
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def table_exists(self, table_name):
        """Check if table exists"""
        # Quick check using information_schema
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
        """
        result = self.execute_query(query, (table_name,), fetchall=False)
        return result[0] if result else False
    
    def get_table_columns(self, table_name):
        """Get columns and their types"""
        query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        return self.execute_query(query, (table_name,))
    
    def has_pgvector_extension(self):
        """See if pgvector is installed"""
        query = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector');"
        result = self.execute_query(query, fetchall=False)
        return result[0] if result else False
    
    def get_text_columns(self, entity_name):
        """Find text columns we can use for search/embeddings"""
        # Get all text columns first
        query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s 
        AND data_type IN ('character varying', 'text')
        ORDER BY ordinal_position;
        """
        result = self.execute_query(query, (entity_name,))
        
        all_columns = [row[0] for row in result]
        if not all_columns:
            logger.warning(f"No text columns found in {entity_name}")
            return []
            
        # Look for useful columns first
        good_columns = []
        for col in ['name', 'description', 'title', 'summary', 'content']:
            if col in all_columns:
                good_columns.append(col)
        
        # If we don't find any good columns, just use the first few
        # Limiting to 3 to avoid making embeddings too complex
        if not good_columns:
            good_columns = all_columns[:3]
            logger.info(f"No standard text columns found, using: {good_columns}")
        else:
            logger.info(f"Using text columns: {good_columns}")
            
        return good_columns
    
    def insert_with_execute_values(self, table, columns, values, page_size=100):
        """
        Batch insert using psycopg2.extras.execute_values
        Much faster than individual INSERTs
        """
        conn = None
        cursor = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Build the INSERT query
            cols_str = ', '.join(columns)
            insert_sql = f"INSERT INTO {table} ({cols_str}) VALUES %s"
            
            # Do the insert with batching for better performance
            psycopg2.extras.execute_values(
                cursor, 
                insert_sql, 
                values, 
                page_size=page_size
            )
            conn.commit()
            
            # Check if we actually inserted anything
            if cursor.rowcount == 0:
                logger.warning(f"No rows inserted into {table}")
                return False
                
            logger.info(f"Inserted {cursor.rowcount} rows into {table}")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Insert failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    # Shortcut for count query
    def count_rows(self, table):
        """Quick row count for a table"""
        try:
            result = self.execute_query(f"SELECT COUNT(*) FROM {table}", fetchall=False)
            return result[0] if result else 0
        except Exception:
            return 0