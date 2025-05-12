"""
Parquet Importer Module

This module provides functionality for importing Parquet files to PostgreSQL.
"""

import logging
import io
import pandas as pd
import pyarrow.parquet as pq

from .base import PipelineComponent

logger = logging.getLogger(__name__)

class ParquetImporter(PipelineComponent):
    """Imports Parquet files to PostgreSQL"""
    
    def __init__(self, s3_client, db_client):
        """
        Initialize the Parquet importer
        
        Args:
            s3_client (S3Client): S3 client
            db_client (DBClient): Database client
        """
        super().__init__("ParquetImporter")
        self.s3_client = s3_client
        self.db_client = db_client
    
    def process_entity(self, entity_name, entity_data=None, **kwargs):
        """
        Import Parquet files for an entity
        
        Args:
            entity_name (str): Entity name
            entity_data (dict, optional): Entity data from previous steps
            **kwargs: Additional arguments (entity_folder)
            
        Returns:
            dict: Processing results
        """
        entity_folder = kwargs.get('entity_folder')
        if not entity_folder:
            return {
                'success': False,
                'message': "No entity folder provided"
            }
        
        try:
            # Find parquet files
            parquet_files = self.s3_client.get_parquet_files(entity_folder)
            
            if not parquet_files:
                return {
                    'success': False,
                    'message': f"No parquet files found in {entity_folder}"
                }
                
            logger.info(f"Found {len(parquet_files)} parquet files for {entity_name}")
            
            # Process each parquet file
            for file_key in parquet_files:
                logger.info(f"Processing file: {file_key}")
                
                # Download parquet file
                parquet_bytes = self.s3_client.download_file(file_key)
                
                # Convert to DataFrame
                buffer = io.BytesIO(parquet_bytes)
                table = pq.read_table(buffer)
                df = table.to_pandas()
                logger.info(f"DataFrame shape: {df.shape}")
                
                # Process DataFrame
                df = self._preprocess_dataframe(df)
                
                # Create table schema
                create_table_sql = self._generate_create_table_sql(entity_name, df)
                
                # Create table
                conn = self.db_client.get_connection()
                cursor = conn.cursor()
                
                try:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    logger.info(f"Table {entity_name} created successfully")
                    
                    # Insert data
                    self._insert_data(entity_name, df)
                    
                finally:
                    cursor.close()
                    conn.close()
            
            # Return success
            return {
                'success': True,
                'message': f"Imported {len(parquet_files)} parquet files to {entity_name}"
            }
            
        except Exception as e:
            logger.error(f"Error importing parquet files: {str(e)}")
            return {
                'success': False,
                'message': f"Error: {str(e)}"
            }
    
    def _preprocess_dataframe(self, df):
        """
        Preprocess a DataFrame before importing
        
        Args:
            df (pandas.DataFrame): DataFrame to preprocess
            
        Returns:
            pandas.DataFrame: Preprocessed DataFrame
        """
        # Process list columns
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: [] if not isinstance(x, list) else x)
                
        # Handle large IDs
        if 'id' in df.columns:
            df['id'] = df['id'].astype(str)
            
        if 'parent_id' in df.columns:
            df['parent_id'] = df['parent_id'].fillna('').astype(str)
            df['parent_id'] = df['parent_id'].replace('', None)
        
        return df
    
    def _generate_create_table_sql(self, table_name, df):
        """
        Generate CREATE TABLE SQL for a DataFrame
        
        Args:
            table_name (str): Table name
            df (pandas.DataFrame): DataFrame
            
        Returns:
            str: CREATE TABLE SQL
        """
        columns = []
        for col, dtype in df.dtypes.items():
            if col == 'id':
                columns.append(f"{col} NUMERIC(38,0) PRIMARY KEY")
            elif 'int' in str(dtype):
                columns.append(f"{col} BIGINT")
            elif 'float' in str(dtype):
                columns.append(f"{col} DOUBLE PRECISION")
            elif dtype == 'bool':
                columns.append(f"{col} BOOLEAN")
            elif 'datetime' in str(dtype):
                columns.append(f"{col} TIMESTAMP")
            elif df[col].apply(lambda x: isinstance(x, list)).any():
                columns.append(f"{col} TEXT[]")
            else:
                columns.append(f"{col} VARCHAR(1000)")
        
        create_table_sql = f"""
        DROP TABLE IF EXISTS {table_name} CASCADE;
        CREATE TABLE {table_name} (
            {', '.join(columns)}
        );
        """
        
        return create_table_sql
    
    def _insert_data(self, table_name, df):
        """
        Insert DataFrame data into a table
        
        Args:
            table_name (str): Table name
            df (pandas.DataFrame): DataFrame
            
        Returns:
            bool: Success status
        """
        logger.info(f"Inserting {len(df)} rows into {table_name}...")
        
        # Prepare data for insertion
        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]
        
        # Insert data with execute_values for better performance
        result = self.db_client.insert_with_execute_values(table_name, columns, values)
        
        if result:
            # Verify
            conn = self.db_client.get_connection()
            cursor = conn.cursor()
            
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                logger.info(f"Inserted {count} rows into {table_name}")
                return True
            finally:
                cursor.close()
                conn.close()
        
        return False