#!/usr/bin/env python
"""
Test script to verify S3 and PostgreSQL connections
"""

import sys
import os
import argparse

# Add the parent directory to the path so we can import the package
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from src.config import ConfigLoader
from src.utils import LoggingManager
from src.s3 import S3Client
from src.db import DBClient

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Test S3 and PostgreSQL connections')
    
    parser.add_argument('--log-file', '-l',
                      help='Log file path. If not provided, logs to console only.')
    
    parser.add_argument('--config-file', '-c',
                      help='Path to configuration file. If not provided, looks in standard locations.')
    
    return parser.parse_args()

def test_s3_connection(s3_client, logger):
    """Test connection to S3 and check bucket structure"""
    logger.info("Testing S3 connection...")
    
    try:
        # List buckets
        bucket_names = s3_client.list_buckets()
        logger.info(f"Available buckets: {bucket_names}")
        
        # Test date folder retrieval
        latest_folder = s3_client.get_latest_date_folder()
        if latest_folder:
            logger.info(f"Latest date folder: {latest_folder}")
            
            # Test entity folder retrieval
            entity_folders = s3_client.get_entity_folders(latest_folder)
            if entity_folders:
                logger.info(f"Entity folders found: {[e[0] for e in entity_folders]}")
                return True
            else:
                logger.error("No entity folders found")
                return False
        else:
            logger.error("No date folders found")
            return False
    
    except Exception as e:
        logger.error(f"S3 connection error: {str(e)}")
        return False

def test_db_connection(db_client, logger):
    """Test connection to PostgreSQL"""
    logger.info("Testing PostgreSQL connection...")
    
    try:
        # Test simple query
        result = db_client.execute_query("SELECT version();", fetchall=False)
        version = result[0]
        logger.info(f"PostgreSQL version: {version}")
        
        # Test table existence
        test_tables = ['spend_category', 'product']
        for table in test_tables:
            exists = db_client.table_exists(table)
            if exists:
                logger.info(f"Table '{table}' exists")
                
                # Get row count
                result = db_client.execute_query(f"SELECT COUNT(*) FROM {table};", fetchall=False)
                row_count = result[0] if result else 0
                logger.info(f"Table '{table}' has {row_count} rows")
            else:
                logger.info(f"Table '{table}' does not exist (will be created by pipeline)")
        
        # Test pgvector extension
        has_pgvector = db_client.has_pgvector_extension()
        if has_pgvector:
            logger.info("pgvector extension is installed")
        else:
            logger.warning("pgvector extension is not installed (needed for vector similarity search)")
        
        return True
        
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return False

def main():
    """Main function"""
    # Parse command line arguments
    args = parse_args()
    
    # Load configuration
    config_loader = ConfigLoader(args.config_file)
    
    # Setup logging
    logger = LoggingManager.setup_logging(
        config=config_loader.get_logging_config(),
        log_file=args.log_file
    )
    
    # Create clients
    s3_client = S3Client(config_loader.get_s3_config())
    db_client = DBClient(config_loader.get_db_config())
    
    # Run tests
    logger.info("Starting connection tests...")
    
    s3_success = test_s3_connection(s3_client, logger)
    db_success = test_db_connection(db_client, logger)
    
    if s3_success and db_success:
        logger.info("All connection tests passed successfully")
        print("\n✅ All connection tests passed successfully")
        sys.exit(0)
    else:
        logger.error("Connection tests failed")
        print("\n❌ Connection tests failed. Check the log for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()