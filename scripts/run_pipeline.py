#!/usr/bin/env python
"""
Pipeline runner script

Run this script to execute the S3 to PostgreSQL pipeline
with command-line arguments.
"""

import argparse
import json
import os
import sys
import logging
from datetime import datetime

# Add the parent directory to the path so we can import packages
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Configure imports - local modules
from src.config import ConfigLoader
from src.utils import LoggingManager
from src.s3 import S3Client
from src.db import DBClient, EmbeddingsManager, FTSManager
from src.pipeline import (
    Pipeline, 
    ParquetImporter, 
    EmbeddingsGenerator, 
    FTSGenerator
)

# Version tracking
__version__ = "0.2.0"

# Runtime flags
DEBUG = os.environ.get("DEBUG", "").lower() in ("true", "1", "yes")

def parse_args():
    """Parse command line args"""
    parser = argparse.ArgumentParser(
        description='S3 to PostgreSQL Pipeline',
        epilog='Example: run_pipeline.py --entity product --output results.json'
    )
    
    parser.add_argument('--date-folder', '-d', 
                        help='S3 date folder to process (default: latest)')
    
    parser.add_argument('--bucket', '-b',
                        help='S3 bucket name (overrides config)')
    
    parser.add_argument('--entity', '-e',
                        help='Only process this entity')
    
    parser.add_argument('--log-file', '-l',
                        help='Log file path (default: pipeline_YYYYMMDD.log)')
    
    parser.add_argument('--output', '-o',
                        help='Output file for results JSON')
    
    parser.add_argument('--config-file', '-c',
                        help='Config file path')
    
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be processed without running')
    
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    
    parser.add_argument('--skip-embeddings', action='store_true',
                        help='Skip embeddings generation step')
    
    parser.add_argument('--version', '-v', action='store_true',
                        help='Show version and exit')
    
    return parser.parse_args()

def setup_pipeline(config_loader, logger, args):
    """Setup the pipeline components"""
    # Load configs
    s3_config = config_loader.get_s3_config()
    db_config = config_loader.get_db_config()
    embedding_config = config_loader.get_embedding_config()
    
    # Override bucket if provided
    if args.bucket:
        s3_config['bucket'] = args.bucket
        logger.info(f"Using override bucket: {args.bucket}")
    
    # Create clients
    logger.info("Initializing clients...")
    s3_client = S3Client(s3_config)
    db_client = DBClient(db_config)
    
    # Create pipeline & components
    logger.info("Setting up pipeline components...")
    pipeline = Pipeline(s3_client)
    
    # Always add the importer
    pipeline.add_component(ParquetImporter(s3_client, db_client))
    
    # Add embeddings generator unless skipped
    if not args.skip_embeddings:
        embeddings_manager = EmbeddingsManager(
            db_client, 
            embedding_config.get('model')
        )
        pipeline.add_component(EmbeddingsGenerator(db_client, embeddings_manager))
    else:
        logger.info("Skipping embeddings generation")
        
    # Always add FTS generator
    fts_manager = FTSManager(db_client)
    pipeline.add_component(FTSGenerator(db_client, fts_manager))
    
    return pipeline

def main():
    """Main entry point"""
    # Handle args
    args = parse_args()
    
    # Show version and exit if requested
    if args.version:
        print(f"S3 to PostgreSQL Pipeline v{__version__}")
        return 0
    
    # Enable debug mode if flag is set
    global DEBUG
    if args.debug:
        DEBUG = True
    
    # Set default log file if not provided
    log_file = args.log_file
    if not log_file:
        today = datetime.now().strftime("%Y%m%d")
        log_file = f"pipeline_{today}.log"
    
    # Load configuration
    try:
        config_loader = ConfigLoader(args.config_file)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return 1
    
    # Setup logging with appropriate level
    try:
        log_level = "DEBUG" if DEBUG else "INFO"
        logger = LoggingManager.setup_logging(
            config={'level': log_level, 'file': log_file},
            log_file=log_file
        )
    except Exception as e:
        print(f"Error setting up logging: {e}")
        return 1
    
    logger.info(f"Pipeline v{__version__} starting up")
    
    # Handle dry run mode
    if args.dry_run:
        logger.info("DRY RUN MODE - only showing what would be processed")
        
        try:
            s3_client = S3Client(config_loader.get_s3_config())
            date_folder = args.date_folder or s3_client.get_latest_date_folder()
            
            if not date_folder:
                logger.error("No date folder found")
                return 1
                
            logger.info(f"Would process date folder: {date_folder}")
            
            entity_folders = s3_client.get_entity_folders(date_folder)
            if not entity_folders:
                logger.error(f"No entity folders found in {date_folder}")
                return 1
                
            logger.info("Would process the following entities:")
            for entity_name, folder_path in entity_folders:
                if args.entity and entity_name != args.entity:
                    logger.info(f"  - {entity_name}: SKIPPED (not selected)")
                else:
                    logger.info(f"  - {entity_name}: WOULD PROCESS")
                    
                    # Get parquet files
                    parquet_files = s3_client.get_parquet_files(folder_path)
                    logger.info(f"    - Found {len(parquet_files)} parquet files")
            
            logger.info("Dry run complete")
            return 0
        except Exception as e:
            logger.error(f"Error in dry run: {e}")
            return 1
    
    # Setup pipeline
    try:
        pipeline = setup_pipeline(config_loader, logger, args)
    except Exception as e:
        logger.error(f"Failed to setup pipeline: {e}")
        return 1
    
    # Run the pipeline
    try:
        logger.info("Starting pipeline execution")
        results = pipeline.run(
            date_folder=args.date_folder,
            entity_filter=args.entity
        )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return 1
    
    # Output results
    if args.output:
        try:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Results written to {args.output}")
        except Exception as e:
            logger.error(f"Failed to write results to {args.output}: {e}")
    
    # Print summary to console
    print("\n" + "="*60)
    print(" PIPELINE EXECUTION SUMMARY ".center(60, "="))
    print("="*60)
    print(f"Date folder: {results.get('date_folder', 'Unknown')}")
    print(f"Status    : {'✅ SUCCESS' if results.get('success') else '❌ FAILURE'}")
    print(f"Run time  : {results.get('total_time', 'Unknown')}")
    print(f"Message   : {results.get('message', '')}")
    print("-"*60)
    
    # Entity results
    if 'entities' in results and results['entities']:
        print("\nEntity Results:")
        
        entities = results['entities']
        success_count = sum(1 for e in entities if e.get('success', False))
        print(f"Processed {len(entities)} entities ({success_count} successful, {len(entities) - success_count} failed)")
        
        for entity in entities:
            entity_name = entity.get('entity', 'Unknown')
            success = entity.get('success', False)
            time_taken = entity.get('total_time', 'Unknown')
            
            # Show entity status
            print(f"\n  {entity_name}: {'✅' if success else '❌'} ({time_taken})")
            
            # Show stages if we have them
            if 'stages' in entity:
                for stage_name, stage_details in entity['stages'].items():
                    stage_success = stage_details.get('success', False)
                    stage_time = stage_details.get('time', 'unknown')
                    stage_msg = stage_details.get('message', '')
                    
                    status = "✅" if stage_success else "❌"
                    print(f"    - {stage_name.ljust(20)}: {status} {stage_time.ljust(10)} {stage_msg}")
    else:
        print("\nNo entities were processed")
    
    print("\n" + "="*60)
    
    # Return appropriate exit code
    return 0 if results.get('success', False) else 1

if __name__ == "__main__":
    # Catch Ctrl+C gracefully
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(130)