# S3 to PostgreSQL Pipeline - Code Explanation

## Overview

This project is a modular data pipeline that does three main things:
1. Imports Parquet files from AWS S3 into PostgreSQL
2. Generates embeddings (vector representations) for text data
3. Creates full-text search indices for better text searching

The code is designed to be modular, which means each part has a specific job and can be used independently.

## Directory Structure Explained

- **scripts/** - Contains command-line tools to run the pipeline
- **src/** - Main source code, organized into modules
  - **config/** - Handles loading settings from files or environment variables
  - **db/** - Database operations (PostgreSQL)
  - **pipeline/** - The actual pipeline components
  - **s3/** - AWS S3 operations
  - **utils/** - Helper utilities like logging
- **setup.py** - Used to install the package
- **README.md** - Documentation

## File-by-File Explanation

### Root Files

- **setup.py**: Defines package dependencies and installation information. Lists requirements like boto3, pandas, pyarrow.
- **README.md**: Documentation with installation and usage instructions.

### Scripts Directory

- **run_pipeline.py**: The main script you run to start the pipeline. It processes command-line arguments, sets up the pipeline components, and executes them.
- **test_pipeline.py**: Tests connections to S3 and PostgreSQL to make sure they're working before running the full pipeline.

### Source Code (src/)

#### Main Package

- **__init__.py**: Marks the directory as a Python package. Contains version information.

#### Config Module (src/config/)

- **__init__.py**: Exports the ConfigLoader for use in other modules.
- **config_loader.py**: Loads configuration from different sources (JSON files, .env files, environment variables). Has defaults for all settings.

#### Database Module (src/db/)

- **__init__.py**: Exports the database classes (DBClient, EmbeddingsManager, FTSManager).
- **db_client.py**: Handles database connections and queries. Contains retry logic and error handling.
- **embeddings.py**: Manages creation of embedding tables and storing embeddings in the database. Uses sentence-transformers models.
- **fts.py**: Manages full-text search operations. Creates tsvector indices for PostgreSQL text search.

#### Pipeline Module (src/pipeline/)

- **__init__.py**: Exports all pipeline components.
- **base.py**: Base class for pipeline components. Provides common functionality like timing and error handling.
- **importer.py**: Imports Parquet files from S3 into PostgreSQL tables.
- **embeddings_generator.py**: Generates embeddings for text columns in imported tables.
- **fts_generator.py**: Creates full-text search indices for text columns.
- **pipeline.py**: Main orchestrator that runs all components in sequence.

#### S3 Module (src/s3/)

- **__init__.py**: Exports the S3Client class.
- **s3_client.py**: Handles S3 operations with retry logic. Finds date folders, entity folders, and downloads Parquet files.

#### Utilities Module (src/utils/)

- **__init__.py**: Exports utility classes like LoggingManager.
- **logging.py**: Sets up consistent logging throughout the application.

## The Pipeline Flow

1. **Configuration**: The pipeline loads settings from config files and environment variables
2. **S3 Discovery**: It finds the latest date folder and entity folders in S3
3. **Importing Data**: For each entity (table), it downloads Parquet files and imports them into PostgreSQL
4. **Embeddings Generation**: For text columns, it generates embeddings using sentence-transformers
5. **Full-Text Search**: It creates tsvector indices for better text searching

## Common Questions and Answers

### General Questions

**Q: What does this pipeline do?**
A: It imports data from Parquet files in S3 into PostgreSQL, then enhances the data with embeddings for AI/ML and full-text search for better searching.

**Q: Why is it modular?**
A: Modularity makes the code easier to maintain, test, and extend. Each component has a specific job and can be used independently.

**Q: What are the main components?**
A: The main components are the ParquetImporter, EmbeddingsGenerator, and FTSGenerator, which are orchestrated by the Pipeline class.

### Technical Questions

**Q: How does it handle configurations?**
A: Through the ConfigLoader class, which checks config files (JSON, .env) first, then environment variables, and falls back to defaults if needed.

**Q: What databases does it support?**
A: Currently, it's designed for PostgreSQL, with specific support for the pgvector extension for vector similarity search.

**Q: How does it generate embeddings?**
A: It uses the sentence-transformers library with the "all-MiniLM-L6-v2" model by default, but this can be configured.

**Q: How does it handle errors?**
A: Each component has error handling, and the pipeline tracks success/failure for each entity and stage. S3 operations have retry logic.

**Q: What are the system requirements?**
A: Python 3.8+, PostgreSQL 12+, and optionally the pgvector extension for vector similarity search.

### Usage Questions

**Q: How do I run the pipeline?**
A: Run `python scripts/run_pipeline.py` with optional arguments like `--entity product` to process specific entities.

**Q: How do I configure it?**
A: Create a config.json file, set environment variables, or use command-line arguments when running the pipeline.

**Q: How can I see what's happening?**
A: The pipeline creates detailed logs (by default in pipeline_YYYYMMDD.log) and prints a summary to the console.

**Q: How do I know if it worked?**
A: Check the console output for success/failure messages, or examine the log file for details.

### Customization Questions

**Q: How do I add a new pipeline component?**
A: Create a new class that inherits from PipelineComponent, implement the process_entity method, and add it to the pipeline.

**Q: Can I skip certain steps?**
A: Yes, you can add only the components you want when setting up the pipeline, or use command-line flags like `--skip-embeddings`.

**Q: How do I change the embedding model?**
A: Set the 'embedding.model' in your config file or the EMBEDDING_MODEL environment variable.

**Q: Can it work with other cloud storage services?**
A: Currently, it's designed for AWS S3, but you could create a similar client for other services by following the S3Client pattern.

## Key Features

- **Automatic Discovery**: Finds the latest data in S3 automatically
- **Error Handling**: Robust error handling with detailed logs
- **Modular Design**: Components can be used independently
- **Configuration Flexibility**: Multiple ways to configure (files, environment variables, command-line)
- **Performance Optimizations**: Batch processing, connection pooling (in some components)
- **Extensibility**: Easy to add new components or modify existing ones