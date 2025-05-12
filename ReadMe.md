# S3 to PostgreSQL Pipeline

A data pipeline for importing Parquet files from AWS S3 to PostgreSQL with text embeddings and full-text search capabilities.

## Features

- **Automated Data Import**: Import Parquet files from S3 to PostgreSQL tables
- **Smart Discovery**: Automatically finds the latest date folders and entities
- **Text Embeddings**: Generate vector embeddings for text data using sentence-transformers
- **Full-Text Search**: Create PostgreSQL full-text search indices for better text querying
- **Modular Design**: Easily extend or modify individual components
- **Robust Error Handling**: Includes retries and detailed logging
- **Flexible Configuration**: Configure via files, environment variables, or command line

## Requirements

- Python 3.8+
- PostgreSQL 12+
- AWS S3 access
- Optional: pgvector extension for PostgreSQL (for vector similarity search)

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/acies-atharv-bharaskar/HGinsight.git
cd HGinsight

# Install the package
pip install -e .
```

## Configuration

The pipeline can be configured in multiple ways:

### 1. Configuration File

Create a `config.json` file in one of these locations:
- The current directory
- A `config/` subdirectory
- Your home directory at `~/.s3-postgres-pipeline/config.json`

Example `config.json`:

```json
{
  "s3": {
    "bucket": "hg-dpi-prod-ch-dataload1",
    "region": "eu-north-1"
  },
  "database": {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "your-password",
    "port": "5432"
  },
  "logging": {
    "level": "INFO",
    "file": "pipeline.log"
  },
  "embedding": {
    "model": "all-MiniLM-L6-v2"
  }
}
```

### 2. Environment Variables

```bash
# S3 Configuration
export S3_BUCKET=hg-dpi-prod-ch-dataload1
export AWS_REGION=eu-north-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Database Configuration
export DB_HOST=localhost
export DB_NAME=postgres
export DB_USER=postgres
export DB_PASSWORD=your-password
export DB_PORT=5432

# Logging Configuration
export LOG_LEVEL=INFO
export LOG_FILE=pipeline.log

# Embedding Configuration
export EMBEDDING_MODEL=all-MiniLM-L6-v2
```

### 3. Command Line Arguments

Some settings can be overridden via command line arguments:

```bash
run-pipeline --bucket custom-bucket --entity product
```

## Usage

### Basic Usage

```bash
# Run the pipeline with default settings
run-pipeline

# Or directly with Python
python scripts/run_pipeline.py
```

### Processing Specific Data

```bash
# Process a specific date folder
run-pipeline --date-folder 2025-04-14-09/

# Process a specific entity only
run-pipeline --entity product
```

### Additional Options

```bash
# Test connections before running
test-pipeline

# Run in dry-run mode (shows what would be processed without making changes)
run-pipeline --dry-run

# Skip embeddings generation
run-pipeline --skip-embeddings

# Output results to a file
run-pipeline --output results.json
```

## Pipeline Process

The pipeline follows these steps:

1. **Configuration Loading**: Loads settings from files, environment variables, and defaults
2. **S3 Discovery**: Finds the latest date folder and entity folders in S3
3. **Data Import**: Downloads Parquet files from S3 and creates tables in PostgreSQL
4. **Embedding Generation**: Creates vector embeddings for text data
5. **FTS Setup**: Creates full-text search indices for text columns

## Project Structure

```
s3-postgres-pipeline/
├── src/
│   ├── config/       # Configuration management
│   ├── db/           # Database operations
│   ├── pipeline/     # Pipeline components
│   ├── s3/           # S3 operations
│   └── utils/        # Utilities (logging, etc.)
├── scripts/
│   ├── run_pipeline.py     # Main entry point
│   └── test_pipeline.py    # Connection testing
├── setup.py          # Package installation
└── README.md         # Documentation
```

## Troubleshooting

### Common Issues

**S3 Access Issues**
- Ensure AWS credentials are properly configured
- Check that the bucket name is correct
- Verify network connectivity to AWS

**Database Connection Issues**
- Verify PostgreSQL is running
- Check connection details (host, port, username, password)
- Ensure the database exists

**Embedding Generation Issues**
- Make sure sentence-transformers is installed
- Check that the model name is valid
- For pgvector support, ensure the extension is installed in PostgreSQL

### Logging

The pipeline generates detailed logs to help with troubleshooting:

```bash
# View the log file
cat pipeline_20250508.log

# Enable debug logging for more detailed information
run-pipeline --debug
```

## Advanced Usage

### Using as a Python Package

```python
from src.config import ConfigLoader
from src.utils import LoggingManager
from src.s3 import S3Client
from src.db import DBClient, EmbeddingsManager, FTSManager
from src.pipeline import Pipeline, ParquetImporter, EmbeddingsGenerator, FTSGenerator

# Load config
config_loader = ConfigLoader()

# Setup components
s3_client = S3Client(config_loader.get_s3_config())
db_client = DBClient(config_loader.get_db_config())
embeddings_manager = EmbeddingsManager(db_client, config_loader.get_embedding_config().get('model'))
fts_manager = FTSManager(db_client)

# Create pipeline
pipeline = Pipeline(s3_client)
pipeline.add_component(ParquetImporter(s3_client, db_client))
pipeline.add_component(EmbeddingsGenerator(db_client, embeddings_manager))
pipeline.add_component(FTSGenerator(db_client, fts_manager))

# Run pipeline
results = pipeline.run(entity_filter='product')
```

### Adding Custom Pipeline Components

Create a new component by extending the `PipelineComponent` base class:

```python
from src.pipeline.base import PipelineComponent

class MyCustomComponent(PipelineComponent):
    def __init__(self, some_dependency):
        super().__init__("MyCustomComponent")
        self.dependency = some_dependency
    
    def process_entity(self, entity_name, entity_data=None, **kwargs):
        # Your custom processing logic here
        return {
            'success': True,
            'message': f"Processed {entity_name} with custom logic"
        }

# Add to pipeline
pipeline.add_component(MyCustomComponent(dependency))
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
