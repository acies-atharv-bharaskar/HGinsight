"""
Configuration loader module

Loads config from files and environment variables.
"""

import os
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Default configuration values - used as fallbacks
DEFAULT_S3_BUCKET = 'hg-dpi-prod-ch-dataload1'
DEFAULT_AWS_REGION = 'eu-north-1'
DEFAULT_DB_HOST = 'localhost'
DEFAULT_DB_NAME = 'postgres'
DEFAULT_DB_USER = 'postgres'
DEFAULT_DB_PASSWORD = 'Acies@123'  # TODO: Don't hardcode this in production
DEFAULT_DB_PORT = '5432'
DEFAULT_LOG_LEVEL = 'INFO'
DEFAULT_LOG_FILE = 'pipeline.log'
DEFAULT_EMBEDDING_MODEL = 'all-MiniLM-L6-v2'

class ConfigLoader:
    """
    Loads configuration from different sources with priority:
    1. Explicitly provided config_file
    2. Config file in standard locations
    3. Environment variables
    4. Default values
    """
    
    def __init__(self, config_file=None):
        """
        Initialize the configuration loader
        
        Args:
            config_file: Path to config file (optional)
        """
        self.config_file = config_file or self._find_config_file()
        self.config = self._load_config()
        self._validate_config()
        
    def _find_config_file(self):
        """Find config file in standard locations"""
        # Check standard locations
        locations = [
            os.path.join(os.getcwd(), 'config.json'),
            os.path.join(os.getcwd(), 'config', 'config.json'),
            os.path.expanduser('~/.s3-postgres-pipeline/config.json'),
            # Also check for .env file for simple configs
            os.path.join(os.getcwd(), '.env'),
        ]
        
        # Also check up to 3 parent directories
        cwd = Path(os.getcwd())
        for i in range(1, 4):
            parent = cwd.parents[i-1] if i <= len(cwd.parents) else None
            if not parent:
                break
                
            locations.append(str(parent / 'config.json'))
            locations.append(str(parent / 'config' / 'config.json'))
        
        # Try each location
        for location in locations:
            if os.path.exists(location):
                logger.info(f"Found config file at: {location}")
                return location
        
        logger.warning("No config file found, using environment variables and defaults")
        return None
    
    def _parse_env_file(self, env_file):
        """Parse a .env file into a dict"""
        result = {}
        
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                        
                    # Parse key=value
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Remove quotes if present
                        if value.startswith(('"', "'")) and value.endswith(value[0]):
                            value = value[1:-1]
                            
                        result[key] = value
            
            logger.debug(f"Loaded {len(result)} values from .env file")
        except Exception as e:
            logger.error(f"Error parsing .env file: {e}")
            
        return result
    
    def _load_config(self):
        """Load configuration from all sources"""
        # Start with default config
        config = {
            's3': {
                'bucket': os.environ.get('S3_BUCKET', DEFAULT_S3_BUCKET),
                'region': os.environ.get('AWS_REGION', DEFAULT_AWS_REGION),
                'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY')
            },
            'database': {
                'host': os.environ.get('DB_HOST', DEFAULT_DB_HOST),
                'database': os.environ.get('DB_NAME', DEFAULT_DB_NAME),
                'user': os.environ.get('DB_USER', DEFAULT_DB_USER),
                'password': os.environ.get('DB_PASSWORD', DEFAULT_DB_PASSWORD),
                'port': os.environ.get('DB_PORT', DEFAULT_DB_PORT)
            },
            'logging': {
                'level': os.environ.get('LOG_LEVEL', DEFAULT_LOG_LEVEL),
                'file': os.environ.get('LOG_FILE', DEFAULT_LOG_FILE),
                'format': os.environ.get('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            },
            'embedding': {
                'model': os.environ.get('EMBEDDING_MODEL', DEFAULT_EMBEDDING_MODEL)
            }
        }
        
        # Override with file settings if available
        if self.config_file and os.path.exists(self.config_file):
            try:
                # Handle .env file
                if self.config_file.endswith('.env'):
                    env_values = self._parse_env_file(self.config_file)
                    
                    # Map known values to config structure
                    if 'S3_BUCKET' in env_values:
                        config['s3']['bucket'] = env_values['S3_BUCKET']
                    if 'AWS_REGION' in env_values:
                        config['s3']['region'] = env_values['AWS_REGION']
                    if 'AWS_ACCESS_KEY_ID' in env_values:
                        config['s3']['aws_access_key_id'] = env_values['AWS_ACCESS_KEY_ID']
                    if 'AWS_SECRET_ACCESS_KEY' in env_values:
                        config['s3']['aws_secret_access_key'] = env_values['AWS_SECRET_ACCESS_KEY']
                    
                    if 'DB_HOST' in env_values:
                        config['database']['host'] = env_values['DB_HOST']
                    if 'DB_NAME' in env_values:
                        config['database']['database'] = env_values['DB_NAME']
                    if 'DB_USER' in env_values:
                        config['database']['user'] = env_values['DB_USER']
                    if 'DB_PASSWORD' in env_values:
                        config['database']['password'] = env_values['DB_PASSWORD']
                    if 'DB_PORT' in env_values:
                        config['database']['port'] = env_values['DB_PORT']
                    
                    if 'LOG_LEVEL' in env_values:
                        config['logging']['level'] = env_values['LOG_LEVEL']
                    if 'LOG_FILE' in env_values:
                        config['logging']['file'] = env_values['LOG_FILE']
                    if 'LOG_FORMAT' in env_values:
                        config['logging']['format'] = env_values['LOG_FORMAT']
                    
                    if 'EMBEDDING_MODEL' in env_values:
                        config['embedding']['model'] = env_values['EMBEDDING_MODEL']
                    
                # Handle JSON config        
                else:
                    with open(self.config_file, 'r') as f:
                        file_config = json.load(f)
                        
                    # Deep merge the configs
                    self._deep_merge(config, file_config)
                
                logger.info(f"Loaded configuration from {self.config_file}")
            except Exception as e:
                logger.error(f"Error loading config file: {e}")
        
        return config
    
    def _deep_merge(self, target, source):
        """Deep merge two dictionaries"""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value
    
    def _validate_config(self):
        """Do basic validation of the config"""
        # Check required values
        required = {
            's3': ['bucket', 'region'],
            'database': ['host', 'database', 'user', 'password', 'port'],
            'logging': ['level', 'file'],
            'embedding': ['model']
        }
        
        # Just log warnings for missing values - we have defaults for everything
        for section, keys in required.items():
            if section not in self.config:
                logger.warning(f"Missing config section: {section}")
                continue
                
            for key in keys:
                if key not in self.config[section] or not self.config[section][key]:
                    logger.warning(f"Missing required config value: {section}.{key}")
    
    def get_s3_config(self):
        """Get S3 configuration"""
        return self.config.get('s3', {})
    
    def get_db_config(self):
        """Get database configuration"""
        return self.config.get('database', {})
    
    def get_logging_config(self):
        """Get logging configuration"""
        return self.config.get('logging', {})
    
    def get_embedding_config(self):
        """Get embedding configuration"""
        return self.config.get('embedding', {})
    
    def get_config(self, section=None):
        """
        Get configuration
        
        Args:
            section: Section name or None for entire config
        """
        if section:
            return self.config.get(section, {})
        return self.config
    
    def print_config_summary(self, include_sensitive=False):
        """Print a summary of the configuration (useful for debugging)"""
        # Clone the config
        config_copy = json.loads(json.dumps(self.config))
        
        # Mask sensitive values
        if not include_sensitive:
            if 's3' in config_copy and 'aws_secret_access_key' in config_copy['s3']:
                if config_copy['s3']['aws_secret_access_key']:
                    config_copy['s3']['aws_secret_access_key'] = '***MASKED***'
            
            if 'database' in config_copy and 'password' in config_copy['database']:
                if config_copy['database']['password']:
                    config_copy['database']['password'] = '***MASKED***'
        
        print("\nConfiguration Summary:")
        print(json.dumps(config_copy, indent=2))
        print()

# For testing/debugging
if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Test the config loader
    loader = ConfigLoader()
    loader.print_config_summary()
    
    print("S3 Config:", loader.get_s3_config())
    print("DB Config:", loader.get_db_config())
    print("Logging Config:", loader.get_logging_config())
    print("Embedding Config:", loader.get_embedding_config())