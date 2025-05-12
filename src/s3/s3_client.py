"""
S3 Client module for AWS operations
"""
import boto3
import logging
import re
from datetime import datetime
import time  # For retry backoff

logger = logging.getLogger(__name__)

# Max retries for S3 operations
MAX_RETRIES = 3

class S3Client:
    """AWS S3 operations wrapper"""
    
    def __init__(self, config):
        """Initialize with S3 config"""
        self.config = config
        self._client = None  # Lazy initialization
        self.bucket = config.get('bucket', 'hg-dpi-prod-ch-dataload1')
        
    @property
    def client(self):
        """Get the boto3 client - lazy loaded on first use"""
        if not self._client:
            self._create_client()
        return self._client
    
    def _create_client(self):
        """Create the S3 client"""
        # Use credentials if provided, otherwise rely on environment/instance profile
        kwargs = {'region_name': self.config.get('region', 'eu-north-1')}
        
        # Only add credentials if both are provided
        key_id = self.config.get('aws_access_key_id')
        secret_key = self.config.get('aws_secret_access_key')
        
        if key_id and secret_key:
            kwargs.update({
                'aws_access_key_id': key_id,
                'aws_secret_access_key': secret_key
            })
            logger.debug("Using provided AWS credentials")
        else:
            logger.debug("Using environment/instance profile for AWS credentials")
        
        try:
            self._client = boto3.client('s3', **kwargs)
            # Test connection
            self._client.list_buckets()
            logger.info("Successfully connected to S3")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            # Still save the client - it might work later (temporary issue)
            self._client = boto3.client('s3', **kwargs)
            raise
    
    def list_buckets(self):
        """Get list of available S3 buckets"""
        try:
            response = self.client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            logger.error(f"Error listing buckets: {e}")
            return []
    
    def get_bucket_name(self):
        """Get the configured bucket name"""
        return self.bucket
    
    def get_latest_date_folder(self, bucket_name=None):
        """
        Find the most recent date folder
        
        Format expected: YYYY-MM-DD-HH/
        """
        bucket = bucket_name or self.bucket
        
        # Try multiple times with backoff
        for attempt in range(MAX_RETRIES):
            try:
                # List with delimiter to get "directories"
                response = self.client.list_objects_v2(
                    Bucket=bucket,
                    Delimiter='/'
                )
                
                # No folders found
                if 'CommonPrefixes' not in response:
                    logger.warning(f"No folders found in bucket {bucket}")
                    return None
                
                # Look for date-formatted folders
                date_folders = []
                date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}-\d{2}/$')
                
                for prefix in response['CommonPrefixes']:
                    folder = prefix['Prefix']
                    if date_pattern.match(folder):
                        # Parse the date (remove trailing slash)
                        date_str = folder.rstrip('/')
                        try:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d-%H')
                            date_folders.append((date_obj, folder))
                        except ValueError:
                            # Skip invalid dates
                            logger.warning(f"Skipping invalid date format: {folder}")
                
                if not date_folders:
                    logger.warning("No valid date folders found")
                    return None
                
                # Sort newest first and return the latest
                date_folders.sort(reverse=True)
                latest = date_folders[0][1]
                logger.info(f"Latest date folder: {latest}")
                return latest
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    # Exponential backoff
                    sleep_time = 2 ** attempt
                    logger.warning(f"S3 error (attempt {attempt+1}/{MAX_RETRIES}): {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to get date folders after {MAX_RETRIES} attempts: {e}")
                    return None
    
    # Internal table name mapping
    # Keeping this hardcoded for now, might move to config later
    def map_folder_to_table_name(self, folder_name):
        """Map S3 folder names to DB table names"""
        # Handle special cases first
        special_cases = {
            'products': 'product',
            'spend_categories': 'spend_category',
            'vendors': 'vendor',
            'customers': 'customer',
            'employees': 'employee',
        }
        
        if folder_name in special_cases:
            return special_cases[folder_name]
        
        # Handle simple plural -> singular (quick and dirty)
        if folder_name.endswith('s') and not folder_name.endswith(('ss', 'us', 'is')):
            return folder_name[:-1]
        
        # Just use as-is for anything else
        return folder_name
    
    def get_entity_folders(self, date_folder, bucket_name=None):
        """Get all entity folders within the date folder"""
        bucket = bucket_name or self.bucket
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=date_folder,
                    Delimiter='/'
                )
                
                if 'CommonPrefixes' not in response:
                    logger.warning(f"No entity folders found in {date_folder}")
                    return []
                
                # Process each folder
                entity_folders = []
                for prefix in response['CommonPrefixes']:
                    folder_path = prefix['Prefix']  # e.g. 2025-04-14-09/products/
                    
                    # Extract entity name from path
                    # e.g. 2025-04-14-09/products/ -> products
                    s3_folder_name = folder_path.replace(date_folder, '').rstrip('/')
                    
                    # Skip empty or weird names
                    if not s3_folder_name or '/' in s3_folder_name:
                        logger.warning(f"Skipping unusual folder name: {folder_path}")
                        continue
                    
                    # Map to table name
                    table_name = self.map_folder_to_table_name(s3_folder_name)
                    entity_folders.append((table_name, folder_path))
                
                # Log what we found
                folder_names = [e[0] for e in entity_folders]
                logger.info(f"Found {len(entity_folders)} entity folders: {folder_names}")
                return entity_folders
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    sleep_time = 2 ** attempt
                    logger.warning(f"S3 error (attempt {attempt+1}/{MAX_RETRIES}): {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to get entity folders after {MAX_RETRIES} attempts: {e}")
                    return []
    
    def get_parquet_files(self, entity_folder, bucket_name=None):
        """Find parquet files in an entity folder"""
        bucket = bucket_name or self.bucket
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=entity_folder
                )
                
                if 'Contents' not in response:
                    logger.warning(f"No files found in {entity_folder}")
                    return []
                
                # Filter for parquet files
                parquet_files = []
                for item in response['Contents']:
                    key = item['Key']
                    # Typical extensions: .parquet or .snappy.parquet
                    if key.endswith(('.parquet', '.snappy.parquet')):
                        parquet_files.append(key)
                
                # Warn if we don't find any
                if not parquet_files:
                    logger.warning(f"No parquet files found in {entity_folder}")
                
                logger.info(f"Found {len(parquet_files)} parquet files in {entity_folder}")
                return parquet_files
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    sleep_time = 2 ** attempt  
                    logger.warning(f"S3 error (attempt {attempt+1}/{MAX_RETRIES}): {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to get parquet files after {MAX_RETRIES} attempts: {e}")
                    return []
    
    def download_file(self, file_key, bucket_name=None):
        """Download a file from S3"""
        bucket = bucket_name or self.bucket
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.debug(f"Downloading {file_key}...")
                start_time = time.time()
                
                response = self.client.get_object(Bucket=bucket, Key=file_key)
                file_data = response['Body'].read()
                
                download_time = time.time() - start_time
                file_size_mb = len(file_data) / (1024 * 1024)
                logger.info(f"Downloaded {file_size_mb:.2f} MB in {download_time:.2f}s ({file_size_mb/download_time:.2f} MB/s)")
                
                return file_data
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    sleep_time = 2 ** attempt
                    logger.warning(f"S3 download error (attempt {attempt+1}/{MAX_RETRIES}): {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to download file after {MAX_RETRIES} attempts: {e}")
                    raise
    
    def file_exists(self, file_key, bucket_name=None):
        """Check if a file exists in S3"""
        bucket = bucket_name or self.bucket
        
        try:
            self.client.head_object(Bucket=bucket, Key=file_key)
            return True
        except Exception:
            return False

# Quick helper to make testing easier
def get_test_client():
    """Create a client for testing"""
    config = {
        'bucket': 'hg-dpi-prod-ch-dataload1',
        'region': 'eu-north-1',
    }
    return S3Client(config)