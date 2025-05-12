"""
Main pipeline orchestration module
"""

import logging
import time
import traceback
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Main pipeline that runs all the processing steps
    """
    
    def __init__(self, s3_client, components=None):
        self.s3_client = s3_client
        self.components = components or []
        
        # Keep track of runs
        self.run_count = 0
        
        # Make sure components is a list
        if not isinstance(self.components, list):
            self.components = [self.components]
        
        logger.info(f"Pipeline initialized with {len(self.components)} components")
    
    def add_component(self, component):
        """Add a processing component to the pipeline"""
        self.components.append(component)
        logger.debug(f"Added component: {component.name}")
    
    def run(self, date_folder=None, entity_filter=None):
        """
        Run the pipeline
        
        Args:
            date_folder: S3 folder to process (uses latest if None)
            entity_filter: Only process this entity if specified
        """
        self.run_count += 1
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.run_count}"
        logger.info(f"Starting pipeline run {run_id}")
        start_time = time.time()
        
        results = {
            'run_id': run_id,
            'date_folder': None,
            'entities': [],
            'success': False,
            'start_time': datetime.now().isoformat(),
        }
        
        try:
            # Get latest date folder if none provided
            if date_folder is None:
                logger.info("No date folder specified, looking for latest")
                date_folder = self.s3_client.get_latest_date_folder()
            
            if not date_folder:
                err_msg = "No date folder found in S3"
                logger.error(err_msg)
                results['message'] = err_msg
                results['total_time'] = f"{time.time() - start_time:.2f}s"
                return results
                
            results['date_folder'] = date_folder
            logger.info(f"Processing date folder: {date_folder}")
            
            # Find entity folders
            entity_folders = self.s3_client.get_entity_folders(date_folder)
            if not entity_folders:
                err_msg = f"No entity folders found in {date_folder}"
                logger.error(err_msg)
                results['message'] = err_msg
                results['total_time'] = f"{time.time() - start_time:.2f}s"
                return results
                
            logger.info(f"Found {len(entity_folders)} entity folders")
            
            # Save list of all entities even if we don't process them all
            results['available_entities'] = [e[0] for e in entity_folders]
            
            # Track overall success
            all_success = True
            entities_processed = 0
            
            # Process each entity
            for entity_name, entity_folder in entity_folders:
                # Filter if requested
                if entity_filter and entity_name != entity_filter:
                    logger.info(f"Skipping {entity_name} (filter active for {entity_filter})")
                    continue

                # Actually process the entity
                entity_result = self.process_entity(entity_name, entity_folder)
                entities_processed += 1
                
                # Add to results
                results['entities'].append(entity_result)
                
                # Update overall success flag
                if not entity_result.get('success', False):
                    all_success = False
                    logger.warning(f"Entity {entity_name} had errors")

            # Check that we actually processed something
            if entities_processed == 0:
                if entity_filter:
                    err_msg = f"Entity '{entity_filter}' not found in {date_folder}"
                else:
                    err_msg = "No entities processed"
                    
                logger.error(err_msg)
                results['message'] = err_msg
                results['success'] = False
            else:
                # Set overall success and message
                results['success'] = all_success
                if all_success:
                    results['message'] = f"Successfully processed {entities_processed} entities"
                else:
                    results['message'] = f"Completed with errors in some entities ({entities_processed} processed)"
            
            # Add timing info
            total_time = time.time() - start_time
            results['total_time'] = f"{total_time:.2f}s"
            results['end_time'] = datetime.now().isoformat()
                
            logger.info(f"Pipeline completed in {total_time:.2f}s - Success: {all_success}")
            
            # Add simple stats
            success_count = sum(1 for e in results['entities'] if e.get('success', False))
            results['stats'] = {
                'total_entities': len(results['entities']),
                'successful_entities': success_count,
                'failed_entities': len(results['entities']) - success_count
            }
            
            return results
            
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            logger.debug(traceback.format_exc())
            
            # Complete the results with error info
            total_time = time.time() - start_time
            results['message'] = f"Error: {str(e)}"
            results['total_time'] = f"{total_time:.2f}s"
            results['error'] = str(e)
            results['end_time'] = datetime.now().isoformat()
            
            return results
    
    def process_entity(self, entity_name, entity_folder):
        """
        Process a single entity through all components
        """
        logger.info(f"Processing entity: {entity_name} ({entity_folder})")
        entity_start = time.time()
        
        result = {
            'entity': entity_name,
            'folder': entity_folder,
            'stages': {},
            'success': True,  # Assume success until a component fails
            'start_time': datetime.now().isoformat()
        }
        
        # Data to pass between components
        entity_data = {'entity_folder': entity_folder}
        
        # Track if any critical components failed
        had_critical_failure = False
        
        # Run each component in sequence
        for i, component in enumerate(self.components):
            # Skip further processing if a critical component failed
            if had_critical_failure:
                logger.warning(f"Skipping {component.name} due to previous failures")
                result['stages'][component.name] = {
                    'success': False,
                    'time': '0.00s',
                    'message': 'Skipped due to previous component failure',
                    'skipped': True
                }
                continue
                
            # Process with this component
            try:
                logger.info(f"Running component {i+1}/{len(self.components)}: {component.name}")
                component_result = component.process(entity_name, entity_data, **entity_data)
                
                # Add to results
                result['stages'][component.name] = {
                    'success': component_result.get('success', False),
                    'time': component_result.get('time', '0.00s'),
                    'message': component_result.get('message', '')
                }
                
                # Update entity data for next component
                entity_data.update(component_result)
                
                # Check if this component failed
                if not component_result.get('success', False):
                    result['success'] = False
                    
                    # If importer fails, we can't continue
                    if component.name == 'ParquetImporter':
                        had_critical_failure = True
                        logger.error(f"Critical component {component.name} failed - stopping pipeline for {entity_name}")
            except Exception as e:
                # Handle component exceptions
                logger.error(f"Error in component {component.name}: {str(e)}")
                logger.debug(traceback.format_exc())
                
                result['stages'][component.name] = {
                    'success': False,
                    'time': '0.00s',
                    'message': f"Error: {str(e)}",
                    'error': str(e)
                }
                
                result['success'] = False
                
                # If importer fails, we can't continue
                if component.name == 'ParquetImporter':
                    had_critical_failure = True
        
        # Add timing info
        total_time = time.time() - entity_start
        result['total_time'] = f"{total_time:.2f}s"
        result['end_time'] = datetime.now().isoformat()
        
        # Log completion
        success_str = "successfully" if result['success'] else "with errors"
        logger.info(f"Completed {entity_name} {success_str} in {total_time:.2f}s")
        
        return result

# Helper function to save pipeline results to disk
def save_results(results, output_dir="logs"):
    """Save pipeline results to a JSON file"""
    import json
    
    try:
        # Create directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Create filename based on run info
        run_id = results.get('run_id', 'unknown')
        success = 'success' if results.get('success', False) else 'failed'
        filename = f"{output_dir}/pipeline_{run_id}_{success}.json"
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
            
        return filename
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
        return None