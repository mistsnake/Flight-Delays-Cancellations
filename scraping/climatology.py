
import os
import subprocess
import logging
from multiprocessing import Pool
from tqdm import tqdm
from datetime import datetime

def setup_logging():
    """Sets up basic logging configuration."""
    log_base_dir = 'logs'
    os.makedirs(log_base_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logging.basicConfig(
        filename=os.path.join(log_base_dir, f'scrape_{timestamp}.log'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler())

def try_download_url(url, output_path):
    """Attempts to download from a specific URL."""
    try:
        subprocess.run(['wget', '-O', output_path, url], check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError:
        return False

def download_tar_gz_file(year, output_folder, retry_count=3):
    """Downloads a single .tar.gz file for the given year with retry mechanism."""
    # Define the date patterns to try in order
    date_patterns = ['20240724', '20240731', '20240726']
    
    for date_pattern in date_patterns:
        url = f"https://www.ncei.noaa.gov/oa/local-climatological-data/v2/archive/lcd_v2.0.0_d{year}_c{date_pattern}.tar.gz"
        file_name = url.split('/')[-1]
        output_path = os.path.join(output_folder, file_name)
        
        logging.info(f"Trying pattern {date_pattern} for year {year}: {url}")
        
        # Try multiple times for each URL
        for attempt in range(retry_count):
            if try_download_url(url, output_path):
                logging.info(f"Successfully downloaded {file_name} with pattern {date_pattern} after {attempt + 1} attempt(s)")
                return True
            
            if attempt < retry_count - 1:
                logging.info(f"Retrying download attempt {attempt + 1} for {file_name}")
        
        # If we've exhausted retries for this pattern, try the next pattern
        logging.info(f"Pattern {date_pattern} failed for year {year}, trying next pattern if available")
    
    # If we get here, all patterns have failed
    logging.error(f"Failed to download file for year {year} with all patterns after {retry_count} attempts each")
    return False

def download_tar_gz_file_wrapper(args):
    """Wrapper function for passing arguments to Pool."""
    return download_tar_gz_file(*args)

def download_tar_gz_files_parallel(years, output_folder, num_workers=8):
    """Parallelizes the downloading of .tar.gz files for a list of years."""
    os.makedirs(output_folder, exist_ok=True)
    
    # Prepare the arguments for multiprocessing
    download_args = [(year, output_folder) for year in years]
    
    # Use a multiprocessing pool with progress bar
    with Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(download_tar_gz_file_wrapper, download_args),
            total=len(years),
            desc="Downloading files"
        ))
    
    # Check for any failed downloads
    failed_years = [years[i] for i, success in enumerate(results) if not success]
    if failed_years:
        logging.error(f"Failed to download files for the following years: {failed_years}")
    else:
        logging.info("All files downloaded successfully.")
    
    # Return failed years for potential retry
    return failed_years

if __name__ == '__main__':
    setup_logging()
    
    # Define the years we want to download
    years = [1750] + list(range(1790, 2025))
    
    # Set the output folder
    output_folder = 'tar_gz_downloads'
    
    # Number of parallel workers
    num_workers = 8
    
    # Start parallel downloading with progress bar
    failed_years = download_tar_gz_files_parallel(years, output_folder, num_workers=num_workers)
    
    # Log summary
    if failed_years:
        logging.info(f"Download complete. Failed years: {failed_years}")
    else:
        logging.info("Download complete. All years successful.")