import kagglehub
import os
import shutil
import pathlib
from logging_config import setup_logger


logger = setup_logger(__name__)  # or setup_logger("etl.utils")


def create_dir(dir):
    try:
        os.makedirs(dir)
        logger.info(f"Created directory: {dir}")
    except FileExistsError as e:
        logger.warning(f"the dir:- {dir} already exists")
    except Exception as e:
        logger.error(f"Error:- {e}")

def path_exists(dir):
    return os.path.exists(dir)


def download_dataset(url, output_dir):
    try:
        # Download latest version
        logger.info("Starting dataset download function.")

        if path_exists(output_dir) and os.listdir(output_dir):
            logger.info(f"Dataset already exists in '{output_dir}'. Skipping download.")
            return
        
        logger.info(f"Downloading dataset from: {url}")
        path = kagglehub.dataset_download(url)


        create_dir(output_dir)

        #move content in path to input path
        for file in os.listdir(path):
            src = os.path.join(path, file)
            shutil.move(src, output_dir)
        logger.info(f"Moved files: {path} → {output_dir}")

        
        shutil.rmtree(path)
        logger.info(f"Removed temporary download directory: {path}")
    except FileExistsError as e:
        logger.warning(f"File Already Exist:- {e}")
    except Exception as e:
        logger.error(f"Error:- {e}")
    finally:    
        return output_dir