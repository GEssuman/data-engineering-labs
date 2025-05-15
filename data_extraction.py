from utils import download_dataset
from logging_config import setup_logger

kaggle_url = "mahatiratusher/flight-price-dataset-of-bangladesh"
output_dir = "./input"


logger = setup_logger(__name__) 

def extract():
    logger.info("Running extract_data")
    path = download_dataset(kaggle_url, output_dir)
    logger.info(f"Dataset downloaded and prepared. Directory:- {path}")


if __name__=="__main__":
    extract()