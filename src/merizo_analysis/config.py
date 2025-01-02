import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("merizo_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('merizo_pipeline_logger')
