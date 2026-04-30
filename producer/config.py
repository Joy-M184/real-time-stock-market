import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging to console AND file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),                                    # ✅ logs to terminal/Docker
        logging.FileHandler('stock_data_logs.logs.log')            # ✅ logs to file
    ]
)

logger = logging.getLogger(__name__)

BASEURL = "alpha-vantage.p.rapidapi.com"
url = f"https://{BASEURL}/query"
api_key = os.getenv("API_KEY")

headers = {
    "X-RapidAPI-Key": api_key,
    "X-RapidAPI-Host": BASEURL
}