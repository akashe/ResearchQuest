import os
import logging
from dotenv import load_dotenv


os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s\n",
    handlers=[logging.FileHandler("data/llm_usage.log"), logging.StreamHandler()]
)

logger = logging.getLogger(__name__)