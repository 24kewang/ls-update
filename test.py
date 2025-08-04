import requests
import pandas as pd
import json
from datetime import datetime
import logging
import os
import time
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from asset_update import LansweeperAPI, is_empty, parse_date, compare_values, get_user_choice

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug.log'),
        logging.StreamHandler()
    ]
)

def main():
    # Load configuration from environment variables
    SITE_ID = os.getenv('LANSWEEPER_SITE_ID')
    PAT_TOKEN = os.getenv('LANSWEEPER_PAT_TOKEN')
    SPREADSHEET_PATH = os.getenv('SPREADSHEET_PATH', 'assets.xlsx')  # Default to assets.xlsx
    DISCREPANCIES_FILE = os.getenv('DISCREPANCIES_FILE', 'discrepancies.txt')  # Default filename
    
    # Validate required environment variables
    if not SITE_ID:
        logging.error("LANSWEEPER_SITE_ID environment variable is not set")
        return
    
    if not PAT_TOKEN:
        logging.error("LANSWEEPER_PAT_TOKEN environment variable is not set")
        return
    
    # Initialize API client
    api = LansweeperAPI(SITE_ID, PAT_TOKEN)
    count = 0
    # try:
    #     while True:
    #         count += 1
    #         asset = api.get_asset_by_serial('0F38CM324163GT')
    #         logging.info(f"Retrieved asset: {asset}, count: {count}")
    # except Exception as e:
    #     logging.error(f"Error retrieving asset: {e}")
    #     return

if __name__ == "__main__":
    main()