import requests
import pandas as pd
import json
from datetime import datetime
import logging
import os
import time
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from asset_update import is_empty, parse_date, compare_values, get_user_choice

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger('test')

# global variable for manual quit
quit_processing = False

class LansweeperAPI:
    def __init__(self, site_id: str, pat_token: str):
        """
        Initialize Lansweeper API client
        
        Args:
            site_id: Your Lansweeper site ID
            pat_token: Personal Access Token
        """
        self.site_id = site_id
        self.pat_token = pat_token
        self.base_url = f"https://api.lansweeper.com/api/v2/graphql"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {pat_token}"
        }
        self.request_count = 0
    
    def _check_rate_limit(self):
        """Check if we need to wait due to rate limiting"""
        # Lansweeper API allows 150 requests per minute, but we will most likely not hit this limit if only this script is running
        if self.request_count % 150 == 0 and self.request_count > 0:
            logger.info(f"Reached {self.request_count} requests.")
            choice = input("Press enter to continue processing, or 'q' to quit: ").strip()
            if choice == 'q':
                logger.info("User chose to quit processing.")
                global quit_processing
                quit_processing = True
                return

    def get_asset_by_serial(self, serial_number: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve asset information by serial number
        
        Args:
            serial_number: The serial number to search for
            
        Returns:
            Asset data dictionary or None if not found
        """
        self._check_rate_limit()
        self.request_count += 1
        
        query = """
        query GetAssetBySerial($siteId: ID!, $serialNumber: String!) {
            site(id: $siteId) {
                assetResources(
                    assetPagination: { limit: 1 }
                    filters: {
                        conditions: [{
                            path: "assetCustom.serialNumber"
                            operator: EQUAL
                            value: $serialNumber
                        }]
                    }
                    fields: [
                        "key"
                        "assetBasicInfo.name"
                        "assetCustom.barCode"
                        "assetCustom.serialNumber"
                        "assetCustom.purchaseDate"
                        "assetCustom.warrantyDate"
                        "url"
                    ]
                ) {
                    total
                    items
                }
            }
        }
        """
        
        variables = {
            "siteId": self.site_id,
            "serialNumber": serial_number
        }
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={"query": query, "variables": variables}
            )
            response.raise_for_status()
            
            data = response.json()
            if 'errors' in data:
                logger.error(f"GraphQL errors for serial {serial_number}: {data['errors']}")
                return None
            
            assets = data['data']['site']['assetResources']['items']
            if assets:
                return assets[0]
            else:
                logger.warning(f"No asset found with serial number: {serial_number}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for serial {serial_number}: {e}")
            return None
        except (KeyError, TypeError) as e:
            logger.error(f"Unexpected response structure for serial {serial_number}: {e}")
            return None
    def get_asset_test(self, serial_number: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve asset information by serial number
        
        Args:
            serial_number: The serial number to search for
            
        Returns:
            Asset data dictionary or None if not found
        """
        self._check_rate_limit()
        self.request_count += 1
        
        query = """
        query GetAssetBySerial($siteId: ID!, $serialNumber: String!) {
            site(id: $siteId) {
                assetResources(
                    assetPagination: { limit: 10 }
                    filters: {
                        conditions: [{
                            path: "assetCustom.serialNumber"
                            operator: EQUAL
                            value: $serialNumber
                        }]
                    }
                    fields: [
                        "key"
                        "assetBasicInfo.name"
                        "assetCustom.barCode"
                        "assetCustom.serialNumber"
                        "assetCustom.purchaseDate"
                        "assetCustom.warrantyDate"
                        "url"
                    ]
                ) {
                    total
                    items
                }
            }
        }
        """
        
        variables = {
            "siteId": self.site_id,
            "serialNumber": serial_number
        }
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={"query": query, "variables": variables}
            )
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"Response data for serial {json.dumps(data, indent=2)}\n")
            logger.debug(f"Number of items returned: {data['data']['site']['assetResources']['total']}")
            if 'errors' in data:
                logger.error(f"GraphQL errors for serial {serial_number}: {data['errors']}")
                return None
            
            assets = data['data']['site']['assetResources']['items']
            if assets:
                return assets[0]
            else:
                logger.warning(f"No asset found with serial number: {serial_number}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for serial {serial_number}: {e}")
            return None
        except (KeyError, TypeError) as e:
            logger.error(f"Unexpected response structure for serial {serial_number}: {e}")
            return None
    def update_asset(self, asset_key: str, serial_number: str, fields_to_update: Dict[str, str]) -> bool:
        """
        Update asset information
        
        Args:
            asset_key: The asset key/ID
            fields_to_update: Dictionary of field names and values to update
                            e.g., {'purchaseDate': '2024-01-01', 'warrantyDate': '2025-01-01', 'barCode': 'BC123'}
            
        Returns:
            True if successful, False otherwise
        """
        self._check_rate_limit()
        self.request_count += 1
        
        if not fields_to_update:
            return True  # Nothing to update
        
        # Build the custom fields update object
        custom_fields = {}
        for field_name, value in fields_to_update.items():
            if field_name in ['purchaseDate', 'warrantyDate']:
                # Convert to ISO 8601 DateTime format and wrap in ValueDateInput object
                iso_date = parse_date(value, 'lansweeper')
                if iso_date:
                    custom_fields[field_name] = {"value": iso_date}
            else:
                # For other fields like barCode, use the value directly
                custom_fields[field_name] = value

        if not custom_fields:
            return True  # Nothing to update after processing
        
        mutation = """
        mutation EditAsset($siteId: ID!, $key: ID!, $customFields: AssetCustomInput!) {
            site(id: $siteId) {
                editAsset(
                    key: $key
                    fields: {
                        assetCustom: $customFields
                    }
                ) {
                    assetCustom {
                        purchaseDate
                        warrantyDate
                        barCode
                    }
                }
            }
        }
        """
        
        variables = {
            "siteId": self.site_id,
            "key": asset_key,
            "customFields": custom_fields
        }
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={"query": mutation, "variables": variables}
            )
            response.raise_for_status()

            data = response.json()
            if 'errors' in data:
                logger.error(f"Update failed for Serial {serial_number}: {data['errors']}")
                return False

            logger.info(f"Successfully updated Serial {serial_number} with fields: {list(fields_to_update.keys())}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Update request failed for Serial {serial_number}: {e}")
            return False


def main():
    # Load configuration from environment variables
    SITE_ID = os.getenv('LANSWEEPER_SITE_ID')
    PAT_TOKEN = os.getenv('LANSWEEPER_PAT_TOKEN')
    SPREADSHEET_PATH = os.getenv('SPREADSHEET_PATH', 'assets.xlsx')  # Default to assets.xlsx
    DISCREPANCIES_FILE = os.getenv('DISCREPANCIES_FILE', 'discrepancies.txt')  # Default filename
    
    # Validate required environment variables
    if not SITE_ID:
        logger.error("LANSWEEPER_SITE_ID environment variable is not set")
        return
    
    if not PAT_TOKEN:
        logger.error("LANSWEEPER_PAT_TOKEN environment variable is not set")
        return
    
    # Initialize API client
    api = LansweeperAPI(SITE_ID, PAT_TOKEN)
    count = 0
    logger.info("Testing Debug")

    try:
        # Read the spreadsheet
        api.get_asset_test('PW006MH4')
        # logger.info(f"Reading spreadsheet: {SPREADSHEET_PATH}")
        # df = pd.read_excel(SPREADSHEET_PATH)
        # logger.debug((df.at[262, 'Barcode Number']))  # Example modification for testing
        # logger.debug(str(df.at[256, 'Serial Number']))  # Example modification for testing
        # logger.debug(df.at[256, 'Invoice Date'])  # Example modification for testing
        # logger.debug(df.at[256, 'Department'])  # Example modification for testing

        # df.at[256, 'Barcode Number'] = '0002222222'  # Example modification for testing
        # df.at[256, 'Invoice Date'] = '2033-04-16'  # Example modification for testing
        # df.at[256, 'Extended Warranty'] = '2023-03-16 00:00:00'  # Example modification for testing
        # df.at[256, 'Department'] = 'Human Resources'  # Example modification for testing
        # df.at[256, 'Status'] = 'In Stock'  # Example modification for testing
        # df.to_excel(SPREADSHEET_PATH, index=False)  # Save changes back to the spreadsheet
        # df.to
        # Verify required columns exist
        # required_columns = ['Serial Number', 'Barcode Number', 'Invoice Date', 'Extended Warranty']
        # missing_columns = [col for col in required_columns if col not in df.columns]
        # if missing_columns:
        #     raise ValueError(f"Missing required columns: {missing_columns}")
        
        # for index, row in df.iterrows():
        #     api.get_asset_test(row['Serial Number'])
        # logger.info(f"Done Processing")
    except Exception as e:
        logger.error(f"Failed to read spreadsheet: {e}")
        return
    # try:
    #     while True:
    #         count += 1
    #         asset = api.get_asset_by_serial('0F38CM324163GT')
    #         logger.info(f"Retrieved asset: {asset}, count: {count}")
    # except Exception as e:
    #     logger.error(f"Error retrieving asset: {e}")
    #     return

if __name__ == "__main__":
    main()