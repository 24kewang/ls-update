import requests
import pandas as pd
import json
from datetime import datetime
import logging
import os
import time
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from logging_config import setup_loggers

# Load environment variables from .env file
load_dotenv()

# Setup loggers
setup_loggers()

# Configure logging
logger = logging.getLogger('asset_update')

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
            if 'errors' in data:
                logger.error(f"GraphQL errors for serial {serial_number}: {data['errors']}")
                return None
            return data['data']['site']['assetResources']
                
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

def parse_date(date_str: str, output_fmt: str = 'normal') -> Optional[str]:
    """
    Parse date string and return in specified format
    
    Args:
        date_str: Date string in various formats
        output_fmt: Output format - 'normal' for YYYY-MM-DD or 'lansweeper' for ISO 8601
        
    Returns:
        Standardized date string or None if parsing fails
    """
    if is_empty(date_str):
        return None
    
    # Convert to string if it's not already
    date_str = str(date_str).strip()
    
    # Try different date formats
    date_formats = [
        # ISO formats (from GraphQL responses)
        '%Y-%m-%dT%H:%M:%S.%fZ',      # 2024-11-08T00:00:00.000Z
        '%Y-%m-%dT%H:%M:%SZ',         # 2024-11-08T00:00:00Z  LS currently uses this format
        '%Y-%m-%d %H:%M:%S',          # 2024-11-08 00:00:00  xlsx uses this format
        '%Y-%m-%dT%H:%M:%S',          # 2024-11-08T00:00:00
        # Standard date formats
        '%Y-%m-%d',                   # 2024-11-08
        '%m/%d/%Y',                   # 11/08/2024
        '%d/%m/%Y',                   # 08/11/2024
        '%Y/%m/%d',                   # 2024/11/08
        '%m-%d-%Y',                   # 11-08-2024
        '%d-%m-%Y',                   # 08-11-2024
        # Excel date formats
        '%m/%d/%y',                   # 11/8/24
        '%d/%m/%y',                   # 8/11/24
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            if output_fmt == 'lansweeper':
                return parsed_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:  # normal
                return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # Try to handle pandas Timestamp objects
    try:
        if hasattr(date_str, 'strftime'):
            if output_fmt == 'lansweeper':
                return date_str.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:  # normal
                return date_str.strftime('%Y-%m-%d')
    except:
        pass
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def compare_values(spreadsheet_val, lansweeper_val, field_name: str) -> bool:
    """
    Compare values and return True if they match
    
    Args:
        spreadsheet_val: Value from spreadsheet
        lansweeper_val: Value from Lansweeper
        field_name: Name of the field being compared
        
    Returns:
        True if values match, False otherwise
    """
    # Handle None/empty values
    if pd.isna(spreadsheet_val) and (lansweeper_val is None or lansweeper_val == ''):
        return True
    if pd.isna(spreadsheet_val) or (lansweeper_val is None or lansweeper_val == ''):
        return False
    
    # For dates, normalize format
    if 'date' in field_name.lower():
        spreadsheet_date = parse_date(spreadsheet_val, 'normal')
        lansweeper_date = parse_date(lansweeper_val, 'normal')
        return spreadsheet_date == lansweeper_date
    
    # For other fields, do string comparison
    return str(spreadsheet_val).strip() == str(lansweeper_val).strip()

def is_empty(value) -> bool:
    """Check if a value is empty/null"""
    return pd.isna(value) or value == '' or value is None or str(value).strip() == ''

def get_user_choice(serial_number: str, field_name: str, spreadsheet_val: str, lansweeper_val: str) -> str:
    """
    Get user choice for handling conflicting values
    
    Returns: 'ls_to_sheet', 'sheet_to_ls', 'skip', or 'quit'
    """
    print(f"\n=== CONFLICT DETECTED ===")
    print(f"Serial Number: {serial_number}")
    print(f"Field: {field_name}")
    print(f"Spreadsheet value: '{spreadsheet_val}'")
    print(f"Lansweeper value: '{lansweeper_val}'")
    print("\nOptions:")
    print("1. Update spreadsheet with Lansweeper value")
    print("2. Update Lansweeper with spreadsheet value") 
    print("3. Skip this field")
    print("4. Quit processing")
    
    while True:
        choice = input("Enter your choice (1/2/3/4): ").strip()
        if choice == '1':
            return 'ls_to_sheet'
        elif choice == '2':
            return 'sheet_to_ls'
        elif choice == '3':
            return 'skip'
        elif choice == '4':
            return 'quit'
        else:
            print("Invalid choice. Please enter 1, 2, 3, or 4.")

def main():
    global quit_processing
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
    
    try:
        # Read the spreadsheet
        logger.info(f"Reading spreadsheet: {SPREADSHEET_PATH}")
        df = pd.read_excel(SPREADSHEET_PATH)
        
        # Verify required columns exist
        required_columns = ['Serial Number', 'Barcode Number', 'Invoice Date', 'Extended Warranty']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Track changes made to spreadsheet
        spreadsheet_changes = []
        ls_changes = []
        missing_info = []
        conflict_info = []
        quit_processing = False
        
        # Open discrepancies file
        with open(DISCREPANCIES_FILE, 'a') as discrepancy_file:
            discrepancy_file.write("\n" + "=" * 80 + "\n")
            discrepancy_file.write(f"Asset Discrepancy Report - Generated: {datetime.now()}\n")
            discrepancy_file.write("=" * 80 + "\n\n")
            
            # Process each row
            for index, row in df.iterrows():
                if quit_processing:
                    break
                    
                serial_number = row['Serial Number']
                if is_empty(serial_number):
                    logger.warning(f"Skipping row {index + 1}: No serial number")
                    continue
                
                logger.info(f"Processing serial number: {serial_number}")
                
                # Get asset from Lansweeper, check for correct number (1)
                asset = api.get_asset_by_serial(str(serial_number))
                if asset['total'] == 0:
                    discrepancy_file.write(f"ERROR: Asset not found for serial number: {serial_number}\n\n")
                    logger.error(f"Asset not found for serial number: {serial_number}")
                    continue
                elif asset['total'] > 1:
                    discrepancy_file.write(f"WARNING: Multiple assets found for serial number: {serial_number}. Please review.\n\n")
                    logger.warning(f"Multiple assets found for serial number: {serial_number}.")
                    continue
                else:
                    asset = asset['items'][0]

                logger.info(f"Retrieved asset for serial {serial_number}: {asset['assetBasicInfo']['name']}")

                # Extract values
                ls_barcode = asset['assetCustom'].get('barCode', '') if asset.get('assetCustom') else ''
                # catch case where LS barcode isn't number and log accordingly
                try:
                    int(ls_barcode) if not is_empty(ls_barcode) else ''
                except Exception:
                    conflict_info.append(f"Row {index + 1}: Serial {serial_number} - Invalid LS Barcode Format (not a number) - LS: '{ls_barcode}'\n")
                ls_purchase_date = asset['assetCustom'].get('purchaseDate', '') if asset.get('assetCustom') else ''
                ls_warranty_date = asset['assetCustom'].get('warrantyDate', '') if asset.get('assetCustom') else ''
                
                # truncate barcode from decimal to int then convert to string, try-except to catch cases where spreadsheet barcode isn't a number and log accordingly
                spreadsheet_barcode = row['Barcode Number']
                try:
                    int(spreadsheet_barcode) if not is_empty(spreadsheet_barcode) else ''
                except Exception:
                    conflict_info.append(f"Row {index + 1}: Serial {serial_number} - Invalid Sheet Barcode Format (not a number) - Sheet: '{spreadsheet_barcode}'\n")
                spreadsheet_purchase_date = row['Invoice Date']
                spreadsheet_warranty_date = row['Extended Warranty']

                # Track updates needed for LS
                ls_updates = {}
                
                # Process each field
                fields_to_process = [
                    ('Barcode Number', 'barCode', spreadsheet_barcode, ls_barcode),
                    ('Invoice Date', 'purchaseDate', spreadsheet_purchase_date, ls_purchase_date),
                    ('Extended Warranty', 'warrantyDate', spreadsheet_warranty_date, ls_warranty_date)
                ]
                
                for field_display_name, field_ls_name, sheet_val, ls_val in fields_to_process:
                    if quit_processing:
                        break
                        
                    sheet_empty = is_empty(sheet_val)
                    ls_empty = is_empty(ls_val)
                    
                    if sheet_empty and ls_empty:
                        # Both empty - log but continue
                        missing_info.append(f"Row {index + 1}: Serial {serial_number} - {field_display_name}: Both values are empty\n")
                        logger.info(f"Serial {serial_number} - {field_display_name}: Both values are empty")
                        
                    elif sheet_empty and not ls_empty:
                        # Spreadsheet empty, LS has value - update spreadsheet
                        normalized_ls_val = parse_date(ls_val, 'normal') if 'date' in field_ls_name.lower() else str(ls_val)
                        df.at[index, field_display_name] = normalized_ls_val
                        spreadsheet_changes.append(f"Row {index + 1}: Serial {serial_number} - Updated {field_display_name} from empty to '{normalized_ls_val}'\n")
                        logger.info(f"Serial {serial_number} - {field_display_name}: Updated spreadsheet from empty to '{normalized_ls_val}'")
                        
                    elif not sheet_empty and ls_empty:
                        # LS empty, spreadsheet has value - prepare to update LS
                        normalized_sheet_val = parse_date(sheet_val, 'normal') if 'date' in field_ls_name.lower() else str(sheet_val)
                        ls_updates[field_ls_name] = normalized_sheet_val
                        ls_changes.append(f"Serial {serial_number} - {field_display_name}: Will update LS from empty to '{normalized_sheet_val}'\n")
                        logger.info(f"Serial {serial_number} - {field_display_name}: Will update LS from empty to '{normalized_sheet_val}'")
                    
                    elif not sheet_empty and not ls_empty:
                        # Both have values - check if they match
                        if not compare_values(sheet_val, ls_val, field_ls_name):
                            # Values differ - get user input
                            normalized_sheet_val = parse_date(sheet_val, 'normal') if 'date' in field_ls_name.lower() else str(sheet_val)
                            normalized_ls_val = parse_date(ls_val, 'normal') if 'date' in field_ls_name.lower() else str(ls_val)
                            
                            logger.info(f"Serial {serial_number} - {field_display_name}: Conflict Detected")

                            choice = get_user_choice(serial_number, field_display_name, normalized_sheet_val, normalized_ls_val)
                            
                            if choice == 'quit':
                                quit_processing = True
                                logger.info("User chose to quit processing")
                                break
                            elif choice == 'ls_to_sheet':
                                df.at[index, field_display_name] = normalized_ls_val
                                conflict_info.append(f"Override Sheet with LS value: Serial {serial_number} - Updated {field_display_name} from '{normalized_sheet_val}' to '{normalized_ls_val}'\n")
                                spreadsheet_changes.append(f"Row {index + 1}: Serial {serial_number} - Updated {field_display_name} from '{normalized_sheet_val}' to '{normalized_ls_val}'\n")
                                logger.info(f"Serial {serial_number} - {field_display_name}: Updated spreadsheet from '{normalized_sheet_val}' to '{normalized_ls_val}'")
                                
                            elif choice == 'sheet_to_ls':
                                ls_updates[field_ls_name] = normalized_sheet_val
                                conflict_info.append(f"Override LS with Sheet value: Serial {serial_number} - Updated {field_display_name} from '{normalized_ls_val}' to '{normalized_sheet_val}'\n")
                                ls_changes.append(f"Serial {serial_number} - {field_display_name}: Will update LS from '{normalized_ls_val}' to '{normalized_sheet_val}'\n")
                                logger.info(f"Serial {serial_number} - {field_display_name}: Will update LS from '{normalized_ls_val}' to '{normalized_sheet_val}'")
                                    
                            else:  # skip
                                conflict_info.append(f"Serial {serial_number} - {field_display_name}: SKIPPED - Sheet: '{normalized_sheet_val}' vs LS: '{normalized_ls_val}'\n")
                                logger.info(f"Serial {serial_number} - {field_display_name}: SKIPPED - values differ")
                
                # Perform all LS updates for this row in one API call
                if ls_updates and not quit_processing:
                    success = api.update_asset(asset['key'], serial_number, ls_updates)
                    if success:
                        update_summary = ", ".join([f"{k}='{v}'" for k, v in ls_updates.items()])
                        ls_changes.append(f"UPDATED LS for Serial {serial_number}: {update_summary}\n")
                        logger.info(f"Successfully updated LS for Serial {serial_number}: {update_summary}")
                    else:
                        ls_changes.append(f"FAILED to update LS for Serial {serial_number}: {ls_updates}\n")
                        logger.error(f"Failed to update LS for Serial {serial_number}: {ls_updates}")
            if quit_processing:
                discrepancy_file.write(f"\n=== PROCESSING STOPPED BY USER ===\n\n")
        
        # Save spreadsheet changes if any were made
        if missing_info:
            # Log all changes
            with open(DISCREPANCIES_FILE, 'a') as discrepancy_file:
                discrepancy_file.write("\n" + "=" * 40 + "\n")
                discrepancy_file.write("ASSETS WITH MISSING FIELDS:\n")
                discrepancy_file.write("=" * 40 + "\n")
                for missing in missing_info:
                    discrepancy_file.write(missing)
        if spreadsheet_changes:
            logger.info(f"Saving {len(spreadsheet_changes)} changes to spreadsheet...")
            df.to_excel(SPREADSHEET_PATH, index=False)
            
            # Log all changes
            with open(DISCREPANCIES_FILE, 'a') as discrepancy_file:
                discrepancy_file.write("\n\n" + "=" * 40 + "\n")
                discrepancy_file.write("SPREADSHEET CHANGES MADE:\n")
                discrepancy_file.write("=" * 40 + "\n")
                for change in spreadsheet_changes:
                    discrepancy_file.write(change)
        if ls_changes:
            # Log all changes
            with open(DISCREPANCIES_FILE, 'a') as discrepancy_file:
                discrepancy_file.write("\n\n" + "=" * 40 + "\n")
                discrepancy_file.write("LS CHANGES MADE:\n")
                discrepancy_file.write("=" * 40 + "\n")
                for change in ls_changes:
                    discrepancy_file.write(change)
        if conflict_info:
            # Log all changes
            with open(DISCREPANCIES_FILE, 'a') as discrepancy_file:
                discrepancy_file.write("\n\n" + "=" * 40 + "\n")
                discrepancy_file.write("CONFLICTS:\n")
                discrepancy_file.write("=" * 40 + "\n")
                for conflict in conflict_info:
                    discrepancy_file.write(conflict)
                discrepancy_file.write("\n")
        
        logger.info(f"Processing complete. Total API requests made: {api.request_count}")
        if quit_processing:
            logger.info("Processing was stopped by user")
        logger.info(f"Check {DISCREPANCIES_FILE} for results.")
        
    except FileNotFoundError:
        logger.error(f"Spreadsheet file not found: {SPREADSHEET_PATH}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()