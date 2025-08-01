import requests
import pandas as pd
import json
from datetime import datetime
import logging
import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lansweeper_audit.log'),
        logging.StreamHandler()
    ]
)

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
            "Authorization": f"Bearer {pat_token}",
            "Content-Type": "application/json"
        }
    
    def get_asset_by_serial(self, serial_number: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve asset information by serial number
        
        Args:
            serial_number: The serial number to search for
            
        Returns:
            Asset data dictionary or None if not found
        """
        query = """
        query GetAssetBySerial($siteId: String!, $serialNumber: String!) {
            site(id: $siteId) {
                assetResources(
                    assetPagination: { limit: 1 }
                    filters: {
                        conjunction: AND
                        groups: [{
                            filters: [{
                                path: "assetBasicInfo.serialNumber"
                                operator: EQUAL
                                value: $serialNumber
                            }]
                        }]
                    }
                ) {
                    total
                    items {
                        key
                        assetBasicInfo {
                            name
                            serialNumber
                            barcode
                        }
                        assetCustom {
                            purchaseDate
                            warrantyDate
                        }
                        url
                    }
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
                logging.error(f"GraphQL errors for serial {serial_number}: {data['errors']}")
                return None
            
            assets = data['data']['site']['assetResources']['items']
            if assets:
                return assets[0]
            else:
                logging.warning(f"No asset found with serial number: {serial_number}")
                return None
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for serial {serial_number}: {e}")
            return None
        except (KeyError, TypeError) as e:
            logging.error(f"Unexpected response structure for serial {serial_number}: {e}")
            return None
    
    def update_asset(self, asset_key: str, purchase_date: str = None, warranty_date: str = None) -> bool:
        """
        Update asset information
        
        Args:
            asset_key: The asset key/ID
            purchase_date: Purchase date in YYYY-MM-DD format
            warranty_date: Warranty date in YYYY-MM-DD format
            
        Returns:
            True if successful, False otherwise
        """
        # Build the custom fields update object
        custom_fields = {}
        if purchase_date:
            custom_fields["purchaseDate"] = purchase_date
        if warranty_date:
            custom_fields["warrantyDate"] = warranty_date
        
        if not custom_fields:
            return True  # Nothing to update
        
        mutation = """
        mutation UpdateAsset($siteId: String!, $key: String!, $customFields: AssetCustomInput!) {
            updateAsset(
                siteId: $siteId
                key: $key
                assetCustom: $customFields
            ) {
                key
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
                logging.error(f"Update failed for asset {asset_key}: {data['errors']}")
                return False
            
            logging.info(f"Successfully updated asset {asset_key}")
            return True
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Update request failed for asset {asset_key}: {e}")
            return False

def parse_date(date_str: str) -> Optional[str]:
    """
    Parse date string and return in YYYY-MM-DD format
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        Standardized date string or None if parsing fails
    """
    if pd.isna(date_str) or date_str == '':
        return None
    
    # Try different date formats
    date_formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y/%m/%d',
        '%m-%d-%Y',
        '%d-%m-%Y'
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(str(date_str), fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    logging.warning(f"Could not parse date: {date_str}")
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
        spreadsheet_date = parse_date(spreadsheet_val)
        lansweeper_date = parse_date(lansweeper_val) if lansweeper_val else None
        return spreadsheet_date == lansweeper_date
    
    # For other fields, do string comparison
    return str(spreadsheet_val).strip() == str(lansweeper_val).strip()

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
    
    try:
        # Read the spreadsheet
        logging.info(f"Reading spreadsheet: {SPREADSHEET_PATH}")
        df = pd.read_excel(SPREADSHEET_PATH)
        
        # Verify required columns exist
        required_columns = ['Serial Number', 'Barcode', 'Purchase Date', 'Warranty Date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Open discrepancies file
        with open(DISCREPANCIES_FILE, 'w') as discrepancy_file:
            discrepancy_file.write(f"Asset Discrepancy Report - Generated: {datetime.now()}\n")
            discrepancy_file.write("=" * 80 + "\n\n")
            
            # Process each row
            for index, row in df.iterrows():
                serial_number = row['Serial Number']
                if pd.isna(serial_number):
                    logging.warning(f"Skipping row {index + 1}: No serial number")
                    continue
                
                logging.info(f"Processing serial number: {serial_number}")
                
                # Get asset from Lansweeper
                asset = api.get_asset_by_serial(str(serial_number))
                if not asset:
                    discrepancy_file.write(f"ERROR: Asset not found for serial number: {serial_number}\n\n")
                    continue
                
                # Extract values
                ls_barcode = asset['assetBasicInfo'].get('barcode', '')
                ls_purchase_date = asset['assetCustom'].get('purchaseDate', '') if asset.get('assetCustom') else ''
                ls_warranty_date = asset['assetCustom'].get('warrantyDate', '') if asset.get('assetCustom') else ''
                
                spreadsheet_barcode = row['Barcode']
                spreadsheet_purchase_date = row['Purchase Date']
                spreadsheet_warranty_date = row['Warranty Date']
                
                # Track discrepancies
                discrepancies = []
                
                # Compare barcode
                if not compare_values(spreadsheet_barcode, ls_barcode, 'barcode'):
                    discrepancies.append(f"  Barcode: Spreadsheet='{spreadsheet_barcode}' vs Lansweeper='{ls_barcode}'")
                
                # Compare purchase date
                if not compare_values(spreadsheet_purchase_date, ls_purchase_date, 'purchase_date'):
                    discrepancies.append(f"  Purchase Date: Spreadsheet='{spreadsheet_purchase_date}' vs Lansweeper='{ls_purchase_date}'")
                
                # Compare warranty date
                if not compare_values(spreadsheet_warranty_date, ls_warranty_date, 'warranty_date'):
                    discrepancies.append(f"  Warranty Date: Spreadsheet='{spreadsheet_warranty_date}' vs Lansweeper='{ls_warranty_date}'")
                
                # Log discrepancies
                if discrepancies:
                    discrepancy_file.write(f"Serial Number: {serial_number}\n")
                    for discrepancy in discrepancies:
                        discrepancy_file.write(discrepancy + "\n")
                    discrepancy_file.write("\n")
                
                # Update missing dates in Lansweeper
                needs_update = False
                update_purchase_date = None
                update_warranty_date = None
                
                # Check if purchase date is missing in Lansweeper but available in spreadsheet
                if (not ls_purchase_date or ls_purchase_date == '') and not pd.isna(spreadsheet_purchase_date):
                    parsed_date = parse_date(spreadsheet_purchase_date)
                    if parsed_date:
                        update_purchase_date = parsed_date
                        needs_update = True
                        logging.info(f"Will update purchase date for {serial_number}: {parsed_date}")
                
                # Check if warranty date is missing in Lansweeper but available in spreadsheet
                if (not ls_warranty_date or ls_warranty_date == '') and not pd.isna(spreadsheet_warranty_date):
                    parsed_date = parse_date(spreadsheet_warranty_date)
                    if parsed_date:
                        update_warranty_date = parsed_date
                        needs_update = True
                        logging.info(f"Will update warranty date for {serial_number}: {parsed_date}")
                
                # Perform update if needed
                if needs_update:
                    success = api.update_asset(
                        asset['key'], 
                        update_purchase_date, 
                        update_warranty_date
                    )
                    if success:
                        discrepancy_file.write(f"UPDATED: Serial {serial_number} - Purchase: {update_purchase_date}, Warranty: {update_warranty_date}\n\n")
                    else:
                        discrepancy_file.write(f"UPDATE FAILED: Serial {serial_number}\n\n")
        
        logging.info(f"Processing complete. Check {DISCREPANCIES_FILE} for results.")
        
    except FileNotFoundError:
        logging.error(f"Spreadsheet file not found: {SPREADSHEET_PATH}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()