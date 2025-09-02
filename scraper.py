#!/usr/bin/env python3
"""
Broker Data Scraper - Extracts mobile phone contacts from residents
"""

import requests
import csv
import time
import random
from urllib.parse import quote
import logging
import os
from datetime import datetime
import json
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_strip(value):
    """Safely strip a value, handling None and non-string types"""
    if value is None:
        return ""
    try:
        return str(value).strip()
    except:
        return ""

class CSVWriterSingleton:
    """Thread-safe CSV writer singleton for incremental data saving"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.initialized = False
        return cls._instance
    
    def __init__(self):
        if not self.initialized:
            self.file_handle = None
            self.csv_writer = None
            self.filename = None
            self.fieldnames = [
                'street', 'number', 'name', 'document', 'city', 'neighborhood', 'uf',
                'phone_number', 'whatsapp_url'
            ]
            self.written_count = 0
            self.seen_keys = set()
            self.initialized = True
    
    def initialize_file(self, base_filename='broker_contacts'):
        """Initialize CSV file with timestamp"""
        if self.file_handle is not None:
            return self.filename
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.filename = f"{base_filename}_{timestamp}.csv"
        
        # Create CSV file with headers
        self.file_handle = open(self.filename, 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.DictWriter(self.file_handle, fieldnames=self.fieldnames)
        self.csv_writer.writeheader()
        self.file_handle.flush()
        
        logger.info(f"Initialized CSV file: {self.filename}")
        
        return self.filename
    
    def format_whatsapp_url(self, phone_number):
        """Format WhatsApp URL with phone number"""
        if not phone_number:
            return ""
        
        # Clean phone number - remove all non-digits
        clean_phone = ''.join(filter(str.isdigit, phone_number))
        
        # Add country code if not present (assuming Brazil +55)
        if len(clean_phone) == 11 and clean_phone.startswith('9'):
            clean_phone = '55' + clean_phone
        elif len(clean_phone) == 10:
            clean_phone = '559' + clean_phone
        elif not clean_phone.startswith('55'):
            clean_phone = '55' + clean_phone
            
        return f"https://api.whatsapp.com/send?phone={clean_phone}"
    
    def write_contact(self, contact_data):
        """Write a single contact to CSV file immediately"""
        if self.file_handle is None:
            self.initialize_file()
        
        # Create unique key for deduplication
        unique_key = (
            safe_strip(contact_data.get('street')),
            safe_strip(contact_data.get('number')),
            safe_strip(contact_data.get('phone_number')),
            safe_strip(contact_data.get('document'))
        )
        
        # Skip duplicates
        if unique_key in self.seen_keys:
            logger.debug(f"Skipping duplicate contact: {contact_data.get('phone_number')}")
            return False
        
        # Validate phone number
        phone_digits = ''.join(filter(str.isdigit, safe_strip(contact_data.get('phone_number'))))
        if len(phone_digits) < 10:
            logger.debug(f"Skipping invalid phone number: {contact_data.get('phone_number')}")
            return False
        
        self.seen_keys.add(unique_key)
        
        # Prepare contact data with WhatsApp URL
        processed_contact = {
            'street': safe_strip(contact_data.get('street')),
            'number': safe_strip(contact_data.get('number')),
            'name': safe_strip(contact_data.get('name')),
            'document': safe_strip(contact_data.get('document')),
            'city': safe_strip(contact_data.get('city')),
            'neighborhood': safe_strip(contact_data.get('neighborhood')),
            'uf': safe_strip(contact_data.get('uf')),
            'phone_number': safe_strip(contact_data.get('phone_number')),
            'whatsapp_url': self.format_whatsapp_url(contact_data.get('phone_number')),
        }
        
        try:
            self.csv_writer.writerow(processed_contact)
            self.file_handle.flush()  # Immediately flush to disk
            self.written_count += 1
            
            logger.info(f"Saved contact #{self.written_count}: {processed_contact['name']} - {processed_contact['phone_number']}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing contact to CSV: {e}")
            return False
    
    def close(self):
        """Close file handle"""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            logger.info(f"Closed CSV file. Total contacts written: {self.written_count}")
    
    def get_stats(self):
        """Get current statistics"""
        return {
            'filename': self.filename,
            'written_count': self.written_count,
            'seen_keys_count': len(self.seen_keys)
        }

class BrokerScraper:
    def __init__(self, bearer_token, delay_config=None):
        self.session = requests.Session()
        self.base_url = "https://api-prd.brokers.eemovel.com.br"
        self.bearer_token = bearer_token
        self.csv_writer = CSVWriterSingleton()
        self.error_count = 0
        self.max_consecutive_errors = 5
        self.processed_ranges = set()  # Track processed ranges for recovery

        # Default delay configuration (more conservative)
        self.default_delays = {
            'search_delay': {'min': 1, 'max': 1},      # Between searches
            'contact_delay': {'min': 1, 'max': 1},    # Between contact info requests
            'decrypt_delay': {'min': 1, 'max': 1},     # Between decryption requests
            'range_delay': {'min': 2, 'max': 6}       # Between search ranges
        }

        # Override with custom config if provided
        if delay_config:
            self.default_delays.update(delay_config)

        # Set up headers to mimic browser
        self.session.headers.update({
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': f'Bearer {self.bearer_token}',
            'content-type': 'application/json',
            'impersonate': 'false',
            'origin': 'https://brokers.eemovel.com.br',
            'priority': 'u=1, i',
            'referer': 'https://brokers.eemovel.com.br/',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36'
        })

    def random_delay(self, delay_type='search_delay'):
        """Add random delay to avoid detection"""
        if delay_type in self.default_delays:
            min_delay = self.default_delays[delay_type]['min']
            max_delay = self.default_delays[delay_type]['max']
        else:
            # Fallback to search delay
            min_delay = self.default_delays['search_delay']['min']
            max_delay = self.default_delays['search_delay']['max']

        delay = random.uniform(min_delay, max_delay)
        logger.debug(f"Sleeping for {delay:.2f} seconds ({delay_type})")
        #   time.sleep(delay)

    def search_residents(self, street, initial_number, final_number, city_id):
        """Search for residents in a street range"""
        url = f"{self.base_url}/brokers/residents/external/search"
        params = {
            'Street': street,
            'InitialNumber': initial_number,
            'FinalNumber': final_number,
            'CityId': city_id
        }

        try:
            logger.info(f"Searching residents for {street} numbers {initial_number}-{final_number}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error searching residents: {e}")
            return []

    def get_contact_info(self, resident_data, default_city_id=None):
        """Get contact info for a resident"""
        url = f"{self.base_url}/brokers/residents/external/contactinfo"
       
        # Prepare the payload
        number_value = resident_data.get("number", "") or resident_data.get("houseNumber")
        try:
            number_str = str(number_value) if number_value is not None else ""
        except Exception:
            number_str = ""

        city_id_value = resident_data.get("cityId") if resident_data else None
        if city_id_value is None:
            city_id_value = default_city_id
        try:
            city_id_normalized = int(city_id_value) if city_id_value is not None else None
        except Exception:
            city_id_normalized = city_id_value

        # Flexible document extraction (API sometimes changes field names)
        document_candidates = [
            "document",
            "documentEncrypted",
            "encryptedDocument",
            "cpfEncrypted",
            "cpf",
            "documento",
            "cpfCnpj",
            "encodedDocument",
            "documentEncryptedData",
        ]
        document_value = None

        # First try direct fields on resident_data
        for key in document_candidates:
            value = resident_data.get(key) if resident_data else None
            if value:
                document_value = value
                break

        # If not found, try to extract from owners array (this is the main structure now)
        if not document_value and resident_data and 'owners' in resident_data:
            owners = resident_data.get('owners', [])
            if owners and len(owners) > 0:
                first_owner = owners[0]
                # Try documentNumber field in owners (this is the primary field)
                document_value = first_owner.get('documentNumber')
                if not document_value:
                    # Try other possible fields in owner
                    for key in document_candidates:
                        value = first_owner.get(key)
                        if value:
                            document_value = value
                            break
        
        if not document_value:
            # Log available keys to help diagnose mapping issues
            logger.error(
                "Resident missing document field. Available keys: %s | resident=%s",
                list(resident_data.keys()) if isinstance(resident_data, dict) else type(resident_data),
                {k: ("[REDACTED]" if k.lower().startswith("doc") else resident_data.get(k)) for k in (resident_data.keys() if isinstance(resident_data, dict) else [])}
            )

        # Extract name from resident data or owners array (owners is the primary source now)
        name_value = resident_data.get("name", "") or resident_data.get("residentName", "")
        if not name_value and resident_data and 'owners' in resident_data:
            owners = resident_data.get('owners', [])
            if owners and len(owners) > 0:
                name_value = owners[0].get('name', '')  # Primary name source
                if not name_value:
                    # Fallback to other name fields if needed
                    name_value = owners[0].get('residentName', '') or owners[0].get('fullName', '')

        # Get documentType from owners if available
        document_type = "CPF"  # Default
        if resident_data and 'owners' in resident_data:
            owners = resident_data.get('owners', [])
            if owners and len(owners) > 0:
                owner_doc_type = owners[0].get('documentType')
                if owner_doc_type:
                    document_type = owner_doc_type

        payload = {
            "document": document_value,
            "documentType": document_type,
            "name": name_value,
            "number": number_str,
            "street": resident_data.get("street", "") or resident_data.get("streetName", ""),
            "uf": resident_data.get("uf", "") or resident_data.get("state", ""),
            "cityId": city_id_normalized,
            "city": resident_data.get("city", "") or resident_data.get("cityName", ""),
            "neighborhood": resident_data.get("neighborhood", "") or resident_data.get("neighborhoodName", "") or resident_data.get("bairro", ""),
            "complement": resident_data.get("complement", ""),
            "type": resident_data.get("type", "proprietario"),
            "detailing": True
        }
        try:
            # Log sanitized payload for debugging
            sanitized = dict(payload)
            if sanitized.get("document"):
                sanitized["document"] = "[REDACTED]"
            logger.debug(f"POST /contactinfo payload: {sanitized}")

            response = self.session.post(url, json=payload)
            if not response.ok:
                logger.error(
                    "contactinfo error %s: %s | payload=%s",
                    response.status_code,
                    response.text,
                    sanitized,
                )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", "?")
            body = getattr(e.response, "text", "")
            logger.error(f"HTTPError getting contact info ({status}): {body}")
            return None
        except requests.RequestException as e:
            logger.error(f"RequestException getting contact info: {e}")
            return None

    def read_encrypted_data(self, encrypted_data, contact_id):
        """Read encrypted contact data"""
        url = f"{self.base_url}/brokers/residents/external/contactinfo/read"

        payload = {
            "data": encrypted_data,
            "id": contact_id
        }

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error reading encrypted data: {e}")
            return None

    def extract_mobile_contacts(self, contact_data):
        """Extract TELEFONE MÓVEL contacts from the response"""
        mobile_contacts = []

        if not contact_data or 'data' not in contact_data:
            return mobile_contacts

        for person in contact_data['data']:
            if 'contactInfos' in person:
                for contact in person['contactInfos']:
                    if contact.get('type') == 'TELEFONE MÓVEL':
                        mobile_contacts.append({
                            'document': person.get('document', ''),
                            'phone_number': contact.get('phoneNumber', ''),
                            'priority': contact.get('priority', 0),
                            'score': contact.get('score', 0),
                            'plus': contact.get('plus', False),
                            'not_disturb': contact.get('notDisturb', 0),
                            'pfData': person.get('pfData', {})
                        })

        return mobile_contacts

    def scrape_street_range(self, street, start_number, end_number, city_id, step=10):
        """Scrape a range of street numbers with comprehensive error handling and incremental saving"""
        # Initialize CSV file if not already done
        self.csv_writer.initialize_file()
        
        total_contacts_saved = 0
        consecutive_errors = 0

        for initial in range(start_number, end_number + 1, step):
            final = min(initial + step - 1, end_number)
            range_key = f"{street}_{initial}_{final}_{city_id}"
            
            # Skip if this range was already processed (for recovery)
            if range_key in self.processed_ranges:
                logger.info(f"Skipping already processed range {initial}-{final}")
                continue

            try:
                logger.info(f"Processing range {initial}-{final} on {street}")
                
                # Search for residents in this range
                residents = self.search_residents(street, initial, final, city_id)
                
                if not residents:
                    logger.info(f"No residents found in range {initial}-{final}")
                    self.processed_ranges.add(range_key)
                    continue

                logger.info(f"Found {len(residents)} residents in range {initial}-{final}")

                for resident_idx, resident in enumerate(residents):
                    try:
                        # Add delay between requests
                        self.random_delay('search_delay')

                        # Get contact info
                        self.random_delay('contact_delay')
                        contact_info = self.get_contact_info(resident, default_city_id=city_id)

                        if contact_info and 'data' in contact_info:
                            # Read encrypted data
                            logger.debug(f"Requesting decrypted data for resident {resident_idx + 1}/{len(residents)}")
                            self.random_delay('decrypt_delay')
                            decrypted_data = self.read_encrypted_data(
                                contact_info['data'],
                                contact_info.get('id', 0)
                            )

                            if decrypted_data:
                                # Extract mobile contacts
                                mobile_contacts = self.extract_mobile_contacts(decrypted_data)

                                # Process each contact immediately
                                for contact in mobile_contacts:
                                    contact_data = {
                                        'street': street,
                                        'number': safe_strip(resident.get('number')),
                                        'name': safe_strip(contact.get('pfData', {}).get('name')),
                                        'document': safe_strip(contact.get('document')),
                                        'city': safe_strip(resident.get('city')),
                                        'neighborhood': safe_strip(resident.get('neighborhood')),
                                        'uf': safe_strip(resident.get('uf')),
                                        'phone_number': safe_strip(contact.get('phone_number')),
                                    }
                                    
                                    # Write contact immediately to CSV
                                    if self.csv_writer.write_contact(contact_data):
                                        total_contacts_saved += 1
                                        consecutive_errors = 0  # Reset error counter on success
                            else:
                                logger.warning(f"Failed to decrypt data for resident in {street} {resident.get('number', '')}")
                        else:
                            logger.warning(f"No contact info found for resident in {street} {resident.get('number', '')}")
                            
                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"Error processing resident {resident_idx + 1} in range {initial}-{final}: {e}")
                        
                        # If too many consecutive errors, stop processing this range
                        if consecutive_errors >= self.max_consecutive_errors:
                            logger.error(f"Too many consecutive errors ({consecutive_errors}), skipping rest of range {initial}-{final}")
                            break
                
                # Mark this range as processed
                self.processed_ranges.add(range_key)
                
                # Longer delay between search ranges
                self.random_delay('range_delay')
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Critical error processing range {initial}-{final}: {e}")
                
                # If too many consecutive errors across ranges, might need to stop entirely
                if consecutive_errors >= self.max_consecutive_errors:
                    logger.error(f"Too many consecutive errors across ranges ({consecutive_errors}), stopping scrape")
                    break
                    
                # Continue to next range on error
                continue

        logger.info(f"Completed street range scraping. Total contacts saved: {total_contacts_saved}")
        return total_contacts_saved

    def cleanup_and_close(self):
        """Clean up resources and close CSV file"""
        try:
            self.csv_writer.close()
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_scraping_stats(self):
        """Get current scraping statistics"""
        csv_stats = self.csv_writer.get_stats()
        return {
            'csv_file': csv_stats.get('filename', 'Not initialized'),
            'contacts_saved': csv_stats.get('written_count', 0),
            'processed_ranges': len(self.processed_ranges),
            'error_count': self.error_count
        }


def main():
    # Bearer token from the requests.txt file
    BEARER_TOKEN = ".qGkOChXP1fwmA0a--eSSgb_UMhP9kvnCmNdTFZCVPZ55haQSbyDPLALBl5p7TKRwnDSvUt0Z--sCYU7H1pdxybtExwa0a_OojjWMF9K5oFDN7D2xCHoYcyMvlnW2Phdky7ISkXEI1VgCPPg64OC6doO_rGB2PNJrVKjdUWtvK29wCs-z8O13CPnUG95XWDf6nZtil2MQav1TLt1A9LUySqGRN5Uu1db8tSXK_wUzI50YF0jv_yLY9cxQ"

    # Delay configuration - customize delays to avoid detection
    # You can import presets from delay_presets.py or customize here
    try:
        from delay_presets import BALANCED
        delay_config = BALANCED  # Default to balanced preset
    except ImportError:
        # Fallback if delay_presets.py is not available
        delay_config = {
            'search_delay': {'min': 1, 'max': 1},      # Between resident searches (3-8 seconds)
            'contact_delay': {'min': 1, 'max': 1},    # Before getting contact info (5-12 seconds)
            'decrypt_delay': {'min': 1, 'max': 1},     # Before decryption (3-7 seconds)
            'range_delay': {'min': 1, 'max': 1}       # Between number ranges (8-15 seconds)
        }

    # Initialize scraper with delay configuration
    scraper = BrokerScraper(BEARER_TOKEN, delay_config)

    # Configuration - you can modify these
    streets_to_scrape = [
        #{"name": "Rua Tabajaras", "city_id": 4724, "start": 70, "end": 70},  # Broader range
       {"name": "Rua Susano", "city_id": 5270, "start": 55, "end": 55},
       
    ]

    total_contacts_saved = 0

    try:
        for street_config in streets_to_scrape:
            logger.info(f"Starting scrape for {street_config['name']}")
            
            try:
                contacts_saved = scraper.scrape_street_range(
                    street_config['name'],
                    street_config['start'],
                    street_config['end'],
                    street_config['city_id']
                )
                total_contacts_saved += contacts_saved
                logger.info(f"Completed {street_config['name']}, saved {contacts_saved} contacts")
                
            except Exception as e:
                logger.error(f"Error processing street {street_config['name']}: {e}")
                logger.info("Continuing with next street...")
                continue

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user. Saving current progress...")
    except Exception as e:
        logger.error(f"Unexpected error during scraping: {e}")
    finally:
        # Always cleanup and close files
        scraper.cleanup_and_close()
        
        # Get final statistics
        stats = scraper.get_scraping_stats()
        
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Output file: {stats['csv_file']}")
        logger.info(f"Total contacts saved: {stats['contacts_saved']}")
        logger.info(f"Processed ranges: {stats['processed_ranges']}")
        logger.info(f"Error count: {stats['error_count']}")
        logger.info("Configuration used:")
        logger.info(f"  - Delay preset: {type(delay_config).__name__}")
        logger.info(f"  - Streets configured: {len(streets_to_scrape)}")
        logger.info("=" * 60)
        
        if stats['contacts_saved'] > 0:
            logger.info(f"✅ Success! Data saved to: {stats['csv_file']}")
        else:
            logger.warning("⚠️  No contacts were saved. Check the logs for errors.")


if __name__ == "__main__":
    main()
