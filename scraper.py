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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BrokerScraper:
    def __init__(self, bearer_token, delay_config=None):
        self.session = requests.Session()
        self.base_url = "https://api-prd.brokers.eemovel.com.br"
        self.bearer_token = bearer_token

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
        """Scrape a range of street numbers"""
        all_results = []

        for initial in range(start_number, end_number + 1, step):
            final = min(initial + step - 1, end_number)

            # Search for residents in this range
            residents = self.search_residents(street, initial, final, city_id)

            if residents:
                logger.info(f"Found {len(residents)} residents in range {initial}-{final}")

                for resident in residents:
                    # Add delay between requests
                    self.random_delay('search_delay')

                    # Get contact info
                    self.random_delay('contact_delay')
                    contact_info = self.get_contact_info(resident, default_city_id=city_id)

                    if contact_info and 'data' in contact_info:
                        # Read encrypted data
                        print("requesting decrypted data")
                        self.random_delay('decrypt_delay')
                        decrypted_data = self.read_encrypted_data(
                            contact_info['data'],
                            contact_info.get('id', 0)
                        )

                        if decrypted_data:
                            # Extract mobile contacts
                            mobile_contacts = self.extract_mobile_contacts(decrypted_data)

                            # Add street and number info to each contact
                            for contact in mobile_contacts:
                                contact.update({
                                    'street': street,
                                    'number': resident.get('number', ''),
                                    'name': contact.get('pfData', {}).get('name', ''),
                                    'city': resident.get('city', ''),
                                    'neighborhood': resident.get('neighborhood', ''),
                                    'uf': resident.get('uf', '')
                                })
                                all_results.append(contact)

            # Longer delay between search ranges
            self.random_delay('range_delay')

        return all_results

    def validate_and_deduplicate_contacts(self, contacts):
        """Validate and deduplicate contact data"""
        if not contacts:
            return []

        validated_contacts = []
        seen_keys = set()

        for contact in contacts:
            # Skip invalid contacts
            if not contact.get('phone_number'):
                logger.debug("Skipping contact without phone number")
                continue

            # Create a unique key for deduplication
            unique_key = (
                contact.get('street', '').strip(),
                contact.get('number', '').strip(),
                contact.get('phone_number', '').strip(),
                contact.get('document', '').strip()
            )

            # Skip duplicates
            if unique_key in seen_keys:
                logger.debug(f"Skipping duplicate contact: {contact.get('phone_number')}")
                continue

            seen_keys.add(unique_key)

            # Validate and clean data
            validated_contact = {
                'street': contact.get('street', '').strip(),
                'number': contact.get('number', '').strip(),
                'name': contact.get('name', '').strip(),
                'document': contact.get('document', '').strip(),
                'city': contact.get('city', '').strip(),
                'neighborhood': contact.get('neighborhood', '').strip(),
                'uf': contact.get('uf', '').strip(),
                'phone_number': contact.get('phone_number', '').strip(),
                'priority': contact.get('priority', 0),
                'score': contact.get('score', 0),
                'plus': contact.get('plus', False),
                'not_disturb': contact.get('not_disturb', 0)
            }

            # Only add if phone number is valid (has at least 10 digits)
            phone_digits = ''.join(filter(str.isdigit, validated_contact['phone_number']))
            if len(phone_digits) >= 10:
                validated_contacts.append(validated_contact)
            else:
                logger.debug(f"Skipping invalid phone number: {validated_contact['phone_number']}")

        logger.info(f"Validated {len(validated_contacts)} unique contacts from {len(contacts)} raw results")
        return validated_contacts

    def save_to_csv(self, results, filename='broker_contacts.csv'):
        """Save results to CSV file with validation and deduplication"""
        if not results:
            logger.warning("No results to save")
            return

        # Validate and deduplicate results
        validated_results = self.validate_and_deduplicate_contacts(results)

        if not validated_results:
            logger.warning("No valid results after validation")
            return

        fieldnames = [
            'street', 'number', 'name', 'document', 'city', 'neighborhood', 'uf',
            'phone_number', 'priority', 'score', 'plus', 'not_disturb'
        ]

        # Create backup if file exists
        import os
        if os.path.exists(filename):
            backup_name = filename.replace('.csv', '_backup.csv')
            os.rename(filename, backup_name)
            logger.info(f"Created backup: {backup_name}")

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(validated_results)

        logger.info(f"Saved {len(validated_results)} validated contacts to {filename}")
        return len(validated_results)


def main():
    # Bearer token from the requests.txt file
    BEARER_TOKEN = "eyJraWQiOiJEYTNKUDNqVFo0MkxMc0dJcW1RQVROSEtxTWV4TE40NHBlQzhXVG9IVUg4PSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI4ZmY0Y2Q0Yy1hOTJiLTQyM2QtOTY1Yi01ZmNmYWM0NTQ4MzkiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV9OQnQ4Y0JsdXEiLCJjbGllbnRfaWQiOiIxYjRoNGMyczVxMGJmNXBvczEwamlob2JoZiIsIm9yaWdpbl9qdGkiOiIzMGVkZmM0NS0xYjJmLTRjOWItYjg1Mi05Yzg1ZjFlMzM3ZTciLCJldmVudF9pZCI6ImU2NTExYjE2LTdmNDMtNGZhZi1iNDVkLTZhOWViNjViZmE3YyIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXdzLmNvZ25pdG8uc2lnbmluLnVzZXIuYWRtaW4iLCJhdXRoX3RpbWUiOjE3NTY3NjYwMjksImV4cCI6MTc1Njc4NzYyOSwiaWF0IjoxNzU2NzY2MDI5LCJqdGkiOiJmZjczZGE3NS1iZTEwLTQ1NjEtYjEyOC0wNDZmOWFmM2JiMzIiLCJ1c2VybmFtZSI6IjhmZjRjZDRjLWE5MmItNDIzZC05NjViLTVmY2ZhYzQ1NDgzOSJ9.TBwJ4bE7bO_TYnhxVsA_b0MY5LR339X_AYQvpVnEQSHywFEI_hFo5TUBEzumsM11ct6tuGF9PFeW9tlBs7LdeB9YCwdE8mOMP9VtLtqbYuHQ33_SWp3dOvPR3ctDaoTky7EbQv5OKGFFnqyNWm72glSGVdaNmnmptCdPRrWHAmVeAKMABqfRnGPoGcD4s7sMJ9Yjrkgg9dgL6BYyqDbiL8rWur6zNHcxcWZ9PFNPD3vMrnYtZUHI-FrNal0S78kl-IvpOv-ECZxlNxPTBIyMKVDTQPzO6Kzs6Cw4CWt8-rN63xdcVlN5tbVCyARLwtmEHzTrmecgBWfo1rwDpqx6Kw"

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
        #{"name": "Rua Tabajaras", "city_id": 4724, "start": 68, "end": 70},  # Broader range
       {"name": "Rua Susano", "city_id": 5270, "start": 55, "end": 55},
       
    ]

    all_results = []

    for street_config in streets_to_scrape:
        logger.info(f"Starting scrape for {street_config['name']}")
        results = scraper.scrape_street_range(
            street_config['name'],
            street_config['start'],
            street_config['end'],
            street_config['city_id']
        )
        all_results.extend(results)
        logger.info(f"Completed {street_config['name']}, found {len(results)} mobile contacts")

    # Save all results to CSV with validation
    saved_count = scraper.save_to_csv(all_results)

    logger.info("=" * 60)
    logger.info("SCRAPING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total raw contacts collected: {len(all_results)}")
    logger.info(f"Valid contacts after validation: {saved_count or 0}")
    logger.info("Configuration used:")
    logger.info(f"  - Delay preset: {type(delay_config).__name__}")
    logger.info(f"  - Streets processed: {len(streets_to_scrape)}")
    logger.info(f"  - Output file: broker_contacts.csv")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
