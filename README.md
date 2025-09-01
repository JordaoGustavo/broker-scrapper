# Broker Contact Scraper

This Python script scrapes mobile phone contacts from the Broker platform, specifically targeting "TELEFONE MÓVEL" (mobile phone) contacts from residents.

## Features

- **Anti-detection measures**: Random delays between requests, proper browser headers
- **Flexible street scraping**: Configure multiple streets and number ranges
- **Mobile contact filtering**: Only extracts TELEFONE MÓVEL type contacts
- **CSV export**: Clean Excel-compatible CSV output
- **Error handling**: Robust error handling with logging
- **Session management**: Maintains authentication across requests

## Installation

### Prerequisites
- Python 3.8 or higher
- `make` (optional, for using the Makefile)

### Setup with Virtual Environment

1. **Create and activate virtual environment:**
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

2. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

### Quick Setup (using Makefile)
If you have `make` installed, you can use the provided Makefile:
```bash
make setup
```
This will create the virtual environment and install dependencies automatically.

### Alternative: Global Installation
If you prefer not to use a virtual environment:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

1. **Activate virtual environment** (if using venv):
```bash
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows
```

2. **Configure the scraper**:
   - Edit the `streets_to_scrape` list in `scraper.py` with the streets you want to scrape
   - Update the `BEARER_TOKEN` if needed (currently using the one from requests.txt)

3. **Run the scraper**:
```bash
python scraper.py
```

### Makefile Usage
Alternatively, use the provided Makefile commands:

```bash
# Run the scraper
make run

# Run in background (useful for long-running scrapes)
make run-bg

# View logs (if running in background)
make logs

# Stop background process
make stop
```

## Configuration

### Streets Configuration

Modify the `streets_to_scrape` list in `main()`:

```python
streets_to_scrape = [
    {"name": "Rua Tabajaras", "city_id": 4724, "start": 1, "end": 100},
    {"name": "Another Street", "city_id": 1234, "start": 50, "end": 150}
]
```

### Anti-Detection Settings

The scraper includes several anti-detection features:
- **Configurable delays**: Customizable delays between different operations
- **Browser-like headers**: Headers that mimic legitimate browser traffic
- **Session persistence**: Maintains authentication across requests
- **Random delay patterns**: Prevents predictable timing patterns

#### Delay Configuration

The scraper uses different delay types for optimal anti-detection:

```python
delay_config = {
    'search_delay': {'min': 3, 'max': 8},      # Between resident searches
    'contact_delay': {'min': 5, 'max': 12},    # Before getting contact info
    'decrypt_delay': {'min': 3, 'max': 7},     # Before decryption requests
    'range_delay': {'min': 8, 'max': 15}       # Between number ranges
}
```

**Default Delays:**
- **Search Delay**: 3-8 seconds (between individual resident processing)
- **Contact Delay**: 5-12 seconds (before requesting contact information)
- **Decrypt Delay**: 3-7 seconds (before decryption requests)
- **Range Delay**: 8-15 seconds (between different number ranges)

#### Customizing Delays

To adjust delays for your use case:

```python
# More conservative (slower, more stealthy)
delay_config = {
    'search_delay': {'min': 5, 'max': 15},
    'contact_delay': {'min': 8, 'max': 20},
    'decrypt_delay': {'min': 5, 'max': 12},
    'range_delay': {'min': 15, 'max': 30}
}

# Faster (less stealthy, but quicker)
delay_config = {
    'search_delay': {'min': 2, 'max': 5},
    'contact_delay': {'min': 3, 'max': 8},
    'decrypt_delay': {'min': 2, 'max': 4},
    'range_delay': {'min': 5, 'max': 10}
}
```

**⚠️ Warning:** Shorter delays increase the risk of detection and rate limiting. Always prioritize respecting the platform's terms of service.

#### Delay Presets

The project includes ready-made delay configurations in `delay_presets.py`:

```python
from delay_presets import CONSERVATIVE, ULTRA_CONSERVATIVE, FAST

# Use a preset
delay_config = CONSERVATIVE  # Recommended for most cases
# delay_config = ULTRA_CONSERVATIVE  # Maximum stealth
# delay_config = FAST  # Less stealthy, faster

scraper = BrokerScraper(BEARER_TOKEN, delay_config)
```

Available presets:
- **`ULTRA_CONSERVATIVE`**: Maximum stealth (8-40s delays)
- **`CONSERVATIVE`**: Recommended for most cases (5-25s delays)
- **`BALANCED`**: Default settings (3-15s delays)
- **`FAST`**: Less stealthy, faster (2-10s delays)
- **`ULTRA_FAST`**: High risk, fastest (1-6s delays)

## Output

The script generates a `broker_contacts.csv` file with the following columns:

- `street`: Street name
- `number`: House/building number
- `name`: Resident name
- `document`: Document number (CPF/CNPJ)
- `city`: City name
- `neighborhood`: Neighborhood
- `uf`: State (UF)
- `phone_number`: Mobile phone number
- `priority`: Contact priority
- `score`: Contact score
- `plus`: Whether it's a plus number
- `not_disturb`: Do not disturb flag

## Security Considerations

- The script uses the same authentication token from your requests.txt
- Be mindful of rate limiting and platform terms of service
- The delays are designed to be respectful but may need adjustment
- Consider the legal implications of scraping personal contact data

## Troubleshooting

### Authentication Issues
- Verify the bearer token is still valid
- Check if the token has expired (look at the `exp` field in the JWT)

### Rate Limiting
- **Increase delays**: Adjust the `delay_config` in `scraper.py` for longer delays
- **Use conservative settings**: Set delays to 8-15 seconds minimum
- **Reduce batch size**: Decrease the `step` parameter in `scrape_street_range()`
- **Add manual pauses**: Consider adding longer pauses between major operations

### Empty Results
- Verify the street names and city IDs are correct
- Check if the API endpoints have changed
- Ensure the authentication is working properly
- **New Issue**: The API may return empty results even with valid authentication
- Try broader number ranges (e.g., 1-200 instead of 60-78)
- Use the debug script to see detailed API responses: `python debug_scraper.py`
- Test token validity: `python test_token.py`

## API Flow

The scraper follows this 3-step process:

1. **Search**: Find residents in a street number range
2. **Contact Info**: Get encrypted contact information for each resident
3. **Decrypt**: Read the encrypted data to access actual phone numbers
4. **Filter**: Extract only TELEFONE MÓVEL contacts
5. **Export**: Save all results to CSV

## Makefile Commands

The project includes a `Makefile` with the following commands:

| Command | Description |
|---------|-------------|
| `make setup` | Create virtual environment and install dependencies |
| `make install` | Install/update Python dependencies |
| `make run` | Run the scraper |
| `make run-bg` | Run scraper in background |
| `make logs` | View background process logs |
| `make stop` | Stop background scraper process |
| `make clean` | Remove generated files and cache |
| `make clean-all` | Remove venv, generated files, and cache |
| `make help` | Show available commands |

### Examples

```bash
# Initial setup
make setup

# Run scraper
make run

# Run in background for long scrapes
make run-bg
make logs  # View progress
make stop  # Stop when done

# Debug issues
python debug_scraper.py  # Detailed API response logging
python test_token.py     # Test authentication

# Cleanup
make clean
```

## Debugging Tools

The project includes debugging utilities to help troubleshoot issues:

- **`debug_scraper.py`**: Shows detailed API responses at each step
- **`test_token.py`**: Validates authentication token
- **Enhanced logging**: Run with `python scraper.py` for detailed logs

### Common Issues & Solutions

1. **"No residents found"**:
   - Try broader number ranges in `scraper.py`
   - Verify street names and city IDs
   - Use `python debug_scraper.py` to see API responses

2. **"Token expired"**:
   - Run `python test_token.py` to check token validity
   - Update the `BEARER_TOKEN` in `scraper.py` with a fresh token

3. **"Rate limiting"**:
   - Increase delays in the scraper configuration
   - Use the Makefile's background mode for controlled execution
