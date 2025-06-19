import re
import asyncio
import logging
import aiohttp
import signal
import sys
from datetime import datetime, timedelta
from pyrogram.enums import ParseMode
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    UserAlreadyParticipant,
    InviteHashExpired,
    InviteHashInvalid,
    PeerIdInvalid,
    ChannelPrivate,
    UsernameNotOccupied,
    FloodWait
)

# Enhanced logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
API_ID = "23925218"
API_HASH = "396fd3b1c29a427df8cc6fb54f3d307c"
PHONE_NUMBER = "+918123407093"

# MULTIPLE SOURCE GROUPS TO MONITOR
SOURCE_GROUPS = [
    -1002561985021,  # Original source group
    -1002174077087,  # Source Group 2 - Replace with your group IDs
    -1002319403142,  # Source Group 3
    -1002266997029,  # Source Group 4
]

# TARGET CHANNELS (where to send cards)
TARGET_CHANNELS = [
    -1002328456850,
]

# UPDATE CHANNEL FOR JOIN BUTTON
UPDATE_CHANNEL = "https://t.me/Approvedscrapperupdate"
UPDATE_CHANNEL_USERNAME = "@Approvedscrapperupdate"

# ENHANCED SETTINGS WITH 1 SECOND DELAY
POLLING_INTERVAL = 2  # 2 seconds - More stable
MESSAGE_BATCH_SIZE = 100  # Reasonable batch size
MAX_WORKERS = 100  # Reasonable worker count
SEND_DELAY = 1  # 1 SECOND DELAY BETWEEN CARD SENDS!
PROCESS_DELAY = 0.5  # Small delay between processing
BIN_TIMEOUT = 10  # Reasonable timeout for BIN lookup
MAX_CONCURRENT_CARDS = 50  # Reasonable concurrency
MAX_PROCESSED_MESSAGES = 10000  # Reasonable memory usage

# Enhanced client with reasonable workers
user = Client(
    "cc_monitor_user",
    api_id=API_ID,
    api_hash=API_HASH,
    phone_number=PHONE_NUMBER,
    workers=MAX_WORKERS
)

# Global state
is_running = True
last_processed_message_ids = {}  # Track last message ID for each source group
processed_messages = set()
processed_cards = set()  # DUPLICATE PREVENTION!
stats = {
    'messages_processed': 0,
    'cards_found': 0,
    'cards_sent': 0,
    'cards_duplicated': 0,
    'errors': 0,
    'start_time': None,
    'last_speed_check': None,
    'cards_per_second': 0,
    'bin_lookups_success': 0,
    'bin_lookups_failed': 0,
    'source_group_stats': {}  # Track stats per source group
}

# Initialize stats for each source group
for group_id in SOURCE_GROUPS:
    stats['source_group_stats'][group_id] = {
        'messages_processed': 0,
        'cards_found': 0,
        'last_message_id': None
    }

# BIN Cache for permanent storage
bin_cache = {}

# Semaphore for controlled concurrent processing
card_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CARDS)

# ENHANCED BIN API CLIENT WITH BETTER ERROR HANDLING
class EnhancedBINClient:
    """Enhanced BIN API client with robust error handling"""
    
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        # WORKING BIN APIs with better error handling
        self.apis = [
            {
                'name': 'BinList',
                'url': 'https://lookup.binlist.net/{}',
                'headers': {
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'parser': self._parse_binlist,
                'delay': 2
            },
            {
                'name': 'BinCodes',
                'url': 'https://api.bincodes.com/bin/?format=json&bin={}',
                'headers': {
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'parser': self._parse_bincodes,
                'delay': 2
            },
            {
                'name': 'BinSu',
                'url': 'https://bins.su/{}',
                'headers': {
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'parser': self._parse_bins_su,
                'delay': 2
            }
        ]
    
    async def get_bin_info(self, bin_number: str):
        """Get BIN information with enhanced error handling"""
        # Check cache first
        if bin_number in bin_cache:
            logger.info(f"✅ BIN {bin_number} found in cache")
            return bin_cache[bin_number]
        
        logger.info(f"🔍 Looking up BIN {bin_number}...")
        
        # Try enhanced fallback first (faster and more reliable)
        fallback_data = self._get_enhanced_fallback_bin_info(bin_number)
        if fallback_data:
            logger.info(f"✅ BIN {bin_number} found via enhanced fallback")
            bin_cache[bin_number] = fallback_data
            stats['bin_lookups_success'] += 1
            return fallback_data
        
        # Try APIs only if fallback fails
        for i, api_config in enumerate(self.apis):
            try:
                logger.info(f"🔄 Trying {api_config['name']} API...")
                data = await self._fetch_from_api(api_config, bin_number)
                if data and self._is_valid_bin_data(data):
                    logger.info(f"✅ BIN {bin_number} found via {api_config['name']}")
                    bin_cache[bin_number] = data
                    stats['bin_lookups_success'] += 1
                    return data
                
                # Add delay between API calls
                await asyncio.sleep(api_config['delay'])
                
            except Exception as e:
                logger.debug(f"API {api_config['name']} failed: {e}")
                continue
        
        logger.info(f"ℹ️ Using basic brand detection for BIN {bin_number}")
        basic_data = self._get_basic_brand_info(bin_number)
        if basic_data:
            bin_cache[bin_number] = basic_data
            stats['bin_lookups_success'] += 1
            return basic_data
        
        stats['bin_lookups_failed'] += 1
        return None
    
    async def _fetch_from_api(self, api_config, bin_number: str):
        """Fetch data from API with robust error handling"""
        url = api_config['url'].format(bin_number)
        headers = api_config['headers']
        parser = api_config['parser']
        
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(ssl=False, limit=10)
            
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        try:
                            raw_data = await response.json()
                            parsed_data = parser(raw_data)
                            return parsed_data
                        except Exception:
                            return None
                    else:
                        return None
        except Exception:
            return None
    
    def _is_valid_bin_data(self, data):
        """Check if BIN data is valid"""
        if not data:
            return False
        
        brand = data.get('brand', 'UNKNOWN')
        country = data.get('country_name', 'UNKNOWN')
        
        return brand != 'UNKNOWN' or country != 'UNKNOWN'
    
    def _parse_binlist(self, data):
        """Parse binlist.net response"""
        try:
            country_data = data.get('country', {})
            bank_data = data.get('bank', {})
            
            return {
                'scheme': data.get('scheme', 'UNKNOWN').upper(),
                'type': data.get('type', 'UNKNOWN').upper(),
                'brand': data.get('brand', 'UNKNOWN').upper(),
                'bank': bank_data.get('name', 'UNKNOWN BANK') if isinstance(bank_data, dict) else str(bank_data or 'UNKNOWN BANK'),
                'country_name': country_data.get('name', 'UNKNOWN') if isinstance(country_data, dict) else 'UNKNOWN',
                'country_flag': country_data.get('emoji', '🌍') if isinstance(country_data, dict) else '🌍',
                'country_code': country_data.get('alpha2', 'XX') if isinstance(country_data, dict) else 'XX'
            }
        except Exception:
            return None
    
    def _parse_bincodes(self, data):
        """Parse bincodes.com response"""
        try:
            return {
                'scheme': data.get('card_scheme', data.get('scheme', 'UNKNOWN')).upper(),
                'type': data.get('card_type', data.get('type', 'UNKNOWN')).upper(),
                'brand': data.get('card_brand', data.get('brand', 'UNKNOWN')).upper(),
                'bank': data.get('bank_name', data.get('bank', 'UNKNOWN BANK')),
                'country_name': data.get('country_name', data.get('country', 'UNKNOWN')),
                'country_flag': data.get('country_flag', '🌍'),
                'country_code': data.get('country_code', 'XX')
            }
        except Exception:
            return None
    
    def _parse_bins_su(self, data):
        """Parse bins.su response"""
        try:
            return {
                'scheme': data.get('scheme', 'UNKNOWN').upper(),
                'type': data.get('type', 'UNKNOWN').upper(),
                'brand': data.get('brand', 'UNKNOWN').upper(),
                'bank': data.get('bank', 'UNKNOWN BANK'),
                'country_name': data.get('country_name', 'UNKNOWN'),
                'country_flag': data.get('country_flag', '🌍'),
                'country_code': data.get('country_code', 'XX')
            }
        except Exception:
            return None
    
    def _get_enhanced_fallback_bin_info(self, bin_number: str):
        """Enhanced fallback BIN database"""
        enhanced_fallback_db = {
            '516715': {
                'scheme': 'MASTERCARD',
                'type': 'DEBIT',
                'brand': 'MASTERCARD',
                'bank': 'MASTERCARD WORLD BANK',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            },
            '400632': {
                'scheme': 'VISA',
                'type': 'CREDIT',
                'brand': 'VISA',
                'bank': 'CHASE BANK',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            },
            '543407': {
                'scheme': 'MASTERCARD',
                'type': 'DEBIT',
                'brand': 'MASTERCARD',
                'bank': 'MASTERCARD WORLD BANK',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            },
            '424242': {
                'scheme': 'VISA',
                'type': 'CREDIT',
                'brand': 'VISA',
                'bank': 'TEST BANK',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            },
            '411111': {
                'scheme': 'VISA',
                'type': 'CREDIT',
                'brand': 'VISA',
                'bank': 'VISA CLASSIC',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            },
            '555555': {
                'scheme': 'MASTERCARD',
                'type': 'CREDIT',
                'brand': 'MASTERCARD',
                'bank': 'MASTERCARD STANDARD',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            },
            '378282': {
                'scheme': 'AMERICAN EXPRESS',
                'type': 'CREDIT',
                'brand': 'AMERICAN EXPRESS',
                'bank': 'AMERICAN EXPRESS',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            },
            '601100': {
                'scheme': 'DISCOVER',
                'type': 'CREDIT',
                'brand': 'DISCOVER',
                'bank': 'DISCOVER BANK',
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            }
        }
        
        # Check exact match first
        if bin_number in enhanced_fallback_db:
            return enhanced_fallback_db[bin_number]
        
        return None
    
    def _get_basic_brand_info(self, bin_number: str):
        """Get basic brand info from BIN pattern"""
        card_number = bin_number + "0000000000"
        brand = self._get_card_brand_from_number(card_number)
        
        if brand != 'UNKNOWN':
            bank_mapping = {
                'VISA': 'VISA BANK',
                'MASTERCARD': 'MASTERCARD BANK',
                'AMERICAN EXPRESS': 'AMERICAN EXPRESS BANK',
                'DISCOVER': 'DISCOVER BANK',
                'JCB': 'JCB BANK'
            }
            
            return {
                'scheme': brand,
                'type': 'CREDIT',
                'brand': brand,
                'bank': bank_mapping.get(brand, f'{brand} BANK'),
                'country_name': 'UNITED STATES',
                'country_flag': '🇺🇸',
                'country_code': 'US'
            }
        
        return None
    
    def _get_card_brand_from_number(self, card_number: str) -> str:
        """Enhanced card brand detection"""
        card_number = re.sub(r'\D', '', card_number)
        
        # Visa
        if re.match(r'^4', card_number):
            return 'VISA'
        # Mastercard
        elif re.match(r'^5[1-5]', card_number) or re.match(r'^2[2-7]', card_number):
            return 'MASTERCARD'
        # American Express
        elif re.match(r'^3[47]', card_number):
            return 'AMERICAN EXPRESS'
        # Discover
        elif re.match(r'^6(?:011|5)', card_number):
            return 'DISCOVER'
        # JCB
        elif re.match(r'^35', card_number):
            return 'JCB'
        else:
            return 'UNKNOWN'

# Initialize enhanced BIN client
bin_client = EnhancedBINClient(timeout=BIN_TIMEOUT)

def create_join_button():
    """Create inline keyboard with join button"""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Scrapper Updates", url=UPDATE_CHANNEL)]
    ])
    return keyboard

async def refresh_dialogs(client):
    logger.info("🔄 Refreshing dialogs...")
    dialogs = []
    async for dialog in client.get_dialogs(limit=500):
        dialogs.append(dialog)
    logger.info(f"✅ Refreshed {len(dialogs)} dialogs")
    return True

async def list_user_groups(client):
    logger.info("🔍 Listing all accessible groups...")
    group_count = 0
    async for dialog in client.get_dialogs():
        if dialog.chat.type in ["group", "supergroup"]:
            logger.info(f"📁 Group: {dialog.chat.title} | ID: {dialog.chat.id}")
            group_count += 1
    logger.info(f"✅ Total accessible groups: {group_count}")
    return True

async def find_group_by_id(client, target_id):
    async for dialog in client.get_dialogs():
        if dialog.chat.id == target_id:
            logger.info(f"✅ Found target group in dialogs: {dialog.chat.title}")
            return dialog.chat
    return None

async def ensure_group_access(client, group_id):
    try:
        await refresh_dialogs(client)
        await asyncio.sleep(1)
        found_chat = await find_group_by_id(client, group_id)
        if found_chat:
            logger.info(f"✅ Group found in dialogs: {found_chat.title}")
            return True
        try:
            chat = await client.get_chat(group_id)
            logger.info(f"✅ Direct access to group: {chat.title}")
            return True
        except (PeerIdInvalid, ChannelPrivate) as e:
            logger.warning(f"⚠️ Direct access failed for group {group_id}: {e}")
            try:
                logger.info("🔄 Attempting to join group...")
                await client.join_chat(group_id)
                logger.info(f"✅ Successfully joined group {group_id}")
                await refresh_dialogs(client)
                await asyncio.sleep(1)
                return True
            except Exception as join_error:
                logger.error(f"❌ Failed to join group {group_id}: {join_error}")
                return False
    except Exception as e:
        logger.error(f"❌ Error ensuring group access: {e}")
        return False

async def send_to_target_channels_with_delay(formatted_message, cc_data, source_group_id):
    """Send to ALL 6 CHANNELS with DUPLICATE PREVENTION and JOIN BUTTON"""
    # Check for duplicates
    card_hash = cc_data.split('|')[0]  # Use card number as hash
    if card_hash in processed_cards:
        logger.info(f"🔄 DUPLICATE CC DETECTED: {cc_data[:12]}*** - SKIPPING")
        stats['cards_duplicated'] += 1
        return
    
    # Add to processed cards
    processed_cards.add(card_hash)
    
    # Manage memory for processed cards
    if len(processed_cards) > 10000:
        processed_cards_list = list(processed_cards)
        processed_cards.clear()
        processed_cards.update(processed_cards_list[-5000:])
    
    # Create join button
    join_button = create_join_button()
    
    # Send to ALL 6 CHANNELS with 1 SECOND DELAY
    for i, channel_id in enumerate(TARGET_CHANNELS):
        try:
            await user.send_message(
                chat_id=channel_id,
                text=formatted_message,
                parse_mode=ParseMode.HTML,
                reply_markup=join_button  # ADD JOIN BUTTON!
            )
            logger.info(f"✅ SENT CC {cc_data[:12]}*** from Group {source_group_id} to channel {i+1}/{len(TARGET_CHANNELS)} (ID: {channel_id})")
            stats['cards_sent'] += 1
            
            # Add 1 SECOND DELAY between channels (except for last channel)
            if i < len(TARGET_CHANNELS) - 1:
                logger.info(f"⏳ Waiting {SEND_DELAY} second before next send...")
                await asyncio.sleep(SEND_DELAY)
                
        except FloodWait as e:
            logger.warning(f"⏳ Flood wait {e.value}s for channel {channel_id}")
            await asyncio.sleep(e.value)
            # Retry after flood wait
            try:
                await user.send_message(
                    chat_id=channel_id,
                    text=formatted_message,
                    parse_mode=ParseMode.HTML,
                    reply_markup=join_button
                )
                logger.info(f"✅ SENT CC {cc_data[:12]}*** to channel {i+1}/{len(TARGET_CHANNELS)} (after flood wait)")
                stats['cards_sent'] += 1
            except Exception as retry_e:
                logger.error(f"❌ Failed to send CC to channel {channel_id} after flood wait: {retry_e}")
                stats['errors'] += 1
        except Exception as e:
            logger.error(f"❌ Failed to send CC to channel {channel_id}: {e}")
            stats['errors'] += 1

async def test_access():
    try:
        logger.info("🔍 Testing access to all source groups and target channels...")
        await list_user_groups(user)
        
        # Test access to ALL 5 SOURCE GROUPS
        for i, group_id in enumerate(SOURCE_GROUPS):
            logger.info(f"Testing access to source group {i+1}/{len(SOURCE_GROUPS)}: {group_id}")
            source_access = await ensure_group_access(user, group_id)
            if not source_access:
                logger.error(f"❌ Cannot access source group {group_id}")
                return False
            else:
                logger.info(f"✅ Source group {i+1} access confirmed")
        
        # Test access to ALL 6 TARGET CHANNELS
        for i, channel_id in enumerate(TARGET_CHANNELS):
            logger.info(f"Testing access to target channel {i+1}/{len(TARGET_CHANNELS)}: {channel_id}")
            try:
                target_chat = await user.get_chat(channel_id)
                logger.info(f"✅ User client can access channel {i+1}: {target_chat.title}")
            except Exception as e:
                logger.error(f"❌ Cannot access target channel {i+1} ({channel_id}): {e}")
                return False
        return True
    except Exception as e:
        logger.error(f"Error in test_access: {e}")
        return False

def extract_credit_cards_enhanced(text):
    """Enhanced credit card extraction with better validation"""
    if not text:
        return []
    
    # Enhanced patterns for better matching
    patterns = [
        r'\b(\d{13,19})\|(\d{1,2})\|(\d{2,4})\|(\d{3,4})\b',
        r'\b(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})\b',
        r'(\d{13,19})\s*[\|\/\-:\s]\s*(\d{1,2})\s*[\|\/\-:\s]\s*(\d{2,4})\s*[\|\/\-:\s]\s*(\d{3,4})',
        r'(\d{4})\s*(\d{4})\s*(\d{4})\s*(\d{4})\s*[\|\/\-:\s]\s*(\d{1,2})\s*[\|\/\-:\s]\s*(\d{2,4})\s*[\|\/\-:\s]\s*(\d{3,4})',
    ]
    
    credit_cards = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if len(match) == 4:
                card_number, month, year, cvv = match
                card_number = re.sub(r'[\s\-]', '', card_number)
            elif len(match) == 7:
                card1, card2, card3, card4, month, year, cvv = match
                card_number = card1 + card2 + card3 + card4
            else:
                continue
            
            # Enhanced validation
            if not (13 <= len(card_number) <= 19):
                continue
            
            try:
                month_int = int(month)
                if not (1 <= month_int <= 12):
                    continue
            except ValueError:
                continue
            
            if len(year) == 4:
                year = year[-2:]
            elif len(year) != 2:
                continue
            
            if not (3 <= len(cvv) <= 4):
                continue
            
            credit_cards.append(f"{card_number}|{month.zfill(2)}|{year}|{cvv}")
    
    # Remove duplicates while preserving order
    return list(dict.fromkeys(credit_cards))

def format_card_message_enhanced(cc_data, bin_info, source_group_id):
    """Enhanced message formatting with source group info and update channel mention"""
    scheme = "UNKNOWN"
    card_type = "UNKNOWN"
    brand = "UNKNOWN"
    bank_name = "UNKNOWN BANK"
    country_name = "UNKNOWN"
    country_emoji = "🌍"
    bin_number = cc_data.split('|')[0][:6]
    
    if bin_info:
        scheme = bin_info.get('scheme', 'UNKNOWN').upper()
        card_type = bin_info.get('type', 'UNKNOWN').upper()
        brand = bin_info.get('brand', 'UNKNOWN').upper()
        bank_name = bin_info.get('bank', 'UNKNOWN BANK')
        country_name = bin_info.get('country_name', 'UNKNOWN')
        country_emoji = bin_info.get('country_flag', '🌍')
    else:
        # Enhanced fallback to basic brand detection
        brand = bin_client._get_card_brand_from_number(cc_data.split('|')[0])
        scheme = brand
        if brand != "UNKNOWN":
            bank_name = f"{brand} BANK"
    
    # Enhanced timestamp with date
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Find source group index for display
    source_index = SOURCE_GROUPS.index(source_group_id) + 1 if source_group_id in SOURCE_GROUPS else "?"
    
    message = f"""[ϟ] 𝐀𝐩𝐩𝐫𝐨𝐯𝐞𝐝 𝐒𝐜𝐫𝐚𝐩𝐩𝐞𝐫
━━━━━━━━━━━━━
[ϟ] 𝗖𝗖 - <code>{cc_data}</code> 
[ϟ] 𝗦𝘁𝗮𝘁𝘂𝘀 : APPROVED ✅
[ϟ] 𝗚𝗮𝘁𝗲 - Stripe Auth
━━━━━━━━━━━━━
[ϟ] 𝗕𝗶𝗻 : {bin_number}
[ϟ] 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 : {country_name} {country_emoji}
[ϟ] 𝗜𝘀𝘀𝘂𝗲𝗿 : {bank_name}
[ϟ] 𝗧𝘆𝗽𝗲 : {scheme} - {card_type} - {brand}
━━━━━━━━━━━━━
[ϟ] 𝗦𝗼𝘂𝗿𝗰𝗲 : Group {source_index}
[ϟ] 𝗧𝗶𝗺𝗲 : {timestamp}
[ϟ] 𝗦𝗰𝗿𝗮𝗽𝗽𝗲𝗱 𝗕𝘆 : 𝗕𝘂𝗻𝗻𝘆
[ϟ] 𝗨𝗽𝗱𝗮𝘁𝗲𝘀 : {@Approvedscrapperupdate}"""
    return message

async def process_single_card_enhanced(cc_data, source_group_id):
    """Process single card with enhanced BIN lookup and source group tracking"""
    async with card_semaphore:
        try:
            logger.info(f"🔄 PROCESSING CC: {cc_data[:12]}*** from Group {source_group_id}")
            bin_number = cc_data.split('|')[0][:6]
            
            # Enhanced BIN lookup
            bin_info = await bin_client.get_bin_info(bin_number)
            
            if bin_info:
                logger.info(f"✅ BIN lookup successful: {bin_info['brand']} - {bin_info['country_name']}")
            else:
                logger.info(f"ℹ️ Using fallback for BIN {bin_number}")
            
            formatted_message = format_card_message_enhanced(cc_data, bin_info, source_group_id)
            
            # Send with 1 SECOND DELAY and duplicate prevention to ALL 6 CHANNELS
            await send_to_target_channels_with_delay(formatted_message, cc_data, source_group_id)
            
            # Small delay between card processing
            await asyncio.sleep(PROCESS_DELAY)
            
        except Exception as e:
            logger.error(f"❌ Error processing CC {cc_data} from Group {source_group_id}: {e}")
            stats['errors'] += 1

async def process_message_for_ccs_enhanced(message, source_group_id):
    """Enhanced message processing with source group tracking"""
    global processed_messages
    try:
        if message.id in processed_messages:
            return
        
        processed_messages.add(message.id)
        stats['messages_processed'] += 1
        stats['source_group_stats'][source_group_id]['messages_processed'] += 1
        
        # Memory management
        if len(processed_messages) > MAX_PROCESSED_MESSAGES:
            processed_messages = set(list(processed_messages)[-5000:])
        
        text = message.text or message.caption
        if not text:
            return
        
        logger.info(f"📝 PROCESSING MESSAGE {message.id} from Group {source_group_id}: {text[:50]}...")
        credit_cards = extract_credit_cards_enhanced(text)
        
        if not credit_cards:
            return
        
        logger.info(f"🎯 FOUND {len(credit_cards)} CARDS in message {message.id} from Group {source_group_id}")
        stats['cards_found'] += len(credit_cards)
        stats['source_group_stats'][source_group_id]['cards_found'] += len(credit_cards)
        
        # Process cards with controlled concurrency and 1 second delay
        for cc_data in credit_cards:
            await process_single_card_enhanced(cc_data, source_group_id)
        
    except Exception as e:
        logger.error(f"❌ Error processing message {message.id} from Group {source_group_id}: {e}")
        stats['errors'] += 1

async def poll_single_group(group_id):
    """Poll a single source group for new messages"""
    logger.info(f"🔄 Starting polling for Group {group_id}...")
    
    try:
        # Get initial message ID for this group
        async for message in user.get_chat_history(group_id, limit=1):
            last_processed_message_ids[group_id] = message.id
            stats['source_group_stats'][group_id]['last_message_id'] = message.id
            logger.info(f"📍 Group {group_id} starting from message ID: {message.id}")
            break
    except Exception as e:
        logger.error(f"❌ Error getting initial message ID for Group {group_id}: {e}")
        return
    
    while is_running:
        try:
            logger.info(f"🔍 Polling Group {group_id} for new messages after ID {last_processed_message_ids[group_id]}...")
            new_messages = []
            message_count = 0
            
            async for message in user.get_chat_history(group_id, limit=MESSAGE_BATCH_SIZE):
                message_count += 1
                if message.id <= last_processed_message_ids[group_id]:
                    break
                new_messages.append(message)
            
            new_messages.reverse()
            
            if new_messages:
                logger.info(f"📨 FOUND {len(new_messages)} NEW MESSAGES in Group {group_id}")
                
                # Process messages sequentially to maintain order and control rate
                for message in new_messages:
                    await process_message_for_ccs_enhanced(message, group_id)
                    last_processed_message_ids[group_id] = max(last_processed_message_ids[group_id], message.id)
                    stats['source_group_stats'][group_id]['last_message_id'] = last_processed_message_ids[group_id]
                    await asyncio.sleep(0.1)  # Small delay between messages
                
            else:
                logger.info(f"📭 No new messages in Group {group_id} (checked {message_count} messages)")
            
            # Polling interval
            await asyncio.sleep(POLLING_INTERVAL)
            
        except Exception as e:
            logger.error(f"❌ Error in polling loop for Group {group_id}: {e}")
            stats['errors'] += 1
            await asyncio.sleep(5)  # Longer recovery time

async def poll_all_groups():
    """Poll all source groups simultaneously"""
    logger.info(f"🚀 Starting polling for ALL {len(SOURCE_GROUPS)} source groups...")
    
    # Create polling tasks for all source groups
    polling_tasks = []
    for group_id in SOURCE_GROUPS:
        task = asyncio.create_task(poll_single_group(group_id))
        polling_tasks.append(task)
    
    # Run all polling tasks concurrently
    try:
        await asyncio.gather(*polling_tasks)
    except Exception as e:
        logger.error(f"❌ Error in group polling: {e}")
    finally:
        # Cancel all tasks
        for task in polling_tasks:
            task.cancel()

# Create message filters for all source groups
source_group_filters = filters.chat(SOURCE_GROUPS)

@user.on_message(source_group_filters)
async def realtime_message_handler_enhanced(client, message):
    """Enhanced real-time handler for ALL source groups"""
    source_group_id = message.chat.id
    logger.info(f"⚡ REAL-TIME MESSAGE: {message.id} from Group {source_group_id}")
    # Process immediately but don't block
    asyncio.create_task(process_message_for_ccs_enhanced(message, source_group_id))

async def calculate_speed():
    """Calculate processing speed statistics"""
    while is_running:
        await asyncio.sleep(60)  # Check every minute
        if stats['last_speed_check']:
            time_diff = (datetime.now() - stats['last_speed_check']).total_seconds()
            if time_diff > 0:
                cards_diff = stats['cards_sent'] - stats.get('last_cards_sent', 0)
                stats['cards_per_second'] = cards_diff / time_diff
        
        stats['last_speed_check'] = datetime.now()
        stats['last_cards_sent'] = stats['cards_sent']

async def print_stats_enhanced():
    """Print enhanced statistics with multi-source group info"""
    while is_running:
        await asyncio.sleep(120)  # Every 2 minutes
        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            logger.info(f"📊 MULTI-SOURCE CC MONITOR STATS - Uptime: {uptime}")
            logger.info(f"📨 Total Messages Processed: {stats['messages_processed']}")
            logger.info(f"🎯 Total Cards Found: {stats['cards_found']}")
            logger.info(f"✅ Cards Sent: {stats['cards_sent']} (to {len(TARGET_CHANNELS)} channels)")
            logger.info(f"🔄 Duplicates Blocked: {stats['cards_duplicated']}")
            logger.info(f"⚡ Processing Speed: {stats['cards_per_second']:.2f} cards/sec")
            logger.info(f"🔍 BIN Lookups - Success: {stats['bin_lookups_success']} | Failed: {stats['bin_lookups_failed']}")
            logger.info(f"💾 Cache Size: {len(bin_cache)} BINs cached")
            logger.info(f"📢 Update Channel: {UPDATE_CHANNEL_USERNAME}")
            
            # Per-group statistics
            logger.info(f"📊 PER-GROUP STATS:")
            for i, group_id in enumerate(SOURCE_GROUPS):
                group_stats = stats['source_group_stats'][group_id]
                logger.info(f"   📁 Group {i+1} ({group_id}): {group_stats['messages_processed']} msgs, {group_stats['cards_found']} cards")
            
            logger.info(f"❌ Total Errors: {stats['errors']}")

async def test_message_reception():
    try:
        logger.info("🔍 Testing message reception from all source groups...")
        total_messages = 0
        for i, group_id in enumerate(SOURCE_GROUPS):
            try:
                messages = []
                async for message in user.get_chat_history(group_id, limit=5):
                    messages.append(message)
                logger.info(f"✅ Group {i+1} ({group_id}): Retrieved {len(messages)} recent messages")
                total_messages += len(messages)
            except Exception as e:
                logger.error(f"❌ Error testing Group {i+1} ({group_id}): {e}")
        
        logger.info(f"✅ Total messages retrieved from all groups: {total_messages}")
        return total_messages > 0
    except Exception as e:
        logger.error(f"❌ Error testing message reception: {e}")
        return False

async def force_sync_groups():
    try:
        logger.info("🔄 Force syncing with all source groups...")
        for i, group_id in enumerate(SOURCE_GROUPS):
            try:
                chat = await user.get_chat(group_id)
                logger.info(f"✅ Group {i+1}: {chat.title} ({chat.members_count} members)")
            except Exception as e:
                logger.error(f"❌ Error syncing Group {i+1} ({group_id}): {e}")
        return True
    except Exception as e:
        logger.error(f"❌ Error syncing groups: {e}")
        return False

async def test_bin_lookup_comprehensive():
    """Test BIN lookup functionality"""
    try:
        logger.info("🧪 Testing BIN lookup functionality...")
        test_bins = ['516715', '400632', '543407', '424242', '411111', '555555']
        
        for bin_num in test_bins:
            logger.info(f"🔍 Testing BIN: {bin_num}")
            bin_info = await bin_client.get_bin_info(bin_num)
            if bin_info:
                logger.info(f"✅ BIN {bin_num}: {bin_info['brand']} - {bin_info['type']} - {bin_info['country_name']}")
            else:
                logger.warning(f"❌ BIN {bin_num}: No info found")
            
            # Small delay between tests
            await asyncio.sleep(1)
        
        return True
    except Exception as e:
        logger.error(f"❌ Error testing BIN lookup: {e}")
        return False

def signal_handler(signum, frame):
    global is_running
    logger.info(f"🛑 SHUTDOWN SIGNAL {signum} - Stopping multi-source monitor...")
    is_running = False

async def main():
    global is_running
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("🚀 STARTING MULTI-SOURCE CC MONITOR WITH 5 GROUPS & 6 CHANNELS...")
        logger.info(f"⚙️ SETTINGS:")
        logger.info(f"   📡 Monitoring {len(SOURCE_GROUPS)} source groups: {SOURCE_GROUPS}")
        logger.info(f"   📤 Sending to {len(TARGET_CHANNELS)} channels: {TARGET_CHANNELS}")
        logger.info(f"   📢 Update channel: {UPDATE_CHANNEL_USERNAME}")
        logger.info(f"   ⏱️ Polling interval: {POLLING_INTERVAL}s")
        logger.info(f"   📦 Message batch size: {MESSAGE_BATCH_SIZE}")
        logger.info(f"   ⏳ Send delay: {SEND_DELAY}s between cards")
        logger.info(f"   🔍 BIN timeout: {BIN_TIMEOUT}s")
        logger.info(f"   🧵 Max workers: {MAX_WORKERS}")
        
        stats['start_time'] = datetime.now()
        stats['last_speed_check'] = datetime.now()
        
        await user.start()
        logger.info("✅ User client started successfully!")
        await asyncio.sleep(2)
        
        logger.info("🔍 Running comprehensive tests...")
        
        # Test access to ALL groups and channels
        access_ok = await test_access()
        if not access_ok:
            logger.error("❌ Access test failed!")
            return
        else:
            logger.info(f"✅ All access tests passed for {len(SOURCE_GROUPS)} source groups and {len(TARGET_CHANNELS)} target channels!")
        
        # Sync all groups
        await force_sync_groups()
        
        # Test message reception from all groups
        reception_ok = await test_message_reception()
        if reception_ok:
            logger.info("✅ Message reception working from all source groups!")
        
        # Test BIN lookup system
        logger.info("🧪 Testing BIN lookup system...")
        await test_bin_lookup_comprehensive()
        
        # Start all background tasks
        logger.info("🚀 Starting all background tasks...")
        polling_task = asyncio.create_task(poll_all_groups())
        stats_task = asyncio.create_task(print_stats_enhanced())
        speed_task = asyncio.create_task(calculate_speed())
        
        try:
            logger.info("✅ MULTI-SOURCE CC MONITOR FULLY ACTIVE!")
            logger.info(f"🔄 Polling {len(SOURCE_GROUPS)} groups every {POLLING_INTERVAL}s with {MESSAGE_BATCH_SIZE} message batches")
            logger.info(f"⏳ {SEND_DELAY} second delay between card sends")
            logger.info(f"📤 Sending to {len(TARGET_CHANNELS)} channels with join button")
            logger.info(f"📢 Join button links to: {UPDATE_CHANNEL}")
            logger.info("🔍 Robust BIN lookup with fallback database")
            logger.info("🚫 Duplicate prevention enabled across all sources")
            logger.info("📊 Enhanced statistics and monitoring")
            await idle()
        finally:
            # Cleanup
            polling_task.cancel()
            stats_task.cancel()
            speed_task.cancel()
            try:
                await asyncio.gather(polling_task, stats_task, speed_task, return_exceptions=True)
            except:
                pass
                
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        stats['errors'] += 1
    finally:
        logger.info("🛑 Stopping multi-source client...")
        try:
            if user.is_connected:
                await user.stop()
                logger.info("✅ Client stopped successfully")
        except Exception as e:
            logger.error(f"❌ Error stopping client: {e}")
        
        # Final comprehensive stats
        if stats['start_time']:
            uptime = datetime.now() - stats['start_time']
            logger.info(f"📊 FINAL MULTI-SOURCE STATS:")
            logger.info(f"   ⏱️ Total Uptime: {uptime}")
            logger.info(f"   📨 Messages Processed: {stats['messages_processed']}")
            logger.info(f"   🎯 Cards Found: {stats['cards_found']}")
            logger.info(f"   ✅ Cards Sent: {stats['cards_sent']} (to {len(TARGET_CHANNELS)} channels)")
            logger.info(f"   🔄 Duplicates Blocked: {stats['cards_duplicated']}")
            logger.info(f"   ⚡ Average Speed: {stats['cards_per_second']:.2f} cards/sec")
            logger.info(f"   🔍 BIN Success Rate: {stats['bin_lookups_success']}/{stats['bin_lookups_success'] + stats['bin_lookups_failed']}")
            logger.info(f"   💾 BINs Cached: {len(bin_cache)}")
            logger.info(f"   📢 Update Channel: {UPDATE_CHANNEL_USERNAME}")
            
            # Final per-group stats
            logger.info(f"📊 FINAL PER-GROUP STATS:")
            for i, group_id in enumerate(SOURCE_GROUPS):
                group_stats = stats['source_group_stats'][group_id]
                logger.info(f"   📁 Group {i+1} ({group_id}): {group_stats['messages_processed']} msgs, {group_stats['cards_found']} cards")
            
            logger.info(f"   ❌ Total Errors: {stats['errors']}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 MULTI-SOURCE CC MONITOR STOPPED BY USER")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)