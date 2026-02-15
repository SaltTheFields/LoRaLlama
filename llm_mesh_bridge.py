"""
LoRaLlama — LLM-Meshtastic Bridge

This script bridges an LLM to a Meshtastic mesh network,
allowing the LLM to receive and respond to messages from the mesh.

Requirements:
    pip install meshtastic bleak requests

Usage:
    python llm_mesh_bridge.py

For Ollama, make sure it's running locally:
    ollama serve
    ollama pull llama3.2  # or your preferred model
"""

import os
import sys
import time
import signal
import threading
import queue
import logging
import json
import re
import traceback
from typing import Optional, List, Dict
from datetime import datetime
from collections import deque

from meshtastic_connector import MeshtasticConnector, MeshMessage, LLMInterface
from mesh_database import MeshDatabase
from content_filter import ContentFilter, RateLimiter

# ==================== LOGGING SETUP ====================
# Set up comprehensive logging
LOG_FILE = "mesh_bridge.log"
LOG_LEVEL = logging.DEBUG  # Change to INFO for less verbosity

# Create formatters
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

# File handler - captures everything
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(file_formatter)

# Console handler - only INFO and above
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)

# Root logger
logging.basicConfig(
    level=LOG_LEVEL,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)
logger.info("=" * 60)
logger.info("LoRaLlama Starting")
logger.info("=" * 60)

# ==================== CONFIGURATION ====================

# Choose your LLM provider
LLM_PROVIDER = "ollama"  # or "anthropic", "openai", "none"

# Ollama configuration
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "llama3.2"  # Change to your preferred model

# Memory configuration
MEMORY_FILE = "mesh_memory.json"
MAX_CONVERSATION_HISTORY = 50  # Per user
MAX_TOTAL_MEMORY = 200  # Total messages to keep

# Web search configuration
DUCKDUCKGO_ENABLED = True  # Uses DuckDuckGo Instant Answer API (no key needed)

# Weather configuration (Open-Meteo API - free, no key needed)
WEATHER_ENABLED = True
WEATHER_LAT = 30.2672  # Austin, Texas latitude (change to your location)
WEATHER_LON = -97.7431  # Austin, Texas longitude (change to your location)

# Content filtering
CONTENT_FILTER_ENABLED = True
CONTENT_FILTER_STRICT = True  # Strict mode filters profanity, explicit content
RATE_LIMIT_MESSAGES = 10  # Max messages per user per minute
RATE_LIMIT_WINDOW = 60  # Window in seconds

# Location configuration
LOCATION = "Austin, Texas"  # Change to your location
TIMEZONE = "America/Chicago"  # Central Time

# Default system prompt (can be customized via setup wizard)
DEFAULT_SYSTEM_PROMPT = """You're a helpful AI assistant on a Meshtastic mesh radio network.

RULES:
- Max 150 characters! Be VERY brief.
- No emojis (cost 4 bytes each on LoRa radio).
- Answer the user's ACTUAL question directly.
- Use the provided context data ONLY when relevant to the question.
- Do NOT echo back signal/device data unless specifically asked.
- One short sentence answers only."""

# Will be set by setup wizard or use default
SYSTEM_PROMPT = DEFAULT_SYSTEM_PROMPT


def get_current_datetime_info() -> str:
    """Get current date/time info for LLM context."""
    try:
        # Try to use timezone-aware datetime
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TIMEZONE)
        now = datetime.now(tz)
        time_str = now.strftime('%I:%M %p %Z')
    except:
        now = datetime.now()
        time_str = now.strftime('%I:%M %p') + " Central"

    return f"""CURRENT REAL-TIME INFO (USE THIS, NOT YOUR TRAINING DATA):
- Today is: {now.strftime('%A, %B %d, %Y')}
- Current time: {time_str}
- Location: {LOCATION}
- IMPORTANT: Always use this date/time when asked - your training data is outdated!"""


def sanitize_user_input(text: str, username: str) -> str:
    """
    Sanitize user input to prevent prompt injection attacks.

    This prevents users from trying to:
    - Override system prompts
    - Pretend to be the system/assistant
    - Inject fake context or instructions
    """
    if not text:
        return ""

    # Remove or escape potentially dangerous patterns
    dangerous_patterns = [
        # System/role impersonation
        (r'(?i)\[?(system|assistant|ai|bot)\]?\s*:', '[USER_MSG]:'),
        (r'(?i)^(system|assistant):', '[USER_MSG]:'),
        # Instruction injection attempts
        (r'(?i)ignore (all )?(previous|above|prior) (instructions|prompts|rules)', '[BLOCKED]'),
        (r'(?i)disregard (all )?(previous|above|prior)', '[BLOCKED]'),
        (r'(?i)forget (all )?(previous|above|prior)', '[BLOCKED]'),
        (r'(?i)new (instructions|rules|prompt):', '[BLOCKED]:'),
        (r'(?i)override (instructions|rules|prompt)', '[BLOCKED]'),
        # Fake context injection
        (r'(?i)\[context\]', '[USER_CONTEXT]'),
        (r'(?i)\[instructions?\]', '[USER_NOTE]'),
        (r'(?i)<<.*?>>',  ''),  # Remove <<markers>>
        # Role play exploits
        (r'(?i)pretend (to be|you are|you\'re)', 'imagine'),
        (r'(?i)act as (if )?(you are|you\'re)', 'imagine'),
        (r'(?i)you are now', 'imagine you were'),
        # Jailbreak attempts
        (r'(?i)DAN\s*mode', '[BLOCKED]'),
        (r'(?i)developer mode', '[BLOCKED]'),
        (r'(?i)jailbreak', '[BLOCKED]'),
    ]

    sanitized = text
    for pattern, replacement in dangerous_patterns:
        sanitized = re.sub(pattern, replacement, sanitized)

    # Limit message length to prevent context stuffing
    max_length = 500
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "... [truncated]"

    # Log if we modified the message
    if sanitized != text:
        logger.warning(f"[SECURITY] Sanitized message from {username}: original={text[:100]}")

    return sanitized


class ConversationMemory:
    """Manages conversation history and persistent memory for the LLM."""

    def __init__(self, memory_file: str = MEMORY_FILE, max_per_user: int = MAX_CONVERSATION_HISTORY):
        self.memory_file = memory_file
        self.max_per_user = max_per_user
        self.conversations: Dict[str, deque] = {}  # user_id -> deque of messages
        self.user_facts: Dict[str, List[str]] = {}  # user_id -> list of remembered facts
        self.global_context: List[str] = []  # Global facts/context
        self._lock = threading.Lock()
        self._load_memory()

    def _load_memory(self):
        """Load memory from disk."""
        try:
            if os.path.exists(self.memory_file):
                with open(self.memory_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Load conversations
                    for user_id, messages in data.get('conversations', {}).items():
                        self.conversations[user_id] = deque(messages, maxlen=self.max_per_user)
                    # Load user facts
                    self.user_facts = data.get('user_facts', {})
                    # Load global context
                    self.global_context = data.get('global_context', [])
                logger.info(f"[MEMORY] Loaded: {len(self.conversations)} users, {len(self.global_context)} global facts")
        except Exception as e:
            logger.error(f"[MEMORY] Failed to load: {e}")

    def _save_memory(self):
        """Save memory to disk."""
        try:
            with self._lock:
                data = {
                    'conversations': {uid: list(msgs) for uid, msgs in self.conversations.items()},
                    'user_facts': self.user_facts,
                    'global_context': self.global_context,
                    'last_updated': datetime.now().isoformat()
                }
            with open(self.memory_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.debug("[MEMORY] Saved to disk")
        except Exception as e:
            logger.error(f"[MEMORY] Failed to save: {e}")

    def add_message(self, user_id: str, user_name: str, role: str, content: str):
        """Add a message to conversation history."""
        with self._lock:
            if user_id not in self.conversations:
                self.conversations[user_id] = deque(maxlen=self.max_per_user)

            self.conversations[user_id].append({
                'role': role,
                'content': content,
                'user_name': user_name,
                'timestamp': datetime.now().isoformat()
            })

        # Save periodically (every 5 messages)
        total_msgs = sum(len(c) for c in self.conversations.values())
        if total_msgs % 5 == 0:
            self._save_memory()

    def get_conversation_history(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Get recent conversation history for a user."""
        with self._lock:
            if user_id in self.conversations:
                history = list(self.conversations[user_id])
                return history[-limit:]
            return []

    def remember_fact(self, user_id: str, fact: str):
        """Remember a fact about a user."""
        with self._lock:
            if user_id not in self.user_facts:
                self.user_facts[user_id] = []
            if fact not in self.user_facts[user_id]:
                self.user_facts[user_id].append(fact)
                self._save_memory()

    def get_user_facts(self, user_id: str) -> List[str]:
        """Get remembered facts about a user."""
        with self._lock:
            return self.user_facts.get(user_id, [])

    def add_global_context(self, context: str):
        """Add global context/fact."""
        with self._lock:
            if context not in self.global_context:
                self.global_context.append(context)
                self._save_memory()

    def get_context_for_prompt(self, user_id: str, user_name: str) -> str:
        """Build context string for LLM prompt."""
        parts = []

        # Global context
        if self.global_context:
            parts.append("Global context: " + "; ".join(self.global_context[-5:]))

        # User facts
        user_facts = self.get_user_facts(user_id)
        if user_facts:
            parts.append(f"Known about {user_name}: " + "; ".join(user_facts[-5:]))

        # Recent conversation
        history = self.get_conversation_history(user_id, limit=6)
        if history:
            conv_parts = []
            for msg in history:
                role = "You" if msg['role'] == 'assistant' else msg.get('user_name', 'User')
                conv_parts.append(f"{role}: {msg['content']}")
            parts.append("Recent conversation:\n" + "\n".join(conv_parts))

        return "\n\n".join(parts) if parts else ""

    def clear_user(self, user_id: str):
        """Clear memory for a specific user."""
        with self._lock:
            if user_id in self.conversations:
                del self.conversations[user_id]
            if user_id in self.user_facts:
                del self.user_facts[user_id]
            self._save_memory()

    def clear_all(self):
        """Clear all memory."""
        with self._lock:
            self.conversations.clear()
            self.user_facts.clear()
            self.global_context.clear()
            self._save_memory()

    def save(self):
        """Force save memory to disk."""
        self._save_memory()


class WebSearch:
    """Simple web search using DuckDuckGo Instant Answer API."""

    def __init__(self):
        self.enabled = DUCKDUCKGO_ENABLED

    def search(self, query: str) -> Optional[str]:
        """
        Search the web and return a concise answer.

        Returns a short summary or None if no results.
        """
        if not self.enabled:
            return None

        try:
            import requests

            # DuckDuckGo Instant Answer API (no API key needed)
            url = "https://api.duckduckgo.com/"
            params = {
                'q': query,
                'format': 'json',
                'no_html': 1,
                'skip_disambig': 1
            }

            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                return None

            data = response.json()

            # Try to get an instant answer
            if data.get('AbstractText'):
                return data['AbstractText'][:500]

            if data.get('Answer'):
                return data['Answer'][:500]

            # Try related topics
            if data.get('RelatedTopics'):
                topics = data['RelatedTopics'][:3]
                results = []
                for topic in topics:
                    if isinstance(topic, dict) and topic.get('Text'):
                        results.append(topic['Text'][:150])
                if results:
                    return " | ".join(results)

            return None

        except Exception as e:
            logger.error(f"[SEARCH] Failed: {e}")
            return None

    def search_news(self, query: str) -> Optional[str]:
        """Search for recent news/information."""
        # For news, we append "news" or "latest" to get more recent results
        return self.search(f"{query} latest news 2024")


class WeatherService:
    """Weather service using Open-Meteo API (free, no API key needed)."""

    def __init__(self, lat: float = WEATHER_LAT, lon: float = WEATHER_LON):
        self.lat = lat
        self.lon = lon
        self.enabled = WEATHER_ENABLED
        self._cache = {}
        self._cache_time = 0
        self._cache_duration = 600  # Cache for 10 minutes

    def get_weather(self) -> Optional[str]:
        """
        Get current weather conditions.

        Returns a concise weather summary or None if unavailable.
        """
        if not self.enabled:
            return None

        # Check cache
        now = time.time()
        if self._cache and (now - self._cache_time) < self._cache_duration:
            return self._cache.get('summary')

        try:
            import requests

            # Open-Meteo API - free, no key needed
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                'latitude': self.lat,
                'longitude': self.lon,
                'current': 'temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m,wind_direction_10m',
                'temperature_unit': 'fahrenheit',
                'wind_speed_unit': 'mph',
                'timezone': TIMEZONE
            }

            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                logger.warning(f"[WEATHER] API returned status {response.status_code}")
                return None

            data = response.json()
            current = data.get('current', {})

            if not current:
                return None

            # Parse weather data
            temp = current.get('temperature_2m', 'N/A')
            feels_like = current.get('apparent_temperature', temp)
            humidity = current.get('relative_humidity_2m', 'N/A')
            wind_speed = current.get('wind_speed_10m', 0)
            wind_dir = current.get('wind_direction_10m', 0)
            weather_code = current.get('weather_code', 0)

            # Convert weather code to description
            weather_desc = self._weather_code_to_desc(weather_code)

            # Convert wind direction to cardinal
            wind_cardinal = self._degrees_to_cardinal(wind_dir)

            # Build concise summary
            summary = f"{weather_desc}, {temp}°F (feels {feels_like}°F), humidity {humidity}%, wind {wind_speed}mph {wind_cardinal}"

            # Cache the result
            self._cache = {
                'summary': summary,
                'temp': temp,
                'feels_like': feels_like,
                'humidity': humidity,
                'wind_speed': wind_speed,
                'wind_dir': wind_cardinal,
                'condition': weather_desc,
                'raw': current
            }
            self._cache_time = now

            logger.info(f"[WEATHER] Updated: {summary}")
            return summary

        except Exception as e:
            logger.error(f"[WEATHER] Failed to get weather: {e}")
            return None

    def get_forecast(self) -> Optional[str]:
        """Get a brief forecast for the next few hours."""
        if not self.enabled:
            return None

        try:
            import requests

            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                'latitude': self.lat,
                'longitude': self.lon,
                'hourly': 'temperature_2m,precipitation_probability,weather_code',
                'temperature_unit': 'fahrenheit',
                'timezone': TIMEZONE,
                'forecast_hours': 12
            }

            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                return None

            data = response.json()
            hourly = data.get('hourly', {})

            if not hourly:
                return None

            times = hourly.get('time', [])
            temps = hourly.get('temperature_2m', [])
            rain_chance = hourly.get('precipitation_probability', [])
            codes = hourly.get('weather_code', [])

            if not times or len(times) < 3:
                return None

            # Get key forecast points (now, +3h, +6h, +12h)
            forecasts = []
            for i in [0, 3, 6, min(11, len(times)-1)]:
                if i < len(times):
                    t = times[i].split('T')[1][:5]  # HH:MM
                    temp = temps[i] if i < len(temps) else 'N/A'
                    rain = rain_chance[i] if i < len(rain_chance) else 0
                    code = codes[i] if i < len(codes) else 0
                    desc = self._weather_code_to_short(code)

                    if rain > 30:
                        forecasts.append(f"{t}: {temp}°F {desc} {rain}% rain")
                    else:
                        forecasts.append(f"{t}: {temp}°F {desc}")

            return " | ".join(forecasts)

        except Exception as e:
            logger.error(f"[WEATHER] Forecast failed: {e}")
            return None

    def _weather_code_to_desc(self, code: int) -> str:
        """Convert WMO weather code to description."""
        codes = {
            0: "Clear",
            1: "Mostly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Foggy",
            48: "Freezing fog",
            51: "Light drizzle",
            53: "Drizzle",
            55: "Heavy drizzle",
            61: "Light rain",
            63: "Rain",
            65: "Heavy rain",
            66: "Freezing rain",
            67: "Heavy freezing rain",
            71: "Light snow",
            73: "Snow",
            75: "Heavy snow",
            77: "Snow grains",
            80: "Light showers",
            81: "Showers",
            82: "Heavy showers",
            85: "Light snow showers",
            86: "Snow showers",
            95: "Thunderstorm",
            96: "Thunderstorm w/ hail",
            99: "Severe thunderstorm"
        }
        return codes.get(code, "Unknown")

    def _weather_code_to_short(self, code: int) -> str:
        """Convert WMO weather code to short emoji/text."""
        if code == 0:
            return "☀️"
        elif code in [1, 2]:
            return "⛅"
        elif code == 3:
            return "☁️"
        elif code in [45, 48]:
            return "🌫️"
        elif code in [51, 53, 55, 61, 63, 65, 80, 81, 82]:
            return "🌧️"
        elif code in [66, 67]:
            return "🌨️"
        elif code in [71, 73, 75, 77, 85, 86]:
            return "❄️"
        elif code in [95, 96, 99]:
            return "⛈️"
        return "?"

    def _degrees_to_cardinal(self, degrees: float) -> str:
        """Convert wind direction degrees to cardinal direction."""
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                      'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        idx = int((degrees + 11.25) / 22.5) % 16
        return directions[idx]


def build_signal_context(message_data: dict) -> str:
    """Build signal quality context string for LLM."""
    if not message_data:
        return ""

    snr = message_data.get('snr')
    rssi = message_data.get('rssi')
    hop_limit = message_data.get('hop_limit', 0)
    hop_start = message_data.get('hop_start', 0)

    parts = []

    # SNR quality assessment
    if snr is not None:
        if snr > 10:
            quality = "excellent"
        elif snr > 5:
            quality = "good"
        elif snr > 0:
            quality = "fair"
        elif snr > -5:
            quality = "weak"
        else:
            quality = "very weak"
        parts.append(f"SNR: {snr}dB ({quality})")

    # RSSI assessment
    if rssi is not None and rssi != 0:
        if rssi > -70:
            strength = "strong"
        elif rssi > -90:
            strength = "moderate"
        elif rssi > -110:
            strength = "weak"
        else:
            strength = "very weak"
        parts.append(f"RSSI: {rssi}dBm ({strength})")

    # Hop count
    if hop_start and hop_limit:
        hops_used = hop_start - hop_limit
        if hops_used == 0:
            parts.append("Direct connection (no hops)")
        elif hops_used == 1:
            parts.append("1 hop away")
        else:
            parts.append(f"{hops_used} hops away")

    if parts:
        return "Their signal: " + ", ".join(parts)
    return ""


def build_mesh_health_context(db, connector) -> str:
    """Build mesh network health context for troubleshooting."""
    issues = []
    info = []

    try:
        # Get stats from database
        if db:
            stats = db.get_stats()
            node_count = stats.get('total_nodes', 0)
            active_24h = stats.get('active_nodes_24h', 0)

            info.append(f"{node_count} nodes known, {active_24h} active in 24h")

            # Check for potential issues
            if active_24h < node_count * 0.3 and node_count > 3:
                issues.append("Many nodes haven't been heard from recently")

        # Get connector stats
        if connector:
            conn_stats = connector.get_stats()
            sent = conn_stats.get('messages_sent', 0)
            failures = conn_stats.get('send_failures', 0)

            if sent > 0:
                fail_rate = failures / (sent + failures) * 100
                if fail_rate > 20:
                    issues.append(f"High send failure rate ({fail_rate:.0f}%)")

        # Build context string
        context_parts = []
        if info:
            context_parts.append("Mesh network: " + "; ".join(info))
        if issues:
            context_parts.append("Mesh issues: " + "; ".join(issues))

        return "\n".join(context_parts) if context_parts else ""

    except Exception as e:
        logger.error(f"[HEALTH] Error building health context: {e}")
        return ""


def run_setup_wizard() -> dict:
    """Interactive setup wizard for configuring the LLM personality and behavior."""
    global LOCATION

    print("\n" + "=" * 60)
    print("  LoRaLlama - Setup Wizard")
    print("=" * 60)

    config = {}

    # LLM Provider selection
    print("\n[0/7] Choose your LLM provider:")
    print("  1. Ollama (local) - Free, runs on your machine [Recommended]")
    print("  2. Anthropic (Claude) - Requires API key")
    print("  3. OpenAI (GPT) - Requires API key")
    print("  4. None - No LLM, listen-only mode")

    llm_choice = input("  Choose (1-4, press Enter for 1): ").strip() or '1'
    llm_providers = {'1': 'ollama', '2': 'anthropic', '3': 'openai', '4': 'none'}
    config['llm_provider'] = llm_providers.get(llm_choice, 'ollama')

    if config['llm_provider'] == 'ollama':
        print(f"\n  Ollama URL: {OLLAMA_BASE_URL}")
        print("  Available models (checking...):")
        try:
            import requests as req
            resp = req.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
            if resp.status_code == 200:
                models = resp.json().get('models', [])
                for i, m in enumerate(models, 1):
                    size = m.get('size', 0) / (1024**3)
                    print(f"    {i}. {m['name']} ({size:.1f} GB)")
                if models:
                    model_choice = input(f"  Choose model (1-{len(models)}, Enter for 1): ").strip()
                    idx = int(model_choice) - 1 if model_choice.isdigit() else 0
                    config['ollama_model'] = models[max(0, min(idx, len(models)-1))]['name']
                else:
                    print("    No models found. Run: ollama pull llama3.2")
                    config['ollama_model'] = 'llama3.2'
            else:
                print("    Could not connect to Ollama. Using default model.")
                config['ollama_model'] = 'llama3.2'
        except Exception:
            print("    Ollama not running. Start it with: ollama serve")
            config['ollama_model'] = 'llama3.2'
    elif config['llm_provider'] == 'anthropic':
        key = os.environ.get('ANTHROPIC_API_KEY', '')
        if key:
            print(f"  API key found: {key[:8]}...")
        else:
            print("  Set ANTHROPIC_API_KEY environment variable before running.")
    elif config['llm_provider'] == 'openai':
        key = os.environ.get('OPENAI_API_KEY', '')
        if key:
            print(f"  API key found: {key[:8]}...")
        else:
            print("  Set OPENAI_API_KEY environment variable before running.")

    # Location setup
    print(f"\n[1/7] Where is this mesh node located?")
    print(f"  Current: {LOCATION}")
    location = input("  Location (press Enter to keep current): ").strip()
    if location:
        LOCATION = location
    config['location'] = LOCATION

    # AI Name
    print("\n[2/7] What should the AI be called?")
    print("  Examples: MeshBot, RadioAI, LoRa Helper, HAM-GPT")
    name = input("  Name (press Enter for 'MeshBot'): ").strip()
    config['name'] = name if name else "MeshBot"

    # Personality
    print("\n[3/7] Choose a personality style:")
    print("  1. Chill & Laid-back - Relaxed, friendly, like a cool friend [Recommended]")
    print("  2. Friendly & Helpful - Warm, conversational, eager to help")
    print("  3. Professional - Efficient, straight to the point")
    print("  4. Technical - Precise, informative, mesh-focused")
    print("  5. Pirate - Arrr, talks like a pirate!")
    print("  6. Custom - Write your own personality description")

    personalities = {
        '1': ('chill', "a chill, laid-back friend. You're relaxed, use casual language, and don't over-explain things. You keep it real and simple"),
        '2': ('friendly', "friendly, warm, and conversational. You're happy to help and genuinely interested in people"),
        '3': ('professional', "professional and efficient. You give clear, direct answers without unnecessary fluff"),
        '4': ('technical', "technical and precise. You know mesh networking well and can help troubleshoot issues. You share signal stats when relevant"),
        '5': ('pirate', "a cheerful pirate! Ye speak like a salty sea dog, using 'arr', 'matey', 'ahoy', and other pirate speak"),
    }

    choice = input("  Choose (1-6, press Enter for 1): ").strip() or '1'

    if choice == '6':
        print("  Describe the personality (e.g., 'a helpful grandma who loves baking'):")
        custom = input("  > ").strip()
        config['personality'] = ('custom', custom if custom else "helpful and friendly")
    elif choice in personalities:
        config['personality'] = personalities[choice]
    else:
        config['personality'] = personalities['1']

    # Purpose/Role
    print("\n[4/7] What's the primary purpose of this AI?")
    print("  1. General assistant - Help with anything")
    print("  2. Emergency/survival info - Focus on safety and survival tips")
    print("  3. Weather & outdoor - Trail conditions, weather, hiking tips")
    print("  4. Ham radio helper - Amateur radio info, call signs, procedures")
    print("  5. Just chat - Social, conversational, keep people company")
    print("  6. Custom - Describe your own purpose")

    purposes = {
        '1': "You help with general questions and provide useful information on any topic.",
        '2': "You specialize in emergency preparedness, survival tips, first aid, and safety information. Lives may depend on your advice being accurate.",
        '3': "You focus on outdoor activities - weather conditions, trail info, hiking tips, camping advice, and nature information.",
        '4': "You're knowledgeable about amateur radio, including call signs, band conditions, procedures, and radio etiquette.",
        '5': "You're here to chat and keep people company. You're conversational, ask questions back, and enjoy getting to know people.",
    }

    choice = input("  Choose (1-6, press Enter for 1): ").strip() or '1'

    if choice == '6':
        print("  Describe the purpose:")
        custom = input("  > ").strip()
        config['purpose'] = custom if custom else purposes['1']
    elif choice in purposes:
        config['purpose'] = purposes[choice]
    else:
        config['purpose'] = purposes['1']

    # Response length preference
    print("\n[5/7] How verbose should responses be?")
    print("  1. Ultra-short (~80 bytes) - Single sentence max")
    print("  2. Standard (~180 bytes) - Brief, fits in one LoRa packet [Recommended]")
    print("  3. Max single (~230 bytes) - Uses full LoRa packet")
    print("  4. Multi-packet (will be truncated) - For testing only")

    lengths = {
        '1': ("80", "Keep responses EXTREMELY brief - one short sentence, under 80 bytes. NO emojis."),
        '2': ("180", "Keep responses SHORT - under 180 bytes, 1-2 sentences. Avoid emojis."),
        '3': ("230", "Use full packet - under 230 bytes. Avoid emojis (4 bytes each!)."),
        '4': ("230", "Be helpful but you'll be truncated if too long. Avoid emojis."),
    }

    choice = input("  Choose (1-4, press Enter for 2): ").strip() or '2'
    config['length'] = lengths.get(choice, lengths['2'])

    # Special behaviors
    print("\n[6/7] Any special behaviors? (comma-separated, or Enter to skip)")
    print("  Examples: use emojis, no emojis, include call sign KD2ABC,")
    print("            always end with 73, sign messages as 'de MeshBot'")
    special = input("  > ").strip()
    config['special'] = special

    # Build the system prompt
    prompt = build_system_prompt(config)

    print("\n" + "-" * 60)
    print("Generated System Prompt:")
    print("-" * 60)
    print(prompt)
    print("-" * 60)

    print("\n[7/7] Real-time context that will be included with each message:")
    print("-" * 60)
    print(get_current_datetime_info())
    print("-" * 60)

    confirm = input("\nUse this configuration? (Y/n): ").strip().lower()
    if confirm == 'n':
        print("Aborting setup. Using default prompt.")
        return {'prompt': DEFAULT_SYSTEM_PROMPT, 'max_length': 200}

    result = {'prompt': prompt, 'max_length': int(config['length'][0])}
    if 'llm_provider' in config:
        result['llm_provider'] = config['llm_provider']
    if 'ollama_model' in config:
        result['ollama_model'] = config['ollama_model']
    return result


def build_system_prompt(config: dict) -> str:
    """Build a system prompt from the configuration."""
    name = config['name']
    personality_name, personality_desc = config['personality']
    purpose = config['purpose']
    length_limit, length_desc = config['length']
    special = config.get('special', '')

    prompt = f"""You are {name}, an AI on a Meshtastic mesh radio network (LongFast channel).

Personality: {personality_desc}.

Purpose: {purpose}

CRITICAL BYTE LIMITS (LoRa radio):
- {length_desc}
- AVOID EMOJIS - each one costs 4 bytes!
- Be concise - every byte matters
- Short sentences only

Context data is injected below for reference. Use it to answer questions accurately.
Do NOT echo raw context data - synthesize a natural answer instead.
Only mention signal/device info when the user specifically asks about it."""

    if special:
        prompt += f"\n\nSpecial: {special}"

    prompt += "\n\nHelp people stay connected. Keep it short!"

    return prompt


class LLMHandler:
    """Handles LLM API calls for generating responses."""

    def __init__(self, provider: str = "ollama", model: Optional[str] = None, base_url: Optional[str] = None,
                 system_prompt: Optional[str] = None, enable_memory: bool = True, enable_web_search: bool = True):
        """
        Initialize the LLM handler.

        Args:
            provider: LLM provider - "ollama", "anthropic", "openai", or "none"
            model: Model name (for Ollama, e.g., "llama3.2", "mistral", "phi3")
            base_url: Base URL for Ollama API (default: http://localhost:11434)
            system_prompt: Custom system prompt for the LLM.
            enable_memory: Whether to use conversation memory.
            enable_web_search: Whether to enable web search for questions.
        """
        self.provider = provider
        self.client = None
        self.model = model or OLLAMA_MODEL
        self.base_url = base_url or OLLAMA_BASE_URL
        self.system_prompt = system_prompt or SYSTEM_PROMPT

        # Initialize database, content filter, web search, and weather
        self.db = MeshDatabase() if enable_memory else None
        self.web_search = WebSearch() if enable_web_search else None
        self.weather = WeatherService() if WEATHER_ENABLED else None
        self.content_filter = ContentFilter(strict_mode=CONTENT_FILTER_STRICT) if CONTENT_FILTER_ENABLED else None
        self.rate_limiter = RateLimiter(RATE_LIMIT_MESSAGES, RATE_LIMIT_WINDOW) if CONTENT_FILTER_ENABLED else None

        # Keep old memory for compatibility (will migrate to db)
        self.memory = ConversationMemory() if enable_memory else None

        # Store connector reference for mesh health context
        self.connector: Optional['MeshtasticConnector'] = None  # Will be set by bridge

        if self.db:
            logger.info("[LLM] Database storage enabled")
        if self.content_filter:
            logger.info(f"[LLM] Content filter enabled (strict={CONTENT_FILTER_STRICT})")
        if self.web_search:
            logger.info("[LLM] Web search enabled")
        if self.weather:
            logger.info(f"[LLM] Weather service enabled ({LOCATION})")

        if provider == "ollama":
            try:
                import requests
                # Test connection to Ollama
                response = requests.get(f"{self.base_url}/api/tags", timeout=5)
                if response.status_code == 200:
                    models = [m['name'] for m in response.json().get('models', [])]
                    logger.info(f"[LLM] Ollama connected at {self.base_url}")
                    logger.info(f"[LLM] Available models: {', '.join(models) if models else 'none'}")
                    print(f"Using Ollama at {self.base_url}")
                    print(f"Available models: {', '.join(models) if models else 'none'}")
                    if self.model not in [m.split(':')[0] for m in models]:
                        print(f"Warning: Model '{self.model}' may not be pulled. Run: ollama pull {self.model}")
                else:
                    raise Exception(f"Ollama returned status {response.status_code}")
            except ImportError:
                print("requests package not installed. Run: pip install requests")
                self.provider = "none"
            except Exception as e:
                logger.error(f"[LLM] Failed to connect to Ollama: {e}")
                print(f"Failed to connect to Ollama at {self.base_url}: {e}")
                print("Make sure Ollama is running: ollama serve")
                self.provider = "none"

        elif provider == "anthropic":
            try:
                import anthropic  # type: ignore[reportMissingImports]
                self.client = anthropic.Anthropic()
                logger.info("[LLM] Using Anthropic Claude API")
                print("Using Anthropic Claude API")
            except ImportError:
                print("anthropic package not installed. Run: pip install anthropic")
                self.provider = "none"
            except Exception as e:
                print(f"Failed to initialize Anthropic client: {e}")
                self.provider = "none"

        elif provider == "openai":
            try:
                import openai  # type: ignore[reportMissingImports]
                self.client = openai.OpenAI()
                logger.info("[LLM] Using OpenAI API")
                print("Using OpenAI API")
            except ImportError:
                print("openai package not installed. Run: pip install openai")
                self.provider = "none"
            except Exception as e:
                print(f"Failed to initialize OpenAI client: {e}")
                self.provider = "none"

        if self.provider == "none":
            logger.warning("[LLM] Running in echo mode (no LLM)")
            print("Running in echo mode (no LLM)")

    def _should_search(self, message: str) -> bool:
        """Determine if we should do a web search for this message."""
        # Don't search for weather - we have a dedicated service
        if self._is_weather_query(message):
            return False

        # Keywords that suggest a search would be helpful
        search_triggers = [
            'what is', 'who is', 'where is', 'when is', 'how to',
            'tell me about', 'search for', 'look up', 'find info',
            'google', 'search', 'news', 'latest',
            'define', 'meaning of', 'what does', 'how does',
            '?'  # Questions often benefit from search
        ]
        msg_lower = message.lower()
        return any(trigger in msg_lower for trigger in search_triggers)

    def _is_weather_query(self, message: str) -> bool:
        """Check if this is a weather-related query."""
        weather_triggers = [
            'weather', 'temperature', 'temp', 'forecast', 'rain',
            'hot', 'cold', 'humid', 'wind', 'sunny', 'cloudy',
            'storm', 'outside', 'degrees'
        ]
        msg_lower = message.lower()
        return any(trigger in msg_lower for trigger in weather_triggers)

    def _is_signal_query(self, message: str) -> bool:
        """Check if user is asking about their signal or connection."""
        signal_triggers = [
            'signal', 'snr', 'rssi', 'connection', 'reception',
            'how am i', 'how\'s my', 'can you hear', 'receiving',
            'hops', 'range', 'strength', 'quality'
        ]
        msg_lower = message.lower()
        return any(trigger in msg_lower for trigger in signal_triggers)

    def _extract_search_query(self, message: str) -> str:
        """Extract a search query from the message."""
        # Remove common prefixes
        prefixes = ['search for', 'look up', 'google', 'find info on',
                    'tell me about', 'what is', 'who is', 'where is']
        query = message.lower()
        for prefix in prefixes:
            if query.startswith(prefix):
                query = query[len(prefix):].strip()
                break
        return query if query else message

    def generate_response(self, message: str, from_name: str, user_id: Optional[str] = None,
                          message_data: Optional[dict] = None) -> tuple:
        """
        Generate a response to a message.

        Args:
            message: The incoming message text.
            from_name: Name of the sender.
            user_id: Unique identifier for the user (for memory).
            message_data: Full message data for database storage.

        Returns:
            Tuple of (response_text, was_filtered, filter_reason)
        """
        user_id = user_id or from_name  # Fallback to name if no ID
        logger.info(f"[LLM] Generating response for {from_name} ({user_id})")
        logger.debug(f"[LLM] Original message: {message}")

        # Sanitize input to prevent prompt injection
        message = sanitize_user_input(message, from_name)
        logger.debug(f"[LLM] Sanitized message: {message}")

        # Check rate limit
        if self.rate_limiter:
            allowed, reason = self.rate_limiter.is_allowed(user_id)
            if not allowed:
                logger.warning(f"[LLM] Rate limited {from_name} ({user_id}): {reason}")
                print(f"[RATE-LIMIT] {from_name} - {reason}")
                return None, True, reason
            else:
                logger.debug(f"[LLM] Rate limit OK for {from_name} ({user_id})")

        # Filter incoming message
        if self.content_filter:
            filter_result = self.content_filter.filter_message(message)
            if not filter_result.is_allowed:
                logger.warning(f"[LLM] Filtered message from {from_name}: {filter_result.reason}")
                # Log to database
                if self.db:
                    self.db.log_filtered_content(
                        user_id, from_name, message,
                        filter_result.reason,
                        filter_result.category.value if filter_result.category else "unknown"
                    )
                safe_response = self.content_filter.get_safe_response(filter_result)
                return safe_response, True, filter_result.reason

        # Message already saved to DB in _on_message() — no duplicate save needed here

        # Build context from database or memory
        context = ""
        if self.db:
            context = self.db.build_context_for_llm(user_id, from_name)
        elif self.memory:
            context = self.memory.get_context_for_prompt(user_id, from_name)
            self.memory.add_message(user_id, from_name, 'user', message)

        # Check if we should get weather
        weather_info = ""
        if self.weather and self._is_weather_query(message):
            logger.info("[LLM] Weather query detected, fetching weather...")
            weather = self.weather.get_weather()
            if weather:
                weather_info = f"CURRENT_WEATHER ({LOCATION}): {weather}"
                logger.info(f"[LLM] Weather: {weather}")
                # Also get forecast if they ask
                if 'forecast' in message.lower() or 'later' in message.lower():
                    forecast = self.weather.get_forecast()
                    if forecast:
                        weather_info += f"\nFORECAST: {forecast}"

        # Check if we should do a web search
        web_info = ""
        if self.web_search and self._should_search(message):
            query = self._extract_search_query(message)
            logger.info(f"[LLM] Web search for: {query}")
            result = self.web_search.search(query)
            if result:
                web_info = f"\n\nWeb search result for '{query}':\n{result}"
                logger.info(f"[LLM] Search found: {result[:100]}...")

        # Build signal context from message data
        signal_context = ""
        if message_data:
            signal_context = build_signal_context(message_data)
            if signal_context:
                logger.debug(f"[LLM] Signal context: {signal_context}")

        # Build mesh health context (especially if they're asking about issues)
        mesh_health = ""
        network_keywords = ['mesh', 'network', 'topology', 'nodes', 'node count', 'how many']
        if self._is_signal_query(message) or any(kw in message.lower() for kw in network_keywords):
            mesh_health = build_mesh_health_context(self.db, self.connector)
            if mesh_health:
                logger.debug(f"[LLM] Mesh health: {mesh_health}")
            # Add detailed network summary for mesh/network questions
            if self.db and any(kw in message.lower() for kw in network_keywords):
                net_summary = self.db.build_network_summary_for_llm()
                if net_summary:
                    mesh_health = (mesh_health + "\n" + net_summary) if mesh_health else net_summary
                    logger.debug("[LLM] Network summary added")

        # Build the full prompt
        prompt_parts = []

        # Always include current date/time info
        prompt_parts.append(get_current_datetime_info())

        # Include signal info only when user asks about signal/connection
        if signal_context and self._is_signal_query(message):
            prompt_parts.append(signal_context)

        # Include mesh health if relevant
        if mesh_health:
            prompt_parts.append(mesh_health)

        # Include weather if requested
        if weather_info:
            prompt_parts.append(weather_info)

        if context:
            prompt_parts.append(f"Context:\n{context}")
        if web_info:
            prompt_parts.append(f"Research:{web_info}")
        prompt_parts.append(f"Current message from {from_name}: {message}")
        prompt_parts.append(f"IMPORTANT: The sender's name is \"{from_name}\". If you address them, use \"{from_name}\" - never use placeholders like @username.")

        full_prompt = "\n\n".join(prompt_parts)
        logger.debug(f"[LLM] Full prompt length: {len(full_prompt)} chars")

        # Generate response
        logger.info("[LLM] Calling LLM API...")
        start_time = time.time()
        response_text = self._call_llm(full_prompt)
        elapsed = time.time() - start_time
        logger.info(f"[LLM] Response generated in {elapsed:.2f}s: {response_text[:100]}...")

        # Filter outgoing response
        if self.content_filter and response_text:
            filter_result = self.content_filter.filter_response(response_text)
            if not filter_result.is_allowed:
                logger.warning(f"[LLM] Filtered own response: {filter_result.reason}")
                response_text = "I need to rephrase that. Let me try again."

        # Save response to database
        if self.db and response_text:
            logger.debug("[LLM] Saving response to DB")
            self.db.save_message({
                'timestamp': datetime.now().isoformat(),
                'from_id': 'assistant',
                'from_name': 'assistant',
                'to_id': user_id,
                'text': response_text
            }, is_outgoing=True)

        # Store in legacy memory too
        if self.memory and response_text:
            self.memory.add_message(user_id, from_name, 'assistant', response_text)

        # Extract and save facts
        self._extract_and_remember_facts(user_id, from_name, message)

        return response_text, False, None

    def _extract_and_remember_facts(self, user_id: str, user_name: str, message: str):
        """Extract facts from messages to remember about users."""
        # Simple pattern matching for facts
        patterns = [
            (r"(?:i am|i'm|my name is)\s+(\w+)", lambda m: f"Name might be {m.group(1)}"),
            (r"(?:i live in|i'm from|i'm in)\s+(.+?)(?:\.|$)", lambda m: f"Located in {m.group(1)}"),
            (r"(?:my call ?sign is|i'm)\s+([A-Z]{1,2}\d[A-Z]{1,3})", lambda m: f"Call sign: {m.group(1)}"),
            (r"(?:i have|i own|i use)\s+(?:a|an)\s+(.+?)(?:\.|$)", lambda m: f"Has {m.group(1)}"),
        ]

        msg_lower = message.lower()
        for pattern, fact_builder in patterns:
            match = re.search(pattern, msg_lower, re.IGNORECASE)
            if match:
                fact = fact_builder(match)
                # Save to database
                if self.db:
                    fact_type = fact.split(':')[0] if ':' in fact else 'general'
                    fact_value = fact.split(':', 1)[1].strip() if ':' in fact else fact
                    self.db.save_fact(user_id, fact_type, fact_value, source='auto_extract')
                # Also save to legacy memory
                if self.memory:
                    self.memory.remember_fact(user_id, fact)
                logger.info(f"[LLM] Remembered about {user_name}: {fact}")

    def _call_llm(self, prompt: str) -> str:
        """Call the LLM provider with the given prompt."""
        if self.provider == "ollama":
            try:
                import requests
                logger.debug(f"[LLM] Calling Ollama model={self.model}")
                response = requests.post(
                    f"{self.base_url}/api/chat",
                    json={
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": prompt}
                        ],
                        "stream": False,
                        "options": {
                            "num_predict": 256  # Limit response length
                        }
                    },
                    timeout=60
                )
                if response.status_code == 200:
                    result = response.json()['message']['content']
                    logger.debug(f"[LLM] Ollama response: {result}")
                    return result
                else:
                    logger.error(f"[LLM] Ollama error: status={response.status_code}")
                    return f"Ollama error: {response.status_code}"
            except Exception as e:
                logger.error(f"[LLM] Ollama exception: {e}")
                return f"Error: {str(e)[:50]}"

        elif self.provider == "anthropic" and self.client:
            try:
                response = self.client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=256,
                    system=self.system_prompt,
                    messages=[{"role": "user", "content": prompt}]
                )
                return response.content[0].text
            except Exception as e:
                logger.error(f"[LLM] Anthropic exception: {e}")
                return f"Error: {str(e)[:50]}"

        elif self.provider == "openai" and self.client:
            try:
                response = self.client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    max_tokens=256,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": prompt}
                    ]
                )
                return response.choices[0].message.content
            except Exception as e:
                logger.error(f"[LLM] OpenAI exception: {e}")
                return f"Error: {str(e)[:50]}"

        else:
            # Echo mode
            return f"Echo: {prompt[:100]}"


class MeshLLMBridge:
    """
    Bridges Meshtastic mesh network to an LLM.

    Listens for incoming messages and generates LLM responses.
    """

    def __init__(
        self,
        ble_address: Optional[str] = None,
        serial_port: Optional[str] = None,
        use_ble: bool = True,
        llm_provider: str = "ollama",
        ollama_model: Optional[str] = None,
        auto_respond: bool = True,
        response_delay: float = 1.0,  # Reduced from 2.0 for faster responses
        system_prompt: Optional[str] = None,
        max_response_length: int = 230  # Leave room for LoRa header (max 237 bytes)
    ):
        """
        Initialize the bridge.

        Args:
            ble_address: Bluetooth address of Meshtastic device.
            serial_port: Serial port of device (if not using BLE).
            use_ble: Whether to use Bluetooth.
            llm_provider: LLM provider to use.
            ollama_model: Ollama model name (e.g., "llama3.2", "mistral").
            auto_respond: Whether to automatically respond to messages.
            response_delay: Delay before responding (to avoid collisions).
            system_prompt: Custom system prompt for the LLM.
            max_response_length: Maximum response length in characters.
        """
        self.auto_respond = auto_respond
        self.response_delay = response_delay
        self.running = False
        self.max_response_length = max_response_length

        logger.info("[BRIDGE] Initializing LLM handler...")

        # Initialize LLM handler with custom prompt, memory, and web search
        self.llm = LLMHandler(
            llm_provider,
            model=ollama_model,
            system_prompt=system_prompt,
            enable_memory=True,
            enable_web_search=True
        )

        logger.info("[BRIDGE] Initializing Meshtastic connector...")

        # Initialize Meshtastic connector with ALL callbacks
        self.connector = MeshtasticConnector(
            ble_address=ble_address,
            serial_port=serial_port,
            use_ble=use_ble,
            message_callback=self._on_message,
            packet_callback=self._on_packet,
            node_callback=self._on_node_update
        )

        # Give LLM handler access to connector for mesh health stats
        self.llm.connector = self.connector

        # Create LLM-friendly interface
        self.interface = LLMInterface(self.connector)

        # Pending responses queue
        self.pending_responses: list = []
        self.response_lock = threading.Lock()

        logger.info("[BRIDGE] Initialization complete")

    def _on_packet(self, packet: Dict, packet_type: str):
        """Handle ALL packets - save to database."""
        try:
            logger.debug(f"[BRIDGE] Received packet type: {packet_type}")

            # Save raw packet to database
            if self.llm.db:
                try:
                    self.llm.db.save_raw_packet(packet, packet_type)
                except Exception as e:
                    logger.warning(f"[BRIDGE] Could not save raw packet: {e}")

            # Process specific packet types
            from_id = packet.get('fromId') or packet.get('from')
            if not from_id or from_id == 'unknown':
                logger.debug(f"[BRIDGE] Skipping packet with no from_id: {packet_type}")
                return

            decoded = packet.get('decoded', {})

            if packet_type == 'POSITION_APP':
                position = decoded.get('position', {})
                if position and self.llm.db:
                    self.llm.db.save_position(from_id, position)
                    logger.info(f"[BRIDGE] Saved position for {from_id}")

            elif packet_type == 'TELEMETRY_APP':
                telemetry = decoded.get('telemetry', {})
                if telemetry and self.llm.db:
                    # Determine telemetry type
                    if 'deviceMetrics' in telemetry:
                        self.llm.db.save_telemetry(from_id, telemetry, 'device')
                    elif 'environmentMetrics' in telemetry:
                        self.llm.db.save_telemetry(from_id, telemetry, 'environment')
                    elif 'powerMetrics' in telemetry:
                        self.llm.db.save_telemetry(from_id, telemetry, 'power')
                    elif 'airQualityMetrics' in telemetry:
                        self.llm.db.save_telemetry(from_id, telemetry, 'air_quality')
                    elif 'localStats' in telemetry:
                        self.llm.db.save_telemetry(from_id, telemetry, 'local_stats')
                    elif 'healthMetrics' in telemetry:
                        self.llm.db.save_telemetry(from_id, telemetry, 'health')
                    else:
                        self.llm.db.save_telemetry(from_id, telemetry, 'unknown')
                    logger.info(f"[BRIDGE] Saved telemetry for {from_id}")

            elif packet_type == 'ROUTING_APP':
                if self.llm.db:
                    self.llm.db.save_routing(packet)
                    logger.debug("[BRIDGE] Saved routing info")

            elif packet_type == 'NEIGHBORINFO_APP':
                neighbor_info = decoded.get('neighborinfo', {})
                neighbors = neighbor_info.get('neighbors', [])
                if neighbors and self.llm.db:
                    for neighbor in neighbors:
                        self.llm.db.save_neighbor(from_id, neighbor)
                    logger.info(f"[BRIDGE] Saved {len(neighbors)} neighbors for {from_id}")

            elif packet_type == 'WAYPOINT_APP':
                waypoint = decoded.get('waypoint', {})
                if waypoint and self.llm.db:
                    self.llm.db.save_waypoint(from_id, waypoint, packet)
                    logger.info(f"[BRIDGE] Saved waypoint from {from_id}")

            elif packet_type == 'TRACEROUTE_APP':
                if self.llm.db:
                    self.llm.db.save_traceroute(packet)
                    logger.info(f"[BRIDGE] Saved traceroute from {from_id}")

            elif packet_type == 'STORE_FORWARD_APP':
                sf_data = decoded.get('storeAndForward', decoded)
                if self.llm.db:
                    self.llm.db.save_store_forward(from_id, sf_data, packet)
                    logger.info(f"[BRIDGE] Saved store_forward from {from_id}")

            elif packet_type == 'RANGE_TEST_APP':
                payload = decoded.get('text', decoded.get('payload', ''))
                if self.llm.db:
                    self.llm.db.save_range_test(from_id, str(payload), packet)
                    logger.info(f"[BRIDGE] Saved range_test from {from_id}")

            elif packet_type == 'DETECTION_SENSOR_APP':
                alert_text = decoded.get('text', str(decoded.get('payload', '')))
                if self.llm.db:
                    self.llm.db.save_detection_sensor(from_id, alert_text, packet)
                    logger.info(f"[BRIDGE] Saved detection_sensor from {from_id}")

            elif packet_type == 'PAXCOUNTER_APP':
                pax_data = decoded.get('paxcounter', decoded)
                if self.llm.db:
                    self.llm.db.save_paxcounter(from_id, pax_data, packet)
                    logger.info(f"[BRIDGE] Saved paxcounter from {from_id}")

            elif packet_type == 'MAP_REPORT_APP':
                # Map reports are saved as raw packets, no dedicated table needed
                logger.debug(f"[BRIDGE] Map report from {from_id} (saved as raw packet)")

        except Exception as e:
            logger.error(f"[BRIDGE] Error processing packet: {e}")
            logger.error(traceback.format_exc())

    def _on_node_update(self, node: Dict):
        """Handle node updates - save to database."""
        try:
            if self.llm.db:
                self.llm.db.save_node(node)
                node_id = node.get('num') or node.get('user', {}).get('id', 'unknown')
                name = node.get('user', {}).get('longName', 'Unknown')
                logger.info(f"[BRIDGE] Saved/updated node: {name} ({node_id})")
        except Exception as e:
            logger.error(f"[BRIDGE] Error saving node: {e}")
            logger.error(traceback.format_exc())

    def _is_reaction_emoji(self, text):
        """Check if message is just a reaction emoji (no real text content)."""
        if not text:
            return False
        text = text.strip()
        if not text:
            return False
        # If there's any ASCII letter/digit content, it's not just a reaction
        non_emoji = ''.join(c for c in text if ord(c) < 128)
        non_emoji = non_emoji.strip()
        if non_emoji:
            return False
        # It's emoji-only — cap at 8 codepoints to cover compound emojis
        # but reject long emoji-only sentences
        return len(text) <= 8

    def _on_message(self, message: MeshMessage):
        """Handle incoming text messages."""
        # Sanitize for console (replace emojis/unicode with placeholders)
        safe_name = message.from_name.encode('ascii', 'replace').decode() if message.from_name else "Unknown"
        safe_text = message.text.encode('ascii', 'replace').decode() if message.text else ""

        # Log to file with full unicode
        logger.info(f"[RX-MSG] From: {message.from_name} ({message.from_id})")
        logger.info(f"[RX-MSG] Text: {message.text}")
        logger.info(f"[RX-MSG] Channel: {message.channel}, SNR: {message.snr}, RSSI: {message.rssi}")

        # Print safe version to console
        print(f"\n[RX {datetime.now().strftime('%H:%M:%S')}] {safe_name}: {safe_text}")

        # Always save incoming message to database, regardless of sender
        if hasattr(self, 'llm') and self.llm and self.llm.db:
            try:
                self.llm.db.save_message({
                    'timestamp': datetime.now().isoformat(),
                    'from_id': message.from_id or 'unknown',
                    'from_name': message.from_name or 'Unknown',
                    'to_id': message.to_id,
                    'channel': message.channel,
                    'text': message.text,
                    'snr': message.snr,
                    'rssi': message.rssi,
                    'hop_limit': message.hop_limit,
                    'hop_start': message.hop_start,
                    'packet_id': message.packet_id,
                })
                logger.info(f"[BRIDGE] Saved message to DB from {message.from_name or 'unknown'}")
            except Exception as e:
                logger.error(f"[BRIDGE] Failed to save message to DB: {e}")

        # Skip LLM response for reaction emojis (already saved to DB above)
        if message.text and self._is_reaction_emoji(message.text):
            logger.info(f"[BRIDGE] Skipping response to reaction emoji: {message.text}")
            return

        if self.auto_respond:
            # Skip messages where we truly can't identify the sender at all
            if not message.from_id or message.from_id == "unknown":
                logger.info(f"[BRIDGE] Skipping response - no usable sender ID")
                return

            # Skip bot commands (messages starting with !)
            if message.text and message.text.startswith('!'):
                logger.info(f"[BRIDGE] Skipping bot command: {message.text}")
                return

            # Don't respond to our own messages
            my_info = self.connector.get_my_info()
            my_id = my_info.get('user', {}).get('id', '')

            if message.from_id != my_id:
                # Queue response
                with self.response_lock:
                    self.pending_responses.append(message)
                    logger.info(f"[BRIDGE] Queued response for {message.from_name} (queue size: {len(self.pending_responses)})")

    def _response_worker(self):
        """Worker thread that processes pending responses and outbox messages."""
        logger.info("[WORKER] Response worker started")

        # Track last outbox check time
        last_outbox_check = 0
        outbox_check_interval = 2  # Check outbox every 2 seconds

        while self.running:
            message = None
            queue_size = 0

            # ===== Check for dashboard outbox messages =====
            current_time = time.time()
            if self.llm.db and (current_time - last_outbox_check) >= outbox_check_interval:
                last_outbox_check = current_time
                try:
                    pending_outbox = self.llm.db.get_pending_outbox()[:5]  # Process up to 5 at a time
                    for outbox_msg in pending_outbox:
                        outbox_id = outbox_msg['id']
                        text = outbox_msg['message']
                        destination = outbox_msg.get('destination', '^all')
                        channel = outbox_msg.get('channel', 0)
                        msg_type = outbox_msg.get('msg_type', 'text')

                        logger.info(f"[OUTBOX] Processing queued {msg_type} #{outbox_id}: {text[:50]}...")
                        print(f"[OUTBOX] Processing dashboard {msg_type}: {text[:50]}...")

                        try:
                            # Route by message type
                            if msg_type == 'traceroute':
                                send_result = self.connector.send_traceroute(destination)
                            elif msg_type == 'dm':
                                # PKC-encrypted DM via dashboard
                                send_result = self.connector.send_dm(text, destination=destination)
                            else:
                                send_result = self.connector.send_message(
                                    text,
                                    destination=destination,
                                    channel_index=channel
                                )

                            if send_result:
                                # Mark as sent
                                self.llm.db.mark_outbox_sent(outbox_id)
                                logger.info(f"[OUTBOX] Message #{outbox_id} sent successfully")
                                print("[OUTBOX] Message sent successfully!")

                                # Also save to sent_messages for history (skip traceroutes)
                                if msg_type != 'traceroute':
                                    self.llm.db.save_sent_message(text, destination, channel)
                            else:
                                # Mark as failed
                                self.llm.db.mark_outbox_failed(outbox_id, "Send returned False")
                                logger.warning(f"[OUTBOX] Message #{outbox_id} send failed")
                                print("[OUTBOX] Message send failed!")

                        except Exception as e:
                            self.llm.db.mark_outbox_failed(outbox_id, str(e))
                            logger.error(f"[OUTBOX] Error sending message #{outbox_id}: {e}")
                            print(f"[OUTBOX] Error: {e}")

                        # Small delay between outbox messages to avoid radio congestion
                        time.sleep(0.5)

                except Exception as e:
                    logger.error(f"[OUTBOX] Error checking outbox: {e}")

            with self.response_lock:
                queue_size = len(self.pending_responses)
                if self.pending_responses:
                    message = self.pending_responses.pop(0)
                    logger.info(f"[WORKER] Processing message from queue (remaining: {len(self.pending_responses)})")
                    print(f"[QUEUE] Processing 1 of {queue_size} pending messages...")

            if message:
                # Delay to avoid radio collisions
                logger.debug(f"[WORKER] Waiting {self.response_delay}s before responding...")
                time.sleep(self.response_delay)

                try:
                    # Generate LLM response
                    logger.info(f"[WORKER] Generating response to {message.from_name}...")
                    print("[LLM] Thinking...")

                    # Build message data for database
                    message_data = {
                        'to_id': message.to_id,
                        'channel': message.channel,
                        'snr': message.snr,
                        'rssi': message.rssi,
                        'hop_limit': message.hop_limit,
                        'hop_start': message.hop_start,
                        'packet_id': message.packet_id,
                        'raw_packet': message.raw_packet
                    }

                    response, was_filtered, filter_reason = self.llm.generate_response(
                        message.text,
                        message.from_name,
                        user_id=message.from_id,
                        message_data=message_data
                    )

                    # Handle filtered content
                    if was_filtered:
                        if response:  # There's a safe response to send
                            logger.info(f"[WORKER] Sending safe response due to filter: {filter_reason}")
                            print(f"[FILTERED] {filter_reason}")
                        else:  # No response (spam, etc.)
                            logger.info(f"[WORKER] No response due to filter: {filter_reason}")
                            print(f"[FILTERED] {filter_reason} - no response sent")
                            continue

                    if not response:
                        logger.warning("[WORKER] No response generated, skipping")
                        continue

                    # Truncate if too long (use BYTES not chars - emojis are 4 bytes each!)
                    # Meshtastic limit is 237 bytes but use 200 to be safe with overhead
                    max_bytes = 200
                    response_bytes = response.encode('utf-8')

                    if len(response_bytes) > max_bytes:
                        logger.info(f"[WORKER] Truncating response from {len(response_bytes)} bytes to ~{max_bytes}")
                        # Truncate at byte level, then decode safely
                        truncated = response_bytes[:max_bytes-3].decode('utf-8', errors='ignore')
                        # Find last complete word/sentence
                        last_space = truncated.rfind(' ')
                        if last_space > len(truncated) // 2:  # Only use word break if we keep >50%
                            truncated = truncated[:last_space]
                        response = truncated + "..."

                    # FINAL SAFETY CHECK - hard limit at 220 bytes
                    final_bytes = len(response.encode('utf-8'))
                    if final_bytes > 220:
                        logger.warning(f"[WORKER] Still too long ({final_bytes} bytes), hard truncating")
                        response = response.encode('utf-8')[:217].decode('utf-8', errors='ignore') + "..."

                    logger.info(f"[WORKER] Final response: {len(response.encode('utf-8'))} bytes")

                    # Log and send
                    logger.info(f"[TX-MSG] Sending response: {response}")
                    safe_response = response.encode('ascii', 'replace').decode()
                    print(f"[TX {datetime.now().strftime('%H:%M:%S')}] {safe_response}")

                    # Determine if this was a DM (to_id is our node, not broadcast)
                    my_info = self.connector.get_my_info()
                    my_id = my_info.get('user', {}).get('id', '')
                    is_dm = message.to_id and message.to_id != '^all' and message.to_id != '!ffffffff' and message.to_id == my_id
                    destination = message.from_id if is_dm else '^all'

                    # Save sent message to database
                    if self.llm.db:
                        self.llm.db.save_sent_message(response, destination, message.channel)

                    # Send via radio — PKC-encrypted DM back to sender, or broadcast on same channel
                    if is_dm:
                        logger.info(f"[WORKER] Sending PKC DM to {destination}")
                        send_result = self.connector.send_dm(response, destination=destination)
                    else:
                        logger.info(f"[WORKER] Sending broadcast on ch {message.channel}")
                        send_result = self.connector.send_message(
                            response,
                            destination=destination,
                            channel_index=message.channel
                        )
                    logger.info(f"[WORKER] Send result: {send_result}")

                    if send_result:
                        print("[OK] Response sent successfully!")
                    else:
                        print("[FAIL] Response may not have been sent!")

                except Exception as e:
                    logger.error(f"[WORKER] Error generating response: {e}")
                    logger.error(traceback.format_exc())
                    print(f"[ERROR] Failed to generate response: {e}")
                    # Don't let one error stop the worker
                    continue

            time.sleep(0.5)

        logger.info("[WORKER] Response worker stopped")

    def start(self):
        """Start the bridge."""
        print("=" * 60)
        print("LoRaLlama — LLM-Meshtastic Bridge")
        print("=" * 60)
        logger.info("[BRIDGE] Starting bridge...")

        if not self.connector.connect():
            logger.error("[BRIDGE] Failed to connect to Meshtastic device")
            print("Failed to connect to Meshtastic device")
            return False

        # Save all initial nodes to database
        if self.llm.db:
            logger.info("[BRIDGE] Saving initial nodes to database...")
            nodes = self.connector.get_nodes()
            for node_id, node_data in nodes.items():
                self.llm.db.save_node(node_data)
            logger.info(f"[BRIDGE] Saved {len(nodes)} initial nodes")

        print("\n" + self.interface.get_status())
        print("\n" + self.interface.get_nodes_list())

        self.running = True

        # Start response worker thread
        self.worker_thread = threading.Thread(target=self._response_worker, daemon=True)
        self.worker_thread.start()
        logger.info("[BRIDGE] Response worker thread started")

        print("\n" + "=" * 60)
        print("Bridge is running! Listening for mesh messages...")
        print("Commands: /status, /nodes, /send <message>, /db, /quit")
        print("=" * 60)

        return True

    def run_interactive(self):
        """Run interactive command loop."""
        if not self.start():
            return

        print(f"\nLogs are saved to: {LOG_FILE}")
        print("Type /help for commands.\n")

        # Use a dedicated thread for input to avoid msvcrt.kbhit() issues on Windows
        # (background thread prints can trigger kbhit, causing the console to hang)
        input_queue = queue.Queue()

        def _input_reader():
            while self.running:
                try:
                    line = input("> ")
                    input_queue.put(line.strip())
                except EOFError:
                    break
                except KeyboardInterrupt:
                    input_queue.put(None)
                    break

        input_thread = threading.Thread(target=_input_reader, daemon=True)
        input_thread.start()

        try:
            while self.running:
                try:
                    try:
                        user_input = input_queue.get(timeout=0.5)
                    except queue.Empty:
                        continue

                    if user_input is None:
                        raise KeyboardInterrupt

                    if not user_input:
                        continue

                    if user_input.lower() in ('/quit', 'quit', 'exit', '/exit'):
                        break
                    elif user_input == '/status':
                        print(self.interface.get_status())
                    elif user_input == '/nodes':
                        print(self.interface.get_nodes_list())
                    elif user_input.startswith('/send '):
                        message = user_input[6:]
                        print(self.interface.send(message))
                    elif user_input == '/auto on':
                        self.auto_respond = True
                        print("Auto-respond ENABLED - will respond to messages")
                    elif user_input == '/auto off':
                        self.auto_respond = False
                        print("Auto-respond DISABLED - listen only mode")
                    elif user_input == '/log':
                        print(f"Log file: {os.path.abspath(LOG_FILE)}")
                    elif user_input == '/clear':
                        # Clear pending responses
                        with self.response_lock:
                            count = len(self.pending_responses)
                            self.pending_responses.clear()
                        print(f"Cleared {count} pending response(s)")
                    elif user_input == '/db' or user_input == '/stats':
                        # Show database stats
                        if self.llm.db:
                            stats = self.llm.db.get_stats()
                            print("\n=== Database Statistics ===")
                            print(f"  Raw packets:     {stats.get('total_packets', 0)}")
                            print(f"  Text messages:   {stats.get('total_messages', 0)}")
                            print(f"  Sent messages:   {stats.get('sent_messages', 0)}")
                            print(f"  Known nodes:     {stats.get('total_nodes', 0)}")
                            print(f"  Active (24h):    {stats.get('active_nodes_24h', 0)}")
                            print(f"  Position records: {stats.get('position_records', 0)}")
                            print(f"  Telemetry records: {stats.get('telemetry_records', 0)}")
                            print(f"  Routing records: {stats.get('routing_records', 0)}")
                            print(f"  Neighbor records: {stats.get('neighbor_records', 0)}")
                            print(f"  User facts:      {stats.get('total_facts', 0)}")
                            print(f"  Filtered msgs:   {stats.get('filtered_messages', 0)}")
                            print(f"  Database size:   {stats.get('database_size_mb', 0)} MB")
                            if stats.get('packet_types'):
                                print("\n  Packet types:")
                                for ptype, count in stats['packet_types'].items():
                                    print(f"    {ptype}: {count}")
                        else:
                            print("Database is disabled")
                    elif user_input.startswith('/packets'):
                        # Show recent packets
                        parts = user_input.split()
                        ptype = parts[1] if len(parts) > 1 else None
                        limit = int(parts[2]) if len(parts) > 2 else 10
                        if self.llm.db:
                            packets = self.llm.db.get_raw_packets(packet_type=ptype, limit=limit)
                            print(f"\n=== Recent Packets {f'(type={ptype})' if ptype else ''} ===")
                            for p in packets:
                                print(f"  {p['timestamp'][:19]} | {p['packet_type']:15} | {p['from_id']} -> {p['to_id']}")
                        else:
                            print("Database is disabled")
                    elif user_input.startswith('/user '):
                        # Show user profile
                        user_query = user_input[6:].strip()
                        if user_query and self.llm.db:
                            # Search by name or ID
                            nodes = self.llm.db.get_all_nodes()
                            found = None
                            for node in nodes:
                                if (user_query.lower() in (node.get('long_name') or '').lower() or
                                    user_query.lower() in (node.get('short_name') or '').lower() or
                                    user_query in (node.get('node_id') or '')):
                                    found = node
                                    break
                            if found:
                                profile = self.llm.db.get_user_profile(found['node_id'])
                                print(f"\n=== User Profile: {found.get('long_name', 'Unknown')} ===")
                                print(f"  Node ID:    {found['node_id']}")
                                print(f"  Short name: {found.get('short_name', 'N/A')}")
                                print(f"  MAC:        {found.get('mac_address', 'N/A')}")
                                print(f"  Device:     {found.get('hw_model', 'Unknown')}")
                                print(f"  Role:       {found.get('role', 'N/A')}")
                                print(f"  Battery:    {found.get('battery_level', 'N/A')}%")
                                print(f"  Times heard: {found.get('times_heard', 0)}")
                                print(f"  First seen: {found.get('first_seen', 'N/A')}")
                                print(f"  Last updated: {found.get('last_updated', 'N/A')}")
                                if found.get('latitude') and found.get('longitude'):
                                    print(f"  Position:   {found['latitude']:.6f}, {found['longitude']:.6f}")
                                print(f"  Messages:   {profile['message_count']}")
                                if profile['facts']:
                                    print("  Facts:")
                                    for f in profile['facts'][:5]:
                                        print(f"    - {f['fact_type']}: {f['fact_value']}")
                                if profile['recent_positions']:
                                    print(f"  Recent positions: {len(profile['recent_positions'])}")
                                if profile['recent_telemetry']:
                                    print(f"  Recent telemetry: {len(profile['recent_telemetry'])}")
                            else:
                                print(f"User '{user_query}' not found")
                        else:
                            print("Usage: /user <name or id>")
                    elif user_input == '/memory':
                        # Show combined memory/db stats
                        if self.llm.db:
                            stats = self.llm.db.get_stats()
                            print("Database Statistics:")
                            print(f"  Total messages: {stats.get('total_messages', 0)}")
                            print(f"  Known nodes: {stats.get('total_nodes', 0)}")
                            print(f"  Users with facts: {stats.get('users_with_facts', 0)}")
                            print(f"  Global context items: {stats.get('global_context_items', 0)}")
                            print(f"  Filtered messages: {stats.get('filtered_messages', 0)}")
                        elif self.llm.memory:
                            mem = self.llm.memory
                            users = len(mem.conversations)
                            total_msgs = sum(len(c) for c in mem.conversations.values())
                            facts = sum(len(f) for f in mem.user_facts.values())
                            print("Memory stats:")
                            print(f"  Users tracked: {users}")
                            print(f"  Total messages: {total_msgs}")
                            print(f"  User facts: {facts}")
                            print(f"  Global context: {len(mem.global_context)}")
                        else:
                            print("Memory/Database is disabled")
                    elif user_input == '/memory clear' or user_input == '/db clear':
                        if self.llm.db:
                            confirm = input("Clear ALL database data? (yes/no): ").strip().lower()
                            if confirm == 'yes':
                                self.llm.db.clear_all()
                                print("All database data cleared")
                            else:
                                print("Cancelled")
                        elif self.llm.memory:
                            self.llm.memory.clear_all()
                            print("All memory cleared")
                        else:
                            print("Memory/Database is disabled")
                    elif user_input == '/memory save':
                        if self.llm.memory:
                            self.llm.memory.save()
                            print("Memory saved to disk")
                        if self.llm.db:
                            print("Database auto-saves (SQLite)")
                    elif user_input.startswith('/remember '):
                        # Add global context
                        fact = user_input[10:].strip()
                        if fact:
                            if self.llm.db:
                                self.llm.db.save_global_context(fact)
                            if self.llm.memory:
                                self.llm.memory.add_global_context(fact)
                            print(f"Remembered: {fact}")
                        else:
                            print("Usage: /remember <fact to remember>")
                    elif user_input.startswith('/search '):
                        # Manual web search
                        query = user_input[8:].strip()
                        if query and self.llm.web_search:
                            print(f"Searching for: {query}...")
                            result = self.llm.web_search.search(query)
                            if result:
                                print(f"Result: {result[:500]}")
                            else:
                                print("No results found")
                        else:
                            print("Usage: /search <query>")
                    elif user_input == '/weather':
                        # Get current weather
                        if self.llm.weather:
                            print(f"Fetching weather for {LOCATION}...")
                            weather = self.llm.weather.get_weather()
                            forecast = self.llm.weather.get_forecast()
                            if weather:
                                print(f"\nCurrent: {weather}")
                            if forecast:
                                print(f"Forecast: {forecast}")
                            if not weather and not forecast:
                                print("Could not fetch weather data")
                        else:
                            print("Weather service is disabled")
                    elif user_input in ('/help', '/h', '/?'):
                        print("\n=== Commands ===")
                        print("  /send <msg>     - Send a message to the mesh")
                        print("  /status         - Show connection status")
                        print("  /nodes          - List known mesh nodes")
                        print("  /auto on/off    - Enable/disable auto-respond")
                        print("  /clear          - Clear pending response queue")
                        print("")
                        print("Database & Memory:")
                        print("  /db or /stats   - Show comprehensive database stats")
                        print("  /packets [type] [n] - Show recent packets")
                        print("  /user <name>    - Show detailed user profile")
                        print("  /memory         - Show memory stats")
                        print("  /memory clear   - Clear all stored data")
                        print("  /remember <x>   - Add global context/fact")
                        print("")
                        print("Tools:")
                        print("  /weather        - Get current weather")
                        print("  /search <q>     - Manual web search")
                        print("  /log            - Show log file location")
                        print("  /quit           - Exit the bridge")
                    else:
                        print("Unknown command. Type /help for available commands.")

                except KeyboardInterrupt:
                    break

        finally:
            self.stop()

    def stop(self):
        """Stop the bridge."""
        logger.info("[BRIDGE] Stopping...")
        self.running = False
        # Save memory before exit
        if self.llm.memory:
            self.llm.memory.save()
            logger.info("[BRIDGE] Memory saved")
        # Close database
        if self.llm.db:
            self.llm.db.close()
            logger.info("[BRIDGE] Database closed")
        self.connector.disconnect()
        logger.info("[BRIDGE] Bridge stopped")
        print("\nBridge stopped.")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="LLM-Meshtastic Bridge")
    parser.add_argument("--ble", type=str, help="BLE address (e.g., AA:BB:CC:DD:EE:FF)")
    parser.add_argument("--serial", type=str, help="Serial port (e.g., COM4 or /dev/ttyUSB0)")
    parser.add_argument("--llm", type=str, default="ollama",
                        choices=["ollama", "anthropic", "openai", "none"],
                        help="LLM provider to use (default: ollama)")
    parser.add_argument("--model", type=str, default="llama3.2",
                        help="Ollama model to use (default: llama3.2)")
    parser.add_argument("--no-auto", action="store_true",
                        help="Disable automatic responses")
    parser.add_argument("--scan", action="store_true",
                        help="Scan for BLE devices and exit")
    parser.add_argument("--list-models", action="store_true",
                        help="List available Ollama models and exit")
    parser.add_argument("--setup", action="store_true",
                        help="Run interactive setup wizard for AI personality")
    parser.add_argument("--no-setup", action="store_true",
                        help="Skip setup wizard, use default prompt")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging to console")

    args = parser.parse_args()

    # Adjust console log level if debug requested
    if args.debug:
        console_handler.setLevel(logging.DEBUG)
        logger.info("[MAIN] Debug logging enabled")

    if args.list_models:
        try:
            import requests
            response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
            if response.status_code == 200:
                models = response.json().get('models', [])
                print("Available Ollama models:")
                for m in models:
                    size = m.get('size', 0) / (1024**3)  # Convert to GB
                    print(f"  - {m['name']} ({size:.1f} GB)")
                if not models:
                    print("  No models found. Run: ollama pull llama3.2")
            else:
                print(f"Failed to connect to Ollama: {response.status_code}")
        except Exception as e:
            print(f"Error connecting to Ollama: {e}")
            print("Make sure Ollama is running: ollama serve")
        return

    if args.scan:
        connector = MeshtasticConnector(use_ble=True)
        connector.scan_ble_devices()
        return

    # Determine connection type
    use_ble = not args.serial

    # Run setup wizard unless --no-setup is specified
    system_prompt = None
    max_length = 200

    llm_provider = args.llm
    ollama_model = args.model

    if args.no_setup:
        print("Using default configuration...")
        max_length = 230  # Default to max safe length
    else:
        # Run setup wizard (can be forced with --setup or runs by default)
        config = run_setup_wizard()
        system_prompt = config['prompt']
        max_length = config['max_length']
        # Use wizard selections if provided (override CLI defaults)
        if config.get('llm_provider'):
            llm_provider = config['llm_provider']
        if config.get('ollama_model'):
            ollama_model = config['ollama_model']

    bridge = MeshLLMBridge(
        ble_address=args.ble,
        serial_port=args.serial,
        use_ble=use_ble,
        llm_provider=llm_provider,
        ollama_model=ollama_model,
        auto_respond=not args.no_auto,
        system_prompt=system_prompt,
        max_response_length=max_length
    )

    # Install signal handler for clean Ctrl+C shutdown on Windows
    def _signal_handler(sig, frame):
        print("\n[Ctrl+C] Shutting down...")
        bridge.running = False
        bridge.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    try:
        signal.signal(signal.SIGBREAK, _signal_handler)  # Windows-specific
    except AttributeError:
        pass  # SIGBREAK only exists on Windows

    bridge.run_interactive()


if __name__ == "__main__":
    main()
