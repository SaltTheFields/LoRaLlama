"""
Content Filter for Mesh LLM Bridge

Filters inappropriate, harmful, or questionable content from:
1. Incoming messages (before LLM processes them)
2. Outgoing responses (before sending to mesh)

Categories filtered:
- Profanity/obscenity
- Hate speech
- Violence/threats
- Illegal activities
- Personal attacks
- Spam/flooding
- Sensitive personal info (SSN, credit cards, etc.)
"""

import re
import logging
from typing import Tuple, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class FilterCategory(Enum):
    """Categories of filtered content."""
    PROFANITY = "profanity"
    HATE_SPEECH = "hate_speech"
    VIOLENCE = "violence"
    ILLEGAL = "illegal"
    PERSONAL_ATTACK = "personal_attack"
    SPAM = "spam"
    SENSITIVE_INFO = "sensitive_info"
    EXPLICIT = "explicit"
    SCAM = "scam"


@dataclass
class FilterResult:
    """Result of content filtering."""
    is_allowed: bool
    original_text: str
    filtered_text: Optional[str]
    category: Optional[FilterCategory]
    reason: Optional[str]
    confidence: float


class ContentFilter:
    """
    Content filter for mesh messages.

    Filters both incoming and outgoing content to maintain
    a safe and appropriate communication environment.
    """

    def __init__(self, strict_mode: bool = True):
        """
        Initialize the content filter.

        Args:
            strict_mode: If True, uses stricter filtering rules.
        """
        self.strict_mode = strict_mode
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile regex patterns for filtering."""

        # Profanity patterns (basic list - expand as needed)
        # Using word boundaries to avoid false positives
        profanity_words = [
            r'\bf+u+c+k+', r'\bs+h+i+t+', r'\ba+s+s+h+o+l+e',
            r'\bb+i+t+c+h+', r'\bd+a+m+n+', r'\bc+u+n+t+',
            r'\bd+i+c+k+', r'\bp+i+s+s+', r'\bc+o+c+k+',
            r'\bw+h+o+r+e+', r'\bs+l+u+t+', r'\bf+a+g+',
        ]
        self.profanity_pattern = re.compile(
            '|'.join(profanity_words),
            re.IGNORECASE
        )

        # Hate speech patterns
        hate_patterns = [
            r'\b(kill|murder|exterminate)\s+(all|every)\s+\w+',
            r'\b(death\s+to|die)\s+\w+',
            r'\bn+[i1]+g+[g3]+[e3a]+r*',
            r'\bk+[i1]+k+e+',
            r'\bsp+[i1]+c+',
            r'\bch+[i1]+n+k+',
        ]
        self.hate_pattern = re.compile(
            '|'.join(hate_patterns),
            re.IGNORECASE
        )

        # Violence/threat patterns
        violence_patterns = [
            r'\b(going to|gonna|will)\s+(kill|shoot|stab|hurt|attack)',
            r'\b(bomb|explosive|weapon)\s+(threat|attack)',
            r'\bi\'?ll\s+(kill|shoot|stab|hurt)',
            r'\bkill\s+your?(self)?',
            r'\bharm\s+(you|your|myself)',
        ]
        self.violence_pattern = re.compile(
            '|'.join(violence_patterns),
            re.IGNORECASE
        )

        # Illegal activity patterns
        illegal_patterns = [
            r'\b(buy|sell|get)\s+(drugs?|cocaine|heroin|meth)',
            r'\b(hack|crack)\s+(password|account|system)',
            r'\b(child|kid)\s+(porn|nude|naked)',
            r'\bhow\s+to\s+(make|build)\s+(bomb|explosive|weapon)',
            r'\bsteal\s+(credit|identity|money)',
        ]
        self.illegal_pattern = re.compile(
            '|'.join(illegal_patterns),
            re.IGNORECASE
        )

        # Sensitive personal info patterns
        self.ssn_pattern = re.compile(r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b')
        self.credit_card_pattern = re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b')
        self.phone_pattern = re.compile(r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b')

        # Explicit content patterns
        explicit_patterns = [
            r'\b(nude|naked|sex|porn)',
            r'\bsend\s+(nudes?|pics?|photos?)',
            r'\b(penis|vagina|breasts?)',
        ]
        self.explicit_pattern = re.compile(
            '|'.join(explicit_patterns),
            re.IGNORECASE
        )

        # Scam patterns
        scam_patterns = [
            r'\b(send|give)\s+(me\s+)?(money|bitcoin|crypto|btc)',
            r'\b(won|winner|lottery|prize)',
            r'\b(nigerian|prince|inheritance)',
            r'\bclick\s+(this|here|link)',
            r'\bfree\s+(money|crypto|bitcoin)',
        ]
        self.scam_pattern = re.compile(
            '|'.join(scam_patterns),
            re.IGNORECASE
        )

        # Spam detection (repetitive characters, all caps, etc.)
        self.repetition_pattern = re.compile(r'(.)\1{4,}')  # 5+ repeated chars
        self.caps_threshold = 0.7  # 70% caps = likely spam

    def filter_message(self, text: str, is_outgoing: bool = False) -> FilterResult:
        """
        Filter a message for inappropriate content.

        Args:
            text: The message text to filter.
            is_outgoing: True if this is an outgoing response.

        Returns:
            FilterResult with filtering decision and details.
        """
        if not text:
            return FilterResult(
                is_allowed=True,
                original_text=text,
                filtered_text=text,
                category=None,
                reason=None,
                confidence=1.0
            )

        # Check each category
        checks = [
            (self.hate_pattern, FilterCategory.HATE_SPEECH, "Contains hate speech"),
            (self.violence_pattern, FilterCategory.VIOLENCE, "Contains violent threats"),
            (self.illegal_pattern, FilterCategory.ILLEGAL, "References illegal activity"),
        ]

        # Add stricter checks if in strict mode
        if self.strict_mode:
            checks.extend([
                (self.profanity_pattern, FilterCategory.PROFANITY, "Contains profanity"),
                (self.explicit_pattern, FilterCategory.EXPLICIT, "Contains explicit content"),
                (self.scam_pattern, FilterCategory.SCAM, "Appears to be a scam"),
            ])

        for pattern, category, reason in checks:
            if pattern.search(text):
                logger.warning(f"Content filtered ({category.value}): {text[:50]}...")
                return FilterResult(
                    is_allowed=False,
                    original_text=text,
                    filtered_text=None,
                    category=category,
                    reason=reason,
                    confidence=0.9
                )

        # Check for sensitive personal info
        if self._contains_sensitive_info(text):
            logger.warning(f"Sensitive info detected: {text[:30]}...")
            return FilterResult(
                is_allowed=False,
                original_text=text,
                filtered_text=self._redact_sensitive_info(text),
                category=FilterCategory.SENSITIVE_INFO,
                reason="Contains sensitive personal information",
                confidence=0.95
            )

        # Check for spam characteristics
        spam_result = self._check_spam(text)
        if spam_result:
            logger.warning(f"Spam detected: {text[:50]}...")
            return FilterResult(
                is_allowed=False,
                original_text=text,
                filtered_text=None,
                category=FilterCategory.SPAM,
                reason=spam_result,
                confidence=0.8
            )

        # Message is allowed
        return FilterResult(
            is_allowed=True,
            original_text=text,
            filtered_text=text,
            category=None,
            reason=None,
            confidence=1.0
        )

    def _contains_sensitive_info(self, text: str) -> bool:
        """Check if text contains sensitive personal information."""
        if self.ssn_pattern.search(text):
            return True
        if self.credit_card_pattern.search(text):
            return True
        return False

    def _redact_sensitive_info(self, text: str) -> str:
        """Redact sensitive information from text."""
        text = self.ssn_pattern.sub('[SSN REDACTED]', text)
        text = self.credit_card_pattern.sub('[CARD REDACTED]', text)
        return text

    def _check_spam(self, text: str) -> Optional[str]:
        """Check if message appears to be spam."""
        # Check for excessive repetition
        if self.repetition_pattern.search(text):
            return "Excessive character repetition"

        # Check for excessive caps (if message is long enough)
        if len(text) > 10:
            caps_ratio = sum(1 for c in text if c.isupper()) / len(text)
            if caps_ratio > self.caps_threshold:
                return "Excessive capitalization"

        return None

    def filter_response(self, text: str) -> FilterResult:
        """
        Filter an outgoing LLM response.

        More lenient than incoming filter but still checks for issues.
        """
        return self.filter_message(text, is_outgoing=True)

    def get_safe_response(self, filter_result: FilterResult) -> str:
        """Get a safe response when content is filtered."""
        if filter_result.category == FilterCategory.HATE_SPEECH:
            return "I can't respond to that kind of message."
        elif filter_result.category == FilterCategory.VIOLENCE:
            return "I won't engage with threats or violent content."
        elif filter_result.category == FilterCategory.ILLEGAL:
            return "I can't help with that."
        elif filter_result.category == FilterCategory.SENSITIVE_INFO:
            return "Please don't share sensitive personal information over mesh radio."
        elif filter_result.category == FilterCategory.SPAM:
            return None  # Don't respond to spam
        elif filter_result.category == FilterCategory.SCAM:
            return "That looks like a scam. Be careful!"
        else:
            return "I can't respond to that message."


class RateLimiter:
    """Rate limiter to prevent message flooding."""

    def __init__(self, max_messages: int = 5, window_seconds: int = 60):
        """
        Initialize rate limiter.

        Args:
            max_messages: Maximum messages per window.
            window_seconds: Time window in seconds.
        """
        self.max_messages = max_messages
        self.window_seconds = window_seconds
        self.message_times: dict = {}  # user_id -> list of timestamps

    def is_allowed(self, user_id: str) -> Tuple[bool, Optional[str]]:
        """
        Check if a user is allowed to send a message.

        Returns:
            Tuple of (is_allowed, reason_if_blocked)
        """
        import time
        current_time = time.time()

        if user_id not in self.message_times:
            self.message_times[user_id] = []

        # Remove old timestamps
        self.message_times[user_id] = [
            t for t in self.message_times[user_id]
            if current_time - t < self.window_seconds
        ]

        if len(self.message_times[user_id]) >= self.max_messages:
            return False, f"Rate limited: max {self.max_messages} messages per {self.window_seconds}s"

        self.message_times[user_id].append(current_time)
        return True, None

    def reset(self, user_id: Optional[str] = None):
        """Reset rate limits."""
        if user_id:
            self.message_times.pop(user_id, None)
        else:
            self.message_times.clear()
