"""Sponsor Instagram helpers: parse admin input, build URLs, render the sponsor line.

Pure functions (no aiogram) so they can be unit-tested without the bot.
"""
import re
from urllib.parse import urlparse

_HANDLE_RE = re.compile(r"^[a-z0-9._]{1,30}$")
_HOSTS = {"instagram.com", "www.instagram.com", "m.instagram.com"}
# First path segments that are not accounts (posts, reels, stories, service pages).
RESERVED = {"p", "reel", "reels", "stories", "explore", "tv", "accounts", "direct"}

SPONSOR_PREFIX = "Спонсор: 📸 "
CAPTION_LIMIT = 1024
TEXT_LIMIT = 4096


class NotAnAccount(ValueError):
    """Input is an Instagram link, but to a post/reel/story, not to an account."""


def _valid_handle(h: str) -> bool:
    return bool(_HANDLE_RE.match(h)) and not h.startswith(".") and not h.endswith(".") and ".." not in h


def parse_instagram(raw: str) -> str | None:
    """Return the normalized (lowercase) handle, or None if the input is not an account.

    Raises NotAnAccount for links to posts, reels, stories and other non-account pages.
    """
    s = (raw or "").strip()
    if not s:
        return None
    if s.startswith("@"):
        h = s[1:].lower()
        return h if _valid_handle(h) else None

    looks_like_url = "/" in s or s.lower().startswith(("http:", "https:")) or "instagram.com" in s.lower()
    if not looks_like_url:
        h = s.lower()
        return h if _valid_handle(h) else None

    if not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    u = urlparse(s)
    if (u.hostname or "").lower() not in _HOSTS:
        return None
    segments = [p for p in u.path.split("/") if p]
    if not segments:
        return None
    first = segments[0].lower()
    if first in RESERVED:
        raise NotAnAccount(first)
    return first if _valid_handle(first) else None


def instagram_url(handle: str) -> str:
    return f"https://www.instagram.com/{handle}/"


def utf16_len(s: str) -> int:
    """Length in UTF-16 code units, the unit Telegram uses for entity offsets and limits."""
    return len(s.encode("utf-16-le")) // 2


def sponsor_label(handle: str) -> str:
    """Visible link text: names the network so readers know where the link leads."""
    return f"Instagram @{handle}"


def render_with_sponsor(prize_text: str, handle: str | None) -> tuple[str, dict | None]:
    """Append the sponsor line ("Спонсор: 📸 Instagram @handle") to the prize text.

    Returns (text, link_entity) where link_entity is a dict for a Telegram
    text_link MessageEntity covering "Instagram @handle", or None when there is no sponsor.
    Existing entities of prize_text stay valid because the line is appended at the end.
    """
    if not handle:
        return prize_text, None
    head = f"{prize_text}\n\n{SPONSOR_PREFIX}"
    label = sponsor_label(handle)
    entity = {
        "type": "text_link",
        "offset": utf16_len(head),
        "length": utf16_len(label),
        "url": instagram_url(handle),
    }
    return head + label, entity


def overflow(prize_text: str, handle: str | None, has_photo: bool) -> int:
    """How many UTF-16 units the rendered post exceeds the Telegram limit by (0 if it fits)."""
    text, _ = render_with_sponsor(prize_text, handle)
    limit = CAPTION_LIMIT if has_photo else TEXT_LIMIT
    return max(0, utf16_len(text) - limit)
