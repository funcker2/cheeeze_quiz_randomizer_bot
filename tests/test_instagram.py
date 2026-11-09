import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from instagram import (  # noqa: E402
    NotAnAccount,
    instagram_url,
    overflow,
    parse_instagram,
    render_with_sponsor,
    utf16_len,
)


@pytest.mark.parametrize("raw", [
    "@sunny_cafe.bg",
    "sunny_cafe.bg",
    "  @Sunny_Cafe.BG  ",
    "instagram.com/sunny_cafe.bg",
    "www.instagram.com/sunny_cafe.bg/",
    "https://www.instagram.com/Sunny_Cafe.BG/?igsh=abc123",
    "http://m.instagram.com/sunny_cafe.bg",
    "https://instagram.com/sunny_cafe.bg/reels/",
])
def test_accepts_account_inputs(raw):
    assert parse_instagram(raw) == "sunny_cafe.bg"


@pytest.mark.parametrize("raw", [
    "https://www.instagram.com/p/C1abcDEF/",
    "instagram.com/reel/xyz",
    "https://www.instagram.com/stories/someone/123",
    "https://www.instagram.com/explore/",
])
def test_rejects_post_links(raw):
    with pytest.raises(NotAnAccount):
        parse_instagram(raw)


@pytest.mark.parametrize("raw", [
    "",
    "   ",
    "@",
    "@bad..name",
    ".leading",
    "trailing.",
    "a" * 31,
    "has space",
    "кириллица",
    "https://facebook.com/sunny",
    "https://www.instagram.com/",
    "https://evilinstagram.com/sunny",
])
def test_rejects_invalid(raw):
    assert parse_instagram(raw) is None


def test_max_length_handle_ok():
    assert parse_instagram("a" * 30) == "a" * 30


def test_url():
    assert instagram_url("sunny_cafe.bg") == "https://www.instagram.com/sunny_cafe.bg/"


def test_utf16_len_counts_emoji_as_two():
    assert utf16_len("📸") == 2
    assert utf16_len("Приз") == 4


def test_render_without_sponsor_is_identity():
    assert render_with_sponsor("Приз 🎁", None) == ("Приз 🎁", [])


def _covered(text, ent):
    raw = text.encode("utf-16-le")
    return raw[ent["offset"] * 2:(ent["offset"] + ent["length"]) * 2].decode("utf-16-le")


def test_render_entities_point_at_emoji_and_handle():
    prize = "🎁 Разыгрываем <b>сертификат</b> 🍰"
    text, ents = render_with_sponsor(prize, "odessa.shaurmama")
    assert text.endswith("\n\n🗯 @odessa.shaurmama")
    emoji, link = ents
    assert emoji["type"] == "custom_emoji"
    assert emoji["custom_emoji_id"] == "5888682921987937842"
    assert _covered(text, emoji) == "🗯"
    assert link["type"] == "text_link"
    assert _covered(text, link) == "@odessa.shaurmama"
    assert link["url"] == "https://www.instagram.com/odessa.shaurmama/"


def test_sponsor_html():
    from instagram import sponsor_html
    assert sponsor_html("odessa.shaurmama") == (
        '<tg-emoji emoji-id="5888682921987937842">🗯</tg-emoji> '
        '<a href="https://www.instagram.com/odessa.shaurmama/">@odessa.shaurmama</a>'
    )


def test_overflow_caption():
    handle = "sunny_cafe.bg"
    line = len("\n\n🗯 @sunny_cafe.bg") + 1  # emoji = 2 UTF-16 units
    assert overflow("x" * (1024 - line), handle, has_photo=True) == 0
    assert overflow("x" * (1024 - line + 17), handle, has_photo=True) == 17
    assert overflow("x" * 2000, handle, has_photo=False) == 0
    assert overflow("x" * 1024, None, has_photo=True) == 0
