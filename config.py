from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Channel:
    id: str
    label: str


@dataclass(frozen=True)
class Config:
    bot_token: str
    channels: tuple[Channel, ...]
    admin_ids: tuple[int, ...]
    default_cooldown_minutes: int

    @classmethod
    def from_env(cls) -> "Config":
        token = getenv("BOT_TOKEN")
        if not token:
            raise ValueError("BOT_TOKEN is not set")

        raw_admins = getenv("ADMIN_IDS", "")
        admin_ids = tuple(
            int(uid.strip()) for uid in raw_admins.split(",") if uid.strip()
        )
        if not admin_ids:
            raise ValueError("ADMIN_IDS is not set — provide comma-separated Telegram user IDs")

        raw_channels = getenv("CHANNELS", "")
        channels: list[Channel] = []
        for entry in raw_channels.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            parts = entry.split("|", maxsplit=1)
            ch_id = parts[0].strip()
            ch_label = parts[1].strip() if len(parts) > 1 else ch_id
            channels.append(Channel(id=ch_id, label=ch_label))
        if not channels:
            raise ValueError("CHANNELS is not set — use format: @id1|Label1;@id2|Label2")

        cooldown = int(getenv("COOLDOWN_MINUTES", "120"))

        return cls(
            bot_token=token,
            channels=tuple(channels),
            admin_ids=admin_ids,
            default_cooldown_minutes=cooldown,
        )
