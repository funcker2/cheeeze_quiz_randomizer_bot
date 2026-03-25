from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Channel:
    id: str
    label: str


@dataclass(frozen=True)
class Country:
    code: str
    label: str
    admin_ids: tuple[int, ...]
    channels: tuple[Channel, ...]
    default_cooldown_minutes: int


@dataclass(frozen=True)
class Config:
    bot_token: str
    countries: tuple[Country, ...]

    @property
    def all_admin_ids(self) -> set[int]:
        return {uid for c in self.countries for uid in c.admin_ids}

    def country_by_code(self, code: str) -> Country | None:
        for c in self.countries:
            if c.code == code:
                return c
        return None

    def is_admin(self, user_id: int) -> bool:
        return user_id in self.all_admin_ids

    def admin_countries(self, user_id: int) -> list[Country]:
        return [c for c in self.countries if user_id in c.admin_ids]

    @classmethod
    def _parse_channels(cls, raw: str) -> tuple[Channel, ...]:
        channels: list[Channel] = []
        for entry in raw.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            parts = entry.split("|", maxsplit=1)
            ch_id = parts[0].strip()
            ch_label = parts[1].strip() if len(parts) > 1 else ch_id
            channels.append(Channel(id=ch_id, label=ch_label))
        return tuple(channels)

    @classmethod
    def _parse_admins(cls, raw: str) -> tuple[int, ...]:
        return tuple(int(uid.strip()) for uid in raw.split(",") if uid.strip())

    @classmethod
    def from_env(cls) -> "Config":
        token = getenv("BOT_TOKEN")
        if not token:
            raise ValueError("BOT_TOKEN is not set")

        raw_countries = getenv("COUNTRIES", "")
        countries: list[Country] = []
        for entry in raw_countries.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            parts = entry.split("|", maxsplit=1)
            code = parts[0].strip()
            label = parts[1].strip() if len(parts) > 1 else code
            admins = cls._parse_admins(getenv(f"ADMINS_{code}", ""))
            channels = cls._parse_channels(getenv(f"CHANNELS_{code}", ""))
            if not admins:
                raise ValueError(f"ADMINS_{code} is not set")
            if not channels:
                raise ValueError(f"CHANNELS_{code} is not set")
            cooldown = int(getenv(f"COOLDOWN_{code}", getenv("COOLDOWN_MINUTES", "120")))
            countries.append(Country(code=code, label=label, admin_ids=admins, channels=channels, default_cooldown_minutes=cooldown))

        if not countries:
            raise ValueError("COUNTRIES is not set — use format: code1|Label1;code2|Label2")

        return cls(
            bot_token=token,
            countries=tuple(countries),
        )
