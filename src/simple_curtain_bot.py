from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from PIL import Image, ImageFilter

try:
    from reference_store_catalog import (
        CurtainPreset,
        CurtainTypeOption,
        load_reference_store_catalog,
    )
except ImportError:
    from src.reference_store_catalog import (
        CurtainPreset,
        CurtainTypeOption,
        load_reference_store_catalog,
    )

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
UPLOADS_DIR = TMP_DIR / "uploads_simple"
PAYLOADS_DIR = TMP_DIR / "api_payloads_simple"
GENERATED_DIR = TMP_DIR / "generated_simple"
STYLE_REFS_DIR = TMP_DIR / "preset_style_refs"
LOG_FILE = TMP_DIR / "simple_curtain_bot.log"
EXAMPLE_ROOM_PHOTO_PATH = PROJECT_ROOT / "Пример какое фото нужно скинуть.png"
TYPE_COLLAGE_PATH = TMP_DIR / "type_collage.jpg"
PREVIEW_STORE_CANDIDATES = ("Превью", "PreviewStore", "Preview")

CATBOX_UPLOAD_ENDPOINT = "https://catbox.moe/user/api.php"
ZEROX0_UPLOAD_ENDPOINT = "https://0x0.st"
TMPFILES_UPLOAD_ENDPOINT = "https://tmpfiles.org/api/v1/upload"
DEFAULT_NANOBANANA_ENDPOINT = "https://api.nanobananaapi.ai/api/v1/nanobanana/generate-2"
DEFAULT_NANOBANANA_TASK_ENDPOINT = "https://api.nanobananaapi.ai/api/v1/nanobanana/record-info"
DEFAULT_ASPECT_RATIO = "auto"
DEFAULT_RESOLUTION = "1K"
SUPPORTED_ASPECT_RATIOS = (
    "1:1",
    "2:3",
    "3:2",
    "4:3",
    "3:4",
    "4:5",
    "5:4",
    "9:16",
    "16:9",
    "21:9",
)

LOGGER = logging.getLogger(__name__)
ROUTER = Router()


class Flow(StatesGroup):
    waiting_privacy = State()
    waiting_type = State()
    waiting_mount_mode = State()
    waiting_color = State()
    waiting_room_photo = State()
    waiting_human_check = State()
    waiting_name = State()
    waiting_phone = State()


PRESETS: dict[str, CurtainPreset] = {}
TYPE_OPTIONS: dict[str, CurtainTypeOption] = {}
TYPE_START_PREVIEWS: dict[str, Path] = {}
PROMPT_VERSION_ITOG_1 = "itog_1"
PROMPT_VERSION_TITLE = "Итог 1"
LEADS_LOG_PATH = TMP_DIR / "leads_simple.jsonl"
USER_PROGRESS_PATH = TMP_DIR / "user_progress_simple.json"
MAX_FREE_GENERATIONS = 5
SECRET_RESET_CODE = (os.getenv("GEN_RESET_CODE") or "000000").strip()
TXT_AGREE = "\u0421\u043e\u0433\u043b\u0430\u0441\u0435\u043d"
TXT_POLICY = "\u041f\u043e\u043b\u0438\u0442\u0438\u043a\u0430"
TXT_BACK = "\u041d\u0430\u0437\u0430\u0434"
TXT_TO_START = "\u0412 \u043d\u0430\u0447\u0430\u043b\u043e"
TXT_HUMAN = "\u042f \u0447\u0435\u043b\u043e\u0432\u0435\u043a"
TXT_OPENING = "\u041d\u0430 \u043e\u043a\u043e\u043d\u043d\u044b\u0439 \u043f\u0440\u043e\u0435\u043c"
TXT_SASH = "\u041d\u0430 \u043a\u0430\u0436\u0434\u0443\u044e \u0441\u0442\u0432\u043e\u0440\u043a\u0443"
TXT_MANAGER = "\u041f\u043e\u043b\u0443\u0447\u0438\u0442\u044c \u0441\u043a\u0438\u0434\u043a\u0443"
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "your_manager").strip().lstrip("@") or "your_manager"
MANAGER_URL = f"https://t.me/{MANAGER_USERNAME}"
ENVY_CRM_BASE_URL = (os.getenv("ENVY_CRM_BASE_URL") or "").strip().rstrip("/")
ENVY_CRM_API_KEY = (os.getenv("ENVY_CRM_API_KEY") or "").strip()
ENVY_CRM_EMPLOYEE_ID_DEFAULT = int(os.getenv("ENVY_CRM_EMPLOYEE_ID", "0"))
ENVY_CRM_PIPELINE_ID_DEFAULT = int(os.getenv("ENVY_CRM_PIPELINE_ID", "0"))
ENVY_CRM_INBOX_TYPE_ID_DEFAULT = int(os.getenv("ENVY_CRM_INBOX_TYPE_ID", "0"))
ENVY_CRM_STAGE_ID_DEFAULT = int(os.getenv("ENVY_CRM_STAGE_ID", "0"))
ENVY_CRM_TEST_PREFIX = (os.getenv("ENVY_CRM_TEST_PREFIX") or "ТЕСТ").strip()

CALLME_TASK_COMMENT = "Клиент попросил перезвонить"
CALLME_RENAME_SUFFIX = "Попросил перезвонить"


def ensure_runtime_dirs() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    PAYLOADS_DIR.mkdir(parents=True, exist_ok=True)
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    STYLE_REFS_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
    )


def _is_image_file(path: Path) -> bool:
    return path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}


def _resolve_preview_store_root(project_root: Path) -> Path | None:
    for name in PREVIEW_STORE_CANDIDATES:
        candidate = project_root / name
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def _normalize_text_token(text: str) -> str:
    normalized = re.sub(r"[^a-zа-я0-9]+", "", (text or "").casefold().replace("ё", "е"))
    return normalized


def _normalize_color_key(color_title: str) -> str:
    value = _normalize_text_token(color_title)
    for suffix in (
        "овые",
        "евые",
        "ые",
        "ие",
        "ый",
        "ий",
        "ой",
        "ая",
        "яя",
        "ое",
        "ее",
    ):
        if value.endswith(suffix) and len(value) > len(suffix) + 2:
            return value[: -len(suffix)]
    return value


def _find_child_dir_by_token(parent: Path, expected: str) -> Path | None:
    expected_token = _normalize_text_token(expected)
    if not expected_token:
        return None
    children = [p for p in parent.iterdir() if p.is_dir()]
    for child in children:
        if _normalize_text_token(child.name) == expected_token:
            return child
    for child in children:
        child_token = _normalize_text_token(child.name)
        if expected_token in child_token or child_token in expected_token:
            return child
    return None


def _find_mount_dir(parent: Path, mount_mode: str) -> Path | None:
    children = [p for p in parent.iterdir() if p.is_dir()]
    for child in children:
        token = _normalize_text_token(child.name)
        if mount_mode == "opening" and "проем" in token:
            return child
        if mount_mode == "sash" and ("створк" in token or "стоврк" in token):
            return child
    return None


def _find_preview_image_for_preset(preview_root: Path, preset: CurtainPreset) -> Path | None:
    type_dir = _find_child_dir_by_token(preview_root, preset.type_title)
    if not type_dir:
        return None
    mount_dir = _find_mount_dir(type_dir, preset.mount_mode)
    if not mount_dir:
        return None
    color_dir = _find_child_dir_by_token(mount_dir, preset.color_title)
    if not color_dir:
        return None
    images = sorted([p for p in color_dir.iterdir() if p.is_file() and _is_image_file(p)], key=lambda p: p.name.casefold())
    if not images:
        return None
    return images[0]


def _find_type_start_preview(preview_root: Path, type_title: str) -> Path | None:
    type_dir = _find_child_dir_by_token(preview_root, type_title)
    if not type_dir:
        return None

    combined_names = {
        "объединенные",
        "объединенная",
        "обьедененная",
        "обьединенная",
        "combined",
    }
    combined_dirs = [
        child
        for child in type_dir.iterdir()
        if child.is_dir() and _normalize_text_token(child.name) in {_normalize_text_token(v) for v in combined_names}
    ]
    if not combined_dirs:
        return None

    images: list[Path] = []
    for combined_dir in combined_dirs:
        images.extend([p for p in combined_dir.iterdir() if p.is_file() and _is_image_file(p)])
    if not images:
        return None
    return sorted(images, key=lambda p: p.name.casefold())[0]


def _build_type_start_previews(
    *,
    preview_root: Path | None,
    type_options: dict[str, CurtainTypeOption],
) -> dict[str, Path]:
    if not preview_root:
        return {}
    mapping: dict[str, Path] = {}
    for type_id, type_option in type_options.items():
        preview = _find_type_start_preview(preview_root, type_option.title)
        if preview and preview.exists():
            mapping[type_id] = preview
    return mapping


def _apply_preview_overrides(
    presets: dict[str, CurtainPreset],
    *,
    preview_root: Path | None,
) -> dict[str, CurtainPreset]:
    if not preview_root:
        return presets
    remapped: dict[str, CurtainPreset] = {}
    overridden = 0
    for preset_id, preset in presets.items():
        preview = _find_preview_image_for_preset(preview_root, preset)
        if preview and preview.exists():
            remapped[preset_id] = CurtainPreset(
                preset_id=preset.preset_id,
                type_id=preset.type_id,
                type_title=preset.type_title,
                mount_mode=preset.mount_mode,
                mount_mode_title=preset.mount_mode_title,
                color_title=preset.color_title,
                preview_image=preview,
                reference_images=preset.reference_images,
            )
            overridden += 1
        else:
            remapped[preset_id] = preset
    LOGGER.info("preview_overrides_applied total=%s overridden=%s root=%s", len(remapped), overridden, preview_root)
    return remapped


def build_privacy_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Ознакомлен. Согласен", callback_data="privacy:agree")
    builder.adjust(1)
    return builder.as_markup()


def build_type_keyboard() -> InlineKeyboardMarkup:
    if not TYPE_OPTIONS:
        raise RuntimeError("Type options are not loaded")
    builder = InlineKeyboardBuilder()
    for type_option in TYPE_OPTIONS.values():
        builder.button(text=type_option.title, callback_data=f"type:{type_option.type_id}")
    builder.adjust(1)
    return builder.as_markup()


def build_mount_mode_keyboard(*, type_id: str) -> InlineKeyboardMarkup:
    type_option = TYPE_OPTIONS.get(type_id)
    if not type_option:
        raise RuntimeError("Unknown curtain type")

    builder = InlineKeyboardBuilder()
    for mount_mode in type_option.mount_modes:
        title = "РќР° РѕРєРѕРЅРЅС‹Р№ РїСЂРѕРµРј" if mount_mode == "opening" else "РќР° РєР°Р¶РґСѓСЋ СЃС‚РІРѕСЂРєСѓ"
        builder.button(text=title, callback_data=f"mount:{mount_mode}")
    builder.adjust(1)
    return builder.as_markup()


def _presets_for_type_and_mount(*, type_id: str, mount_mode: str) -> list[CurtainPreset]:
    return [
        preset
        for preset in PRESETS.values()
        if preset.type_id == type_id and preset.mount_mode == mount_mode
    ]


def build_color_keyboard(*, type_id: str, mount_mode: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for preset in sorted(
        _presets_for_type_and_mount(type_id=type_id, mount_mode=mount_mode),
        key=lambda item: item.color_title.casefold(),
    ):
        builder.button(text=preset.color_title, callback_data=f"color:{preset.preset_id}")
    builder.adjust(2)
    return builder.as_markup()


def build_mount_mode_keyboard_v2(*, type_id: str) -> InlineKeyboardMarkup:
    type_option = TYPE_OPTIONS.get(type_id)
    if not type_option:
        raise RuntimeError("Unknown curtain type")
    builder = InlineKeyboardBuilder()
    for mount_mode in type_option.mount_modes:
        title = TXT_OPENING if mount_mode == "opening" else TXT_SASH
        builder.button(text=title, callback_data=f"mount:{mount_mode}")
    builder.button(text=TXT_BACK, callback_data="back:type")
    builder.adjust(1)
    return builder.as_markup()


def build_color_keyboard_v2(*, type_id: str, mount_mode: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for preset in sorted(
        _presets_for_type_and_mount(type_id=type_id, mount_mode=mount_mode),
        key=lambda item: item.color_title.casefold(),
    ):
        builder.button(text=preset.color_title, callback_data=f"color:{preset.preset_id}")
    builder.button(text=TXT_BACK, callback_data="back:mount")
    builder.adjust(2)
    return builder.as_markup()


def build_wait_photo_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=TXT_BACK, callback_data="back:color")
    builder.button(text=TXT_TO_START, callback_data="back:start")
    builder.adjust(2)
    return builder.as_markup()


def build_human_check_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=TXT_HUMAN, callback_data="human:ok")
    builder.adjust(1)
    return builder.as_markup()


def _type_preview_for_option(type_option: CurtainTypeOption) -> Path | None:
    forced_preview = TYPE_START_PREVIEWS.get(type_option.type_id)
    if forced_preview and forced_preview.exists():
        return forced_preview
    candidates = [preset for preset in PRESETS.values() if preset.type_id == type_option.type_id]
    if not candidates:
        return None
    return random.choice(candidates).preview_image


def _pick_mount_example_pair(*, type_id: str) -> list[Path]:
    opening = sorted(
        [p for p in PRESETS.values() if p.type_id == type_id and p.mount_mode == "opening" and p.preview_image.exists()],
        key=lambda item: item.color_title.casefold(),
    )
    sash = sorted(
        [p for p in PRESETS.values() if p.type_id == type_id and p.mount_mode == "sash" and p.preview_image.exists()],
        key=lambda item: item.color_title.casefold(),
    )
    if not opening or not sash:
        return []

    opening_by_color: dict[str, CurtainPreset] = {}
    for preset in opening:
        opening_by_color.setdefault(_normalize_color_key(preset.color_title), preset)

    for preset in sash:
        key = _normalize_color_key(preset.color_title)
        matched = opening_by_color.get(key)
        if matched:
            return [matched.preview_image, preset.preview_image]

    return [opening[0].preview_image, sash[0].preview_image]


async def _send_mount_mode_examples(message: Message, state: FSMContext, *, type_id: str) -> None:
    await _delete_tracked_messages(state, message.bot, chat_id=message.chat.id, key="mount_example_message_ids")
    pair = _pick_mount_example_pair(type_id=type_id)
    if not pair:
        await state.update_data(mount_example_message_ids=[])
        return
    media = [InputMediaPhoto(media=FSInputFile(str(path))) for path in pair[:2]]
    sent_messages = await message.answer_media_group(media=media)
    sent_ids = [msg.message_id for msg in sent_messages]
    await state.update_data(mount_example_message_ids=sent_ids)


async def _safe_delete_messages(bot: Bot, *, chat_id: int, message_ids: list[int]) -> None:
    for msg_id in message_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            continue


async def _delete_tracked_messages(state: FSMContext, bot: Bot, *, chat_id: int, key: str) -> None:
    data = await state.get_data()
    raw = data.get(key) or []
    ids = [int(item) for item in raw if isinstance(item, int) or str(item).isdigit()]
    if ids:
        await _safe_delete_messages(bot, chat_id=chat_id, message_ids=ids)
    await state.update_data(**{key: []})


async def _track_message_id(state: FSMContext, *, key: str, message_id: int) -> None:
    data = await state.get_data()
    raw = data.get(key) or []
    current = [int(item) for item in raw if isinstance(item, int) or str(item).isdigit()]
    current.append(int(message_id))
    await state.update_data(**{key: current})


async def _send_tracked_text(
    message: Message,
    state: FSMContext,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> Message:
    sent = await message.answer(text, reply_markup=reply_markup)
    await _track_message_id(state, key="ui_message_ids", message_id=sent.message_id)
    return sent


def _build_type_collage_image() -> Path | None:
    previews: list[Path] = []
    for type_option in TYPE_OPTIONS.values():
        preview = _type_preview_for_option(type_option)
        if preview and preview.exists():
            previews.append(preview)
    previews = previews[:4]
    if len(previews) < 1:
        return None

    canvas_size = (1400, 1400)
    cell_w = canvas_size[0] // 2
    cell_h = canvas_size[1] // 2
    canvas = Image.new("RGB", canvas_size, (245, 245, 245))

    for idx, preview_path in enumerate(previews):
        with Image.open(preview_path).convert("RGB") as im:
            fitted = im.copy()
            fitted.thumbnail((cell_w - 24, cell_h - 24), Image.Resampling.LANCZOS)
            x = (idx % 2) * cell_w + (cell_w - fitted.width) // 2
            y = (idx // 2) * cell_h + (cell_h - fitted.height) // 2
            canvas.paste(fitted, (x, y))

    TYPE_COLLAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(TYPE_COLLAGE_PATH, format="JPEG", quality=92)
    return TYPE_COLLAGE_PATH


async def _send_type_collage(message: Message, state: FSMContext) -> None:
    await _delete_tracked_messages(state, message.bot, chat_id=message.chat.id, key="type_collage_message_ids")
    preview_paths: list[Path] = []
    for type_option in TYPE_OPTIONS.values():
        preview = _type_preview_for_option(type_option)
        if preview and preview.exists():
            preview_paths.append(preview)
    if not preview_paths:
        await state.update_data(type_collage_message_ids=[])
        return
    media = [InputMediaPhoto(media=FSInputFile(str(path))) for path in preview_paths[:10]]
    sent_messages = await message.answer_media_group(media=media)
    sent_ids = [msg.message_id for msg in sent_messages]
    await state.update_data(type_collage_message_ids=sent_ids)


async def _send_color_previews(message: Message, state: FSMContext, *, type_id: str, mount_mode: str) -> None:
    await _delete_tracked_messages(state, message.bot, chat_id=message.chat.id, key="color_preview_message_ids")
    previews = [
        preset.preview_image
        for preset in sorted(
        _presets_for_type_and_mount(type_id=type_id, mount_mode=mount_mode),
        key=lambda item: item.color_title.casefold(),
        )
        if preset.preview_image.exists()
    ]
    if not previews:
        await state.update_data(color_preview_message_ids=[])
        return
    media = [InputMediaPhoto(media=FSInputFile(str(path))) for path in previews[:10]]
    sent_messages = await message.answer_media_group(media=media)
    sent_ids = [msg.message_id for msg in sent_messages]
    await state.update_data(color_preview_message_ids=sent_ids)


def _parse_name_phone(raw_text: str) -> tuple[str, str] | None:
    text = (raw_text or "").strip()
    if len(text) < 5:
        return None

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) >= 2:
        name_candidate = lines[0]
        phone_candidate = " ".join(lines[1:])
        parsed = _validate_name_and_phone(name_candidate, phone_candidate)
        if parsed:
            return parsed

    match = re.search(r"(\+?\d[\d\-\s()]{7,}\d)", text)
    if not match:
        return None
    phone_candidate = match.group(1)
    name_candidate = (text[: match.start()] + " " + text[match.end() :]).strip(" ,;:-")
    return _validate_name_and_phone(name_candidate, phone_candidate)


def _validate_name_and_phone(name_raw: str, phone_raw: str) -> tuple[str, str] | None:
    name = re.sub(r"\s+", " ", (name_raw or "").strip())
    if not re.fullmatch(r"[A-Za-zА-Яа-яЁёІіЇїЄє'-]{2,}(?: [A-Za-zА-Яа-яЁёІіЇїЄє'-]{2,}){0,2}", name):
        return None

    digits = re.sub(r"\D", "", phone_raw or "")
    if len(digits) < 10 or len(digits) > 15:
        return None
    if phone_raw.strip().startswith("+"):
        phone = f"+{digits}"
    else:
        phone = f"+{digits}"
    return name, phone


def _validate_name_only(name_raw: str) -> str | None:
    name = re.sub(r"\s+", " ", (name_raw or "").strip())
    if not re.fullmatch(r"[A-Za-zА-Яа-яЁёІіЇїЄє'-]{2,}(?: [A-Za-zА-Яа-яЁёІіЇїЄє'-]{2,}){0,2}", name):
        return None
    return name


def _create_blurred_preview(source: Path, destination: Path) -> Path:
    with Image.open(source).convert("RGB") as image:
        blurred = image.filter(ImageFilter.GaussianBlur(radius=8))
        blurred.save(destination, format="JPEG", quality=92)
    return destination


def _detect_scene_lighting(source: Path) -> str | None:
    try:
        with Image.open(source).convert("L") as im:
            small = im.resize((256, 256), Image.Resampling.BILINEAR)
            data = list(small.getdata())
        avg = sum(data) / max(1, len(data))
        # Conservative threshold: darker interiors/exterior night shots.
        if avg < 105:
            return "low_light"
    except Exception:
        return None
    return None


def _append_lead_record(payload: dict[str, Any]) -> None:
    LEADS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LEADS_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _load_user_progress() -> dict[str, dict[str, Any]]:
    if not USER_PROGRESS_PATH.exists():
        return {}
    try:
        raw = json.loads(USER_PROGRESS_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return {str(k): v for k, v in raw.items() if isinstance(v, dict)}
    except Exception:
        LOGGER.exception("user_progress_load_failed")
    return {}


def _save_user_progress(data: dict[str, dict[str, Any]]) -> None:
    USER_PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    USER_PROGRESS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_user_progress(user_id: int) -> dict[str, Any]:
    all_data = _load_user_progress()
    record = all_data.get(str(user_id), {})
    generations_used = int(record.get("generations_used", 0) or 0)
    verified = bool(record.get("verified", False))
    crm_deal_id = int(record.get("crm_deal_id", 0) or 0)
    crm_client_id = int(record.get("crm_client_id", 0) or 0)
    crm_employee_id = int(record.get("crm_employee_id", 0) or 0)
    crm_deal_name = str(record.get("crm_deal_name", "") or "")
    callme_requested = bool(record.get("callme_requested", False))
    return {
        "generations_used": max(0, generations_used),
        "verified": verified,
        "crm_deal_id": crm_deal_id,
        "crm_client_id": crm_client_id,
        "crm_employee_id": crm_employee_id,
        "crm_deal_name": crm_deal_name,
        "callme_requested": callme_requested,
    }


def _set_user_progress(
    user_id: int,
    *,
    generations_used: int | None = None,
    verified: bool | None = None,
    crm_deal_id: int | None = None,
    crm_client_id: int | None = None,
    crm_employee_id: int | None = None,
    crm_deal_name: str | None = None,
    callme_requested: bool | None = None,
) -> None:
    all_data = _load_user_progress()
    current = all_data.get(str(user_id), {})
    if not isinstance(current, dict):
        current = {}

    if generations_used is not None:
        current["generations_used"] = max(0, int(generations_used))
    else:
        current["generations_used"] = max(0, int(current.get("generations_used", 0) or 0))

    if verified is not None:
        current["verified"] = bool(verified)
    else:
        current["verified"] = bool(current.get("verified", False))

    if crm_deal_id is not None:
        current["crm_deal_id"] = int(crm_deal_id)
    if crm_client_id is not None:
        current["crm_client_id"] = int(crm_client_id)
    if crm_employee_id is not None:
        current["crm_employee_id"] = int(crm_employee_id)
    if crm_deal_name is not None:
        current["crm_deal_name"] = str(crm_deal_name)
    if callme_requested is not None:
        current["callme_requested"] = bool(callme_requested)

    current["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    all_data[str(user_id)] = current
    _save_user_progress(all_data)


def build_offer_keyboard(*, remaining: int, callme_requested: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Написать менеджеру", url=MANAGER_URL)
    if remaining > 0:
        builder.button(
            text=f"Попробовать еще ({max(0, remaining)})",
            callback_data="offer:more",
        )
    if callme_requested:
        builder.button(text="Заявка на звонок принята ✅", callback_data="offer:callme_done")
    else:
        builder.button(text="Позвоните мне", callback_data="offer:callme")
    if remaining > 0:
        builder.adjust(2, 1)
    else:
        builder.adjust(1)
    return builder.as_markup()


def build_limit_keyboard(*, callme_requested: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Написать менеджеру", url=MANAGER_URL)
    if callme_requested:
        builder.button(text="Заявка на звонок принята ✅", callback_data="offer:callme_done")
    else:
        builder.button(text="Позвоните мне", callback_data="offer:callme")
    builder.adjust(1)
    return builder.as_markup()


def _replace_callme_button_with_done(markup: InlineKeyboardMarkup | None) -> InlineKeyboardMarkup | None:
    if not markup or not getattr(markup, "inline_keyboard", None):
        return markup

    changed = False
    rows: list[list[InlineKeyboardButton]] = []
    for row in markup.inline_keyboard:
        new_row: list[InlineKeyboardButton] = []
        for button in row:
            if button.callback_data == "offer:callme":
                new_row.append(
                    InlineKeyboardButton(
                        text="Заявка на звонок принята ✅",
                        callback_data="offer:callme_done",
                    )
                )
                changed = True
            else:
                new_row.append(button)
        rows.append(new_row)

    if not changed:
        return markup
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _envy_enabled() -> bool:
    return bool(ENVY_CRM_BASE_URL and ENVY_CRM_API_KEY)


def _envy_extract_id(payload: Any, *paths: tuple[str, ...]) -> int:
    for path in paths:
        current: Any = payload
        ok = True
        for key in path:
            if not isinstance(current, dict) or key not in current:
                ok = False
                break
            current = current[key]
        if ok:
            try:
                value = int(current)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
    return 0


def _envy_extract_deal_name(deal_obj: dict[str, Any]) -> str:
    if not isinstance(deal_obj, dict):
        return ""
    try:
        return str(deal_obj["values"]["service"]["name"]["value"] or "").strip()
    except Exception:
        return ""


async def _envy_call(path: str, payload: dict[str, Any], *, timeout: int = 60) -> dict[str, Any]:
    if not _envy_enabled():
        raise RuntimeError("ENVY_CRM is not configured")
    url = f"{ENVY_CRM_BASE_URL}{path}"
    return await asyncio.to_thread(
        _http_json_sync,
        method="POST",
        url=url,
        body=payload,
        query={"api_key": ENVY_CRM_API_KEY},
        timeout=timeout,
    )


def _build_company_facts_block() -> str:
    return (
        "😏 А пока, у нас есть шанс запомниться вам)\n"
        "Вот 3 нестандартных факта о компании:\n\n"
        "1. 607 шт - наш личный рекорд по количеству изделий на одном объекте\n\n"
        "2. У нас летающие мастера - рекорд высотной установки 15 метров от пола!\n"
        "(Делаем то, что боятся делать другие)\n\n"
        "3. Наша компания старше некоторых сотрудников, которые в ней работают.\n"
        "Мы на рынке с 2003 года"
    )


def _build_processing_text(*, include_facts: bool) -> str:
    text = "Фото принято. Запускаю примерку, это может занять 1-5 минут."
    if include_facts:
        text = f"{text}\n\n{_build_company_facts_block()}"
    return text


async def _create_envy_lead_from_form(
    *,
    name: str,
    phone: str,
    telegram_user_id: int,
    telegram_username: str | None,
) -> dict[str, Any]:
    if not _envy_enabled():
        LOGGER.warning("envy_create_skipped reason=env_missing")
        return {"ok": False, "reason": "env_missing"}

    client_payload = {
        "name": name,
        "contacts": [{"type": "phone", "value": phone}],
    }
    client_resp = await _envy_call("/openapi/v1/client/create/", client_payload)
    client_id = _envy_extract_id(
        client_resp,
        ("result", "client", "id"),
        ("result", "id"),
        ("id",),
    )
    if client_id <= 0:
        raise RuntimeError(f"envy_client_create_failed: {json.dumps(client_resp, ensure_ascii=False)[:700]}")

    # Keep explicit phone binding even if phone was passed in create payload.
    try:
        await _envy_call(
            "/openapi/v1/client/addContact/",
            {"client_id": client_id, "type": "phone", "value": phone},
        )
    except Exception:
        LOGGER.exception("envy_add_contact_failed client_id=%s", client_id)

    deal_name = f"{ENVY_CRM_TEST_PREFIX} Визуализация бот {name}".strip()
    deal_payload: dict[str, Any] = {
        "name": deal_name,
        "client_id": client_id,
        "employee_id": ENVY_CRM_EMPLOYEE_ID_DEFAULT,
        "pipeline_id": ENVY_CRM_PIPELINE_ID_DEFAULT,
        "inbox_type_id": ENVY_CRM_INBOX_TYPE_ID_DEFAULT,
    }
    if ENVY_CRM_STAGE_ID_DEFAULT > 0:
        deal_payload["stage_id"] = ENVY_CRM_STAGE_ID_DEFAULT

    deal_resp = await _envy_call("/openapi/v1/deal/create/", deal_payload)
    deal_id = _envy_extract_id(
        deal_resp,
        ("result", "deal", "id"),
        ("result", "id"),
        ("id",),
    )
    if deal_id <= 0:
        raise RuntimeError(f"envy_deal_create_failed: {json.dumps(deal_resp, ensure_ascii=False)[:700]}")

    try:
        await _envy_call("/openapi/v1/deal/toInbox/", {"deal_id": deal_id})
    except Exception:
        LOGGER.exception("envy_toinbox_failed deal_id=%s", deal_id)

    employee_id = ENVY_CRM_EMPLOYEE_ID_DEFAULT
    try:
        deal_get = await _envy_call("/openapi/v1/deal/get/", {"deal_id": deal_id})
        deal_obj = deal_get.get("result", {}) if isinstance(deal_get, dict) else {}
        employee_id = int(deal_obj.get("employee_id", employee_id) or employee_id)
    except Exception:
        LOGGER.exception("envy_deal_get_after_create_failed deal_id=%s", deal_id)

    comment_payload = {
        "deal_id": deal_id,
        "comment": (
            f"Лид из Telegram-бота визуализации.\n"
            f"Имя: {name}\n"
            f"Телефон: {phone}\n"
            f"Telegram user_id: {telegram_user_id}\n"
            f"Telegram username: @{telegram_username or ''}"
        ),
    }
    try:
        await _envy_call("/openapi/v1/deal/createComment/", comment_payload)
    except Exception:
        LOGGER.exception("envy_create_comment_after_create_failed deal_id=%s", deal_id)

    return {
        "ok": True,
        "deal_id": deal_id,
        "client_id": client_id,
        "employee_id": employee_id,
        "deal_name": deal_name,
    }


async def _mark_envy_callme(*, deal_id: int, fallback_name: str = "") -> dict[str, Any]:
    if not _envy_enabled():
        LOGGER.warning("envy_callme_skipped reason=env_missing deal_id=%s", deal_id)
        return {"ok": False, "reason": "env_missing"}

    deal_get = await _envy_call("/openapi/v1/deal/get/", {"deal_id": deal_id})
    deal_obj = deal_get.get("result", {}) if isinstance(deal_get, dict) else {}
    employee_id = int(deal_obj.get("employee_id", 0) or 0)
    client_id = int(deal_obj.get("client_id", 0) or 0)
    user_id = int(deal_obj.get("user_id", 0) or 0)
    if employee_id <= 0:
        employee_id = ENVY_CRM_EMPLOYEE_ID_DEFAULT

    existing_task_ids: list[int] = []
    try:
        tasks_resp = await _envy_call("/openapi/v1/deal/getTasks/", {"deal_id": deal_id})
        tasks = tasks_resp.get("tasks", []) if isinstance(tasks_resp, dict) else []
        for task in tasks if isinstance(tasks, list) else []:
            if not isinstance(task, dict):
                continue
            to_emp = int(task.get("to_employee_id", task.get("employee_id", 0)) or 0)
            if to_emp != employee_id:
                continue
            closed_at = task.get("closed_at")
            if closed_at not in (None, "", "0", 0):
                continue
            text = str(task.get("comment") or task.get("text") or "").strip().lower()
            if text == CALLME_TASK_COMMENT.lower():
                existing_task_ids.append(int(task.get("id", 0) or 0))
    except Exception:
        LOGGER.exception("envy_get_tasks_failed_for_callme deal_id=%s", deal_id)

    if not existing_task_ids:
        run_ts = int(time.time()) + 10 * 60
        task_payload = {
            "deal_id": deal_id,
            "employee_id": employee_id,
            "comment": CALLME_TASK_COMMENT,
            "time": run_ts,
        }
        await _envy_call("/openapi/v1/deal/updatetask/", task_payload)

    await _envy_call("/openapi/v1/deal/createComment/", {"deal_id": deal_id, "comment": CALLME_TASK_COMMENT})

    current_name = _envy_extract_deal_name(deal_obj) or fallback_name or f"{ENVY_CRM_TEST_PREFIX} Визуализация бот"
    if CALLME_RENAME_SUFFIX.casefold() not in current_name.casefold():
        new_name = f"{current_name} | {CALLME_RENAME_SUFFIX}"
        update_payload: dict[str, Any] = {
            "deal_id": deal_id,
            "employee_id": employee_id,
            "fields": [{"input_id": 2001, "value": new_name, "value_type_id": 1}],
        }
        if client_id > 0:
            update_payload["client_id"] = client_id
        if user_id > 0:
            update_payload["user_id"] = user_id
        await _envy_call("/openapi/v1/deal/updateDealValue/", update_payload)
    else:
        new_name = current_name

    return {
        "ok": True,
        "deal_id": deal_id,
        "employee_id": employee_id,
        "deal_name": new_name,
        "existing_task_ids": existing_task_ids,
    }


def _normalize_type_key(type_title: str) -> str:
    normalized = re.sub(r"[^a-zа-я0-9]+", " ", type_title.casefold().replace("ё", "е")).strip()
    if "рулон" in normalized:
        return "roller"
    if "плис" in normalized:
        return "pleated"
    if "алюмин" in normalized:
        return "aluminum_venetian"
    if "дерев" in normalized:
        return "wood_venetian"
    if "жалюз" in normalized:
        return "venetian"
    return "generic"


def _profile_key(type_key: str, mount_mode: str) -> str:
    return f"{type_key}:{mount_mode}"


def _type_mount_lock(type_key: str, mount_mode: str) -> str:
    key = _profile_key(type_key, mount_mode)

    if key == "roller:opening":
        return (
            "PROFILE LOCK (ROLLER / OPENING):\n"
            "- One roller module per physical opening.\n"
            "- Outside-mount over full opening width.\n"
            "- If one outer frame contains multiple sashes, treat it as one opening with one continuous module.\n"
            "- Flat fabric panel with straight bottom bar.\n"
            "- Keep every roller panel fully closed to the sill line (no half-open state).\n"
            "- No split into per-sash roller modules.\n\n"
        )
    if key == "roller:sash":
        return (
            "PROFILE LOCK (ROLLER / SASH):\n"
            "- Separate roller module on each sash.\n"
            "- Module boundaries strictly follow sash boundaries.\n"
            "- Do not merge neighbor sashes into one sheet.\n"
            "- Keep every sash roller fully closed (no partial opening).\n"
            "- No uncovered sash is allowed when sash mode is selected.\n"
            "- All sash roller bottom bars must sit at the same closed level.\n"
            "- Keep compact sash hardware and straight bottom bars.\n\n"
        )
    if key == "pleated:opening":
        return (
            "PROFILE LOCK (PLEATED / OPENING):\n"
            "- One pleated system per full opening.\n"
            "- If one outer frame contains multiple sashes, keep one opening-wide system, not per-sash modules.\n"
            "- Crisp accordion folds with uniform step.\n"
            "- Fully closed coverage from top rail down to lower frame/sill line.\n"
            "- No per-sash segmentation inside one opening.\n\n"
        )
    if key == "pleated:sash":
        return (
            "PROFILE LOCK (PLEATED / SASH):\n"
            "- Independent pleated module on each sash only.\n"
            "- Keep fold rhythm consistent on each sash module.\n"
            "- Each pleated sash module must be fully closed (no half-open gap).\n"
            "- No merge of two sashes into one module.\n\n"
        )
    if key == "aluminum_venetian:opening":
        return (
            "PROFILE LOCK (ALUMINUM VENETIAN / OPENING):\n"
            "- One headrail per opening, no per-sash split.\n"
            "- If one outer frame contains multiple sashes, keep one continuous opening-wide blind field.\n"
            "- Narrow aluminum slats, straight lines, mini-blind look.\n"
            "- No duplicated upper strip.\n\n"
        )
    if key == "aluminum_venetian:sash":
        return (
            "PROFILE LOCK (ALUMINUM VENETIAN / SASH):\n"
            "- Separate mini-blind module per sash.\n"
            "- Narrow metal slats with consistent pitch.\n"
            "- No wide wood-like slats and no module merge.\n\n"
        )
    if key == "wood_venetian:opening":
        return (
            "PROFILE LOCK (WOOD VENETIAN / OPENING):\n"
            "- One wooden venetian module per opening.\n"
            "- If one outer frame contains multiple sashes, keep one opening-wide module, never sash-sized splits.\n"
            "- Thicker slats with visible wood grain.\n"
            "- Mount headrail directly above the opening (close to top frame), not near room ceiling.\n"
            "- Keep top gap compact and realistic; no exaggerated high placement.\n"
            "- Keep inter-opening corner geometry unchanged.\n"
            "- Do not add extra vertical connector strips.\n\n"
        )
    if key == "wood_venetian:sash":
        return (
            "PROFILE LOCK (WOOD VENETIAN / SASH):\n"
            "- Independent wooden venetian module on each sash.\n"
            "- Slats thicker than aluminum mini-blinds.\n"
            "- Wood grain must come from style references.\n"
            "- No extra decorative central bands unless in refs.\n"
            "- WINDOW FRAME PRESERVATION (STRICT): keep original sash/frame color and material from Ref1 unchanged.\n"
            "- Never repaint white PVC frames into black/dark frame style from catalog photos.\n"
            "- Apply edits only to blinds/hardware; frame profiles, mullions, and glazing beads must stay original.\n\n"
        )
    if key == "venetian:opening":
        return (
            "PROFILE LOCK (VENETIAN / OPENING):\n"
            "- One venetian module per full opening.\n"
            "- Horizontal rigid slats with clean headrail.\n"
            "- No per-sash splits and no extra top duplicate layers.\n\n"
        )
    if key == "venetian:sash":
        return (
            "PROFILE LOCK (VENETIAN / SASH):\n"
            "- Separate venetian module on each sash.\n"
            "- Module edges aligned to sash frames.\n"
            "- No merge across sashes.\n\n"
        )

    if mount_mode == "sash":
        return (
            "PROFILE LOCK (GENERIC / SASH):\n"
            "- Separate module on each real sash panel.\n"
            "- No merge of multiple sashes.\n\n"
        )
    return (
        "PROFILE LOCK (GENERIC / OPENING):\n"
        "- One module per physical window opening.\n"
        "- No per-sash split inside one opening.\n\n"
    )


def _global_scene_lock() -> str:
    return (
        "CANVAS LOCK (ABSOLUTE PRIORITY):\n"
        "- Keep exact framing, camera position, crop boundaries, and perspective from Ref1.\n"
        "- No zoom in/out, no recrop, no rotation.\n"
        "- No camera tilt correction, no lens/FOV change, no viewpoint shift.\n"
        "- Preserve exact camera roll angle from Ref1 (if Ref1 is tilted, output must keep the same tilt).\n"
        "- Do not straighten verticals/horizon and do not apply perspective correction.\n"
        "- Keep window corners, reveal edges, and sill endpoints at the same pixel coordinates as Ref1.\n"
        "- Outside edited blind areas, Ref1 should remain pixel-identical as much as possible.\n"
        "- Preserve walls, ceiling, floor, furniture, decor, and all existing objects.\n"
        "- If area is empty in Ref1, it must remain empty.\n"
        "- Never create new windows/openings.\n\n"
    )


def _global_negative_lock() -> str:
    return (
        "NEGATIVE CONSTRAINTS:\n"
        "- No room redesign, no perspective warp, no geometry drift.\n"
        "- No global affine transform of scene (no shift, no scale, no skew, no warp).\n"
        "- No auto-leveling, no keystone fix, no vertical-line straightening.\n"
        "- No new objects anywhere (no toys/figurines/plants/decor additions).\n"
        "- No duplicate extra blind layers or random second rails.\n"
        "- No accidental switch from opening-mode into per-sash segmentation.\n"
        "- No floating hardware, clipping, or broken mechanics.\n"
        "- No text, watermark, logo, or stylized/cartoon look.\n\n"
    )


def _mount_mode_explicit_lock(mount_mode: str) -> str:
    if mount_mode == "opening":
        return (
            "MOUNT MODE (EXPLICIT): opening\n"
            "- Install coverings only at opening level.\n"
            "- Use outside-mount position above the opening (on wall/ceiling line), not inside the reveal.\n"
            "- Segmentation rule: opening boundary is the outermost frame contour, not inner sash lines.\n"
            "- If one outer frame includes two+ sashes, install exactly one module for that whole outer frame.\n"
            "- Place headrail close to upper edge of opening; avoid moving installation to room ceiling level.\n"
            "- Unless opening is physically adjacent to ceiling in Ref1, keep only a small offset above top frame.\n"
            "- For all opening-mode products, top hardware must sit just above each window opening (window-head zone), not at room ceiling height.\n"
            "- Keep installation anchor local to each opening; do not lift modules into a high wall band under ceiling.\n"
            "- Width rule: total system width should be close to opening width with small side overlap only.\n"
            "- Allowed side overlap: about 2-8% per side (or minimal physically plausible overlap).\n"
            "- Do not make opening-mode system excessively wide; never extend to full wall span.\n"
            "- Exactly one module per physical opening.\n"
            "- Never add per-sash modules inside the same opening.\n"
            "- Do not recess hardware into the inner niche unless explicitly present in Ref1.\n"
            "- If both opening-level and sash-level are possible, choose opening-level only.\n\n"
        )
    return (
        "MOUNT MODE (EXPLICIT): sash\n"
        "- Install coverings only at sash level.\n"
        "- Exactly one module per sash.\n"
        "- Never merge neighboring sashes into one opening-wide module.\n"
        "- If both opening-level and sash-level are possible, choose sash-level only.\n\n"
    )


def _edit_zone_lock(mount_mode: str) -> str:
    if mount_mode == "opening":
        return (
            "EDIT ZONE LOCK (OPENING MODE):\n"
            "- Allowed edit zone: opening area + minimal mounting strip directly above opening for headrail/cassette.\n"
            "- Forbidden edit band: high wall/ceiling zone far above opening.\n"
            "- Side overhang is allowed only as physically required by outside-mount hardware.\n"
            "- Prefer overall width approximately equal to sill/opening visual width; only slightly wider than opening.\n"
            "- Do not modify wall/sill texture or geometry outside this narrow installation zone.\n\n"
        )
    return (
        "EDIT ZONE LOCK (SASH MODE):\n"
        "- Allowed edit zone: strictly inside each sash boundaries plus tiny hardware contact edges.\n"
        "- No edits on outer wall reveal, niche surfaces, or opening-wide wall area.\n\n"
    )


def _full_closure_lock(mount_mode: str) -> str:
    if mount_mode == "opening":
        return (
            "FULL CLOSURE LOCK (GLOBAL):\n"
            "- Every installed covering must be fully closed.\n"
            "- In opening mode, each opening-level module must be lowered/closed to the target bottom line.\n"
            "- No partially open modules, no mixed open/closed state.\n\n"
        )
    return (
        "FULL CLOSURE LOCK (GLOBAL):\n"
        "- Every installed covering must be fully closed.\n"
        "- In sash mode, each sash module must be lowered/closed to the target bottom line.\n"
        "- No uncovered sash and no mixed open/closed state.\n\n"
    )


def _full_drop_height_lock(mount_mode: str) -> str:
    if mount_mode == "opening":
        return (
            "DROP HEIGHT LOCK (GLOBAL):\n"
            "- Keep modules fully lowered by height (not only slat tilt/fabric opacity).\n"
            "- Bottom edge of each opening-level module must reach the closed target line near sill/lower opening edge.\n"
            "- No half-raised modules and no stacked-up blind pack at top.\n\n"
        )
    return (
        "DROP HEIGHT LOCK (GLOBAL):\n"
        "- Keep modules fully lowered by height (not only slat tilt/fabric opacity).\n"
        "- Bottom edge of each sash module must reach the closed target line near lower sash edge.\n"
        "- No half-raised modules and no stacked-up blind pack at top.\n\n"
    )


def _type_specific_lock(type_key: str) -> str:
    if type_key == "pleated":
        return (
            "PLEATED REPLACEMENT LOCK:\n"
            "- Remove any pre-existing roller/roman/venetian hardware before installing pleated system.\n"
            "- Pleated fabric must be privacy-opaque: no visible outdoor trees/background through the fabric.\n"
            "- Light diffusion is allowed, but no scene details behind pleated fabric.\n"
            "- No leftover top roller tubes, chains, cassettes, or duplicated old blinds in final image.\n\n"
        )
    if type_key == "aluminum_venetian":
        return (
            "ALUMINUM VENETIAN ANTI-LEAK LOCK:\n"
            "- Use references only for slat profile/material/color (black aluminum mini-blind look).\n"
            "- Never transfer reference interior style (no brick walls, no furniture changes, no shelf/radiator replacement).\n"
            "- Keep original room architecture and window surroundings from Ref1 unchanged.\n\n"
        )
    return ""


def _lighting_lock(scene_lighting: str | None = None) -> str:
    base = (
        "LIGHTING & TIME LOCK (STRICT):\n"
        "- Keep global exposure, white balance, contrast, and noise level from Ref1.\n"
        "- Keep the same time-of-day mood as Ref1 (day stays day, evening stays evening, night stays night).\n"
        "- Keep outside window brightness and weather cues from Ref1.\n"
        "- Forbidden: global relighting, HDR-style brighten, sunlight injection, or day/night conversion.\n"
    )
    if scene_lighting == "low_light":
        base += (
            "- Ref1 is low-light/night scene: preserve dark outdoor view and indoor low-light appearance.\n"
            "- Do not brighten the scene to daylight.\n"
        )
    return base + "\n"


def _color_override_lock(*, type_key: str, color_title: str) -> str:
    normalized = re.sub(r"[^a-zа-я0-9]+", " ", (color_title or "").casefold().replace("ё", "е")).strip()
    if "бел" in normalized or "white" in normalized:
        wood_hint = "painted wooden slats in neutral white tone" if type_key == "wood_venetian" else "neutral white tone"
        return (
            "WHITE COLOR OVERRIDE (STRICT):\n"
            f"- Final product color must be {wood_hint}.\n"
            "- Allowed range: neutral white / cool white / light ivory only.\n"
            "- Forbidden: beige, tan, cream-brown, honey wood, yellow cast.\n"
            "- Keep room white balance from Ref1; do not warm up the product color.\n\n"
        )
    return ""


def build_nanobanana_prompt(
    *,
    preset: CurtainPreset,
    prompt_version: str = PROMPT_VERSION_ITOG_1,
    scene_lighting: str | None = None,
) -> str:
    type_key = _normalize_type_key(preset.type_title)
    profile_lock = _type_mount_lock(type_key, preset.mount_mode)

    closed_lock = ""
    if type_key in {"venetian", "aluminum_venetian", "wood_venetian"}:
        closed_lock = (
            "SLAT POSITION LOCK:\n"
            "- Fully closed privacy position; no visible open tilt gaps.\n"
            "- Blind body must be fully lowered by height to the closed target line.\n"
            "- Keep slat pitch and tilt consistent within each module.\n\n"
        )
    if type_key == "roller":
        closed_lock = (
            "FABRIC PANEL LOCK:\n"
            "- Keep roller fabric perfectly planar with straight lower edge.\n"
            "- Rollers must be fully lowered/closed on all openings.\n"
            "- No visible top-gap or mid-gap above any covered sash area.\n"
            "- In sash mode, each sash area must be covered to the closed target line.\n"
            "- No accordion folds and no slat-like segmentation.\n\n"
        )
    if type_key == "pleated":
        closed_lock = (
            "PLEAT GEOMETRY LOCK:\n"
            "- Uniform pleat spacing and stable fold rhythm.\n"
            "- Pleated modules must be fully closed in all openings/sashes.\n"
            "- Pleated fabric may diffuse light but must not be see-through for exterior details.\n"
            "- No local fold collapse, no chaotic deformation.\n\n"
        )

    return (
        "Photorealistic interior edit with strict geometry preservation and product-faithful installation.\n\n"
        "REFERENCE ORDER (STRICT):\n"
        "Ref1 = client's room photo (immutable base canvas).\n"
        "Ref2..RefN = selected product references (style/material/hardware only).\n\n"
        "BASE IMAGE RULE (HIGHEST PRIORITY):\n"
        "- Always edit Ref1 only.\n"
        "- Never replace Ref1 with any catalog/reference interior.\n"
        "- Keep Ref1 room identity, architecture, and camera framing unchanged.\n\n"
        f"{_global_scene_lock()}"
        f"{_lighting_lock(scene_lighting)}"
        f"TASK:\nInstall selected window covering type={preset.type_title}, mount={preset.mount_mode_title}.\n"
        "If old curtains/tulle/blinds exist in Ref1, remove them and replace with selected system only.\n"
        "Keep physically realistic mounting, scale, alignment, and shadows.\n"
        "Color/material must be inferred from Ref2..RefN references; do not invent a new colorway.\n\n"
        f"{_mount_mode_explicit_lock(preset.mount_mode)}"
        f"{_edit_zone_lock(preset.mount_mode)}"
        f"{_full_closure_lock(preset.mount_mode)}"
        f"{_full_drop_height_lock(preset.mount_mode)}"
        f"{profile_lock}"
        f"{closed_lock}"
        f"{_type_specific_lock(type_key)}"
        f"{_color_override_lock(type_key=type_key, color_title=preset.color_title)}"
        "COLOR LOCK:\n"
        "- Match color palette and tone to the provided product references only.\n"
        "- Keep realistic saturation and white balance under room lighting.\n\n"
        "REALISM LOCK:\n"
        "- Preserve natural light/weather cues from Ref1 (indoor-outdoor consistency).\n"
        "- Output must look like a real photo, not CGI.\n"
        "- Keep clean edges, realistic contact shadows, and material micro-texture.\n\n"
        f"{_global_negative_lock()}"
        "- For roller presets: do not copy reference-only room lighting accents (no LED strips under sills/benches).\n"
        "- For roller presets: do not add new tabletop objects (no bottles, remotes, napkin holders, or any new items).\n"
        "- For opening mode: never split one opening into separate sash modules.\n"
        "- For sash mode: never merge neighboring sashes into one module.\n"
        "- No extra vertical plastic strip between neighboring openings unless present in Ref1."
    )


def build_style_reference_images(preset: CurtainPreset) -> list[Path]:
    # Build style-focused references to reduce layout leakage from example interiors.
    STYLE_REFS_DIR.mkdir(parents=True, exist_ok=True)
    base = preset.reference_images[0]
    generated: list[Path] = []

    with Image.open(base).convert("RGB") as image:
        w, h = image.size
        crops = [
            ("crop_opening_whole", (int(w * 0.08), int(h * 0.04), int(w * 0.92), int(h * 0.96))),
            ("crop_headrail", (int(w * 0.05), int(h * 0.02), int(w * 0.95), int(h * 0.28))),
            ("crop_slats_linearity", (int(w * 0.12), int(h * 0.42), int(w * 0.88), int(h * 0.72))),
            ("crop_slats", (int(w * 0.22), int(h * 0.30), int(w * 0.78), int(h * 0.88))),
        ]

        for suffix, box in crops:
            out_path = STYLE_REFS_DIR / f"{preset.preset_id}_{suffix}.jpg"
            image.crop(box).save(out_path, format="JPEG", quality=95)
            generated.append(out_path)

    return generated


def _download_image_sync(url: str, destination: Path) -> Path:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CurtainBot/1.0",
            "Accept": "image/*,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        destination.write_bytes(response.read())
    return destination


def _normalize_to_input_dimensions(*, source_input: Path, generated_input: Path, destination: Path) -> tuple[tuple[int, int], tuple[int, int]]:
    with Image.open(source_input) as src_im:
        source_size = src_im.size
    with Image.open(generated_input).convert("RGB") as gen_im:
        generated_size = gen_im.size
        if generated_size == source_size:
            gen_im.save(destination, format="JPEG", quality=95)
            return source_size, generated_size

        # Never crop generated content: enforce exact canvas size to avoid zoom-like truncation.
        normalized = gen_im.resize(source_size, Image.Resampling.LANCZOS)
        normalized.save(destination, format="JPEG", quality=95)
        return source_size, generated_size


def _scene_mae_diff(*, source_input: Path, generated_input: Path) -> float:
    with Image.open(source_input).convert("L") as src_im:
        src_small = src_im.resize((128, 128), Image.Resampling.BILINEAR)
    with Image.open(generated_input).convert("L") as gen_im:
        gen_small = gen_im.resize((128, 128), Image.Resampling.BILINEAR)
    src_bytes = src_small.tobytes()
    gen_bytes = gen_small.tobytes()
    total = 0
    for a, b in zip(src_bytes, gen_bytes, strict=True):
        total += abs(a - b)
    return total / len(src_bytes)


def _is_scene_drifted(*, source_input: Path, generated_input: Path) -> bool:
    threshold = float(os.getenv("SCENE_DRIFT_MAE_THRESHOLD", "46"))
    diff = _scene_mae_diff(source_input=source_input, generated_input=generated_input)
    LOGGER.info("scene_similarity_check mae_diff=%.2f threshold=%.2f", diff, threshold)
    return diff >= threshold


def _ratio_to_float(ratio: str) -> float:
    left, right = ratio.split(":", 1)
    return float(left) / float(right)


def _closest_supported_aspect_ratio(width: int, height: int) -> str:
    if width <= 0 or height <= 0:
        return DEFAULT_ASPECT_RATIO
    target = width / height
    return min(SUPPORTED_ASPECT_RATIOS, key=lambda item: abs(_ratio_to_float(item) - target))


def choose_request_aspect_ratio(*, source_image_path: Path) -> str:
    configured = (os.getenv("NANOBANANA_ASPECT_RATIO") or "").strip()
    if configured and configured.lower() != "auto":
        return configured

    try:
        with Image.open(source_image_path) as src_im:
            width, height = src_im.size
    except Exception:
        return DEFAULT_ASPECT_RATIO
    return _closest_supported_aspect_ratio(width, height)


async def save_telegram_image(message: Message, destination_without_ext: Path) -> Path:
    file_id: str | None = None
    extension = ".jpg"

    if message.photo:
        file_id = message.photo[-1].file_id
        extension = ".jpg"
    elif message.document and (message.document.mime_type or "").startswith("image/"):
        file_id = message.document.file_id
        if message.document.file_name:
            doc_ext = Path(message.document.file_name).suffix.lower()
            if doc_ext in {".jpg", ".jpeg", ".png", ".webp"}:
                extension = doc_ext
    else:
        raise ValueError("No image photo/document in message")

    destination = destination_without_ext.with_suffix(extension)
    file_info = await message.bot.get_file(file_id)
    buffer = BytesIO()
    await message.bot.download_file(file_info.file_path, destination=buffer)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(buffer.getvalue())
    return destination.resolve()


def _upload_to_catbox_sync(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(f"Missing file for upload: {file_path}")

    boundary = f"----aiogram-{uuid.uuid4().hex}"
    mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    payload = (
        f"--{boundary}\r\n"
        "Content-Disposition: form-data; name=\"reqtype\"\r\n\r\n"
        "fileupload\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="fileToUpload"; filename="{file_path.name}"\r\n'
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8") + file_path.read_bytes() + f"\r\n--{boundary}--\r\n".encode("utf-8")

    request = urllib.request.Request(
        CATBOX_UPLOAD_ENDPOINT,
        data=payload,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "User-Agent": "Mozilla/5.0",
        },
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        raw = response.read().decode("utf-8", errors="replace")
    url = raw.strip()
    if not url.startswith("https://"):
        raise RuntimeError(f"Catbox upload error: {raw[:400]}")
    return url


def _upload_to_0x0_sync(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(f"Missing file for upload: {file_path}")

    boundary = f"----aiogram-{uuid.uuid4().hex}"
    mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    payload = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{file_path.name}"\r\n'
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8") + file_path.read_bytes() + f"\r\n--{boundary}--\r\n".encode("utf-8")

    request = urllib.request.Request(
        ZEROX0_UPLOAD_ENDPOINT,
        data=payload,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "User-Agent": "Mozilla/5.0",
        },
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        raw = response.read().decode("utf-8", errors="replace")
    url = raw.strip()
    if not url.startswith("https://"):
        raise RuntimeError(f"0x0 upload error: {raw[:400]}")
    return url


def _upload_to_tmpfiles_sync(file_path: Path) -> str:
    if not file_path.exists():
        raise FileNotFoundError(f"Missing file for upload: {file_path}")

    boundary = f"----aiogram-{uuid.uuid4().hex}"
    mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    payload = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{file_path.name}"\r\n'
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8") + file_path.read_bytes() + f"\r\n--{boundary}--\r\n".encode("utf-8")

    request = urllib.request.Request(
        TMPFILES_UPLOAD_ENDPOINT,
        data=payload,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        raw = response.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    if not data.get("status") or "data" not in data:
        raise RuntimeError(f"tmpfiles upload error: {raw[:400]}")
    page_url = str(data["data"].get("url") or "").strip()
    if page_url.startswith("http://tmpfiles.org/"):
        page_url = "https://" + page_url[len("http://") :]
    if not page_url.startswith("https://tmpfiles.org/"):
        raise RuntimeError(f"tmpfiles upload url error: {raw[:400]}")
    # Convert landing page URL to direct file URL.
    direct_url = page_url.replace("https://tmpfiles.org/", "https://tmpfiles.org/dl/")
    return direct_url


async def upload_file_to_catbox_with_retries(file_path: Path, max_attempts: int = 3) -> str:
    # Kept name for backward compatibility, but now acts as generic public uploader.
    host_order_raw = (os.getenv("REF_UPLOAD_HOST_ORDER") or "tmpfiles,catbox,0x0").lower()
    host_order = [item.strip() for item in host_order_raw.split(",") if item.strip()]
    host_order = [item for item in host_order if item in {"tmpfiles", "catbox", "0x0"}] or ["tmpfiles", "catbox", "0x0"]

    upload_map: dict[str, Any] = {
        "tmpfiles": _upload_to_tmpfiles_sync,
        "catbox": _upload_to_catbox_sync,
        "0x0": _upload_to_0x0_sync,
    }

    last_exc: Exception | None = None
    per_host_attempts = max(1, min(3, max_attempts))
    for host in host_order:
        uploader = upload_map[host]
        for attempt in range(1, per_host_attempts + 1):
            try:
                url = await asyncio.to_thread(uploader, file_path)
                LOGGER.info("upload_success path=%s host=%s", file_path, host)
                return url
            except Exception as exc:
                last_exc = exc
                LOGGER.warning(
                    "upload_failed path=%s host=%s attempt=%s/%s error=%s",
                    file_path,
                    host,
                    attempt,
                    per_host_attempts,
                    str(exc)[:300],
                )
                if attempt < per_host_attempts:
                    await asyncio.sleep(0.8 * attempt)

    raise RuntimeError(f"Upload failed for {file_path}") from last_exc


async def upload_refs_to_public_urls_with_retries(
    *,
    local_paths: list[Path],
    user_id: int,
    max_attempts: int = 3,
) -> list[str]:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            concurrency = int(os.getenv("REF_UPLOAD_CONCURRENCY", "4"))
            concurrency = max(1, min(8, concurrency))
            sem = asyncio.Semaphore(concurrency)

            async def _upload_one(ref_path: Path) -> str:
                async with sem:
                    return await upload_file_to_catbox_with_retries(ref_path, max_attempts=2)

            urls = await asyncio.gather(*[_upload_one(ref_path) for ref_path in local_paths])
            return urls
        except Exception as exc:
            last_exc = exc
            LOGGER.warning(
                "reference_upload_attempt_failed user_id=%s attempt=%s/%s error=%s",
                user_id,
                attempt,
                max_attempts,
                str(exc)[:350],
            )
            if attempt < max_attempts:
                await asyncio.sleep(3 * attempt)
    raise RuntimeError("reference_upload_retries_exhausted") from last_exc


def _is_true_env(name: str, default: bool = False) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _public_image_url_available_sync(url: str) -> bool:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CurtainBot/1.0",
            "Accept": "image/*,*/*;q=0.8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            content_type = (response.headers.get("Content-Type") or "").lower()
            sample = response.read(256)
        return bool(sample) and ("image" in content_type or url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")))
    except Exception:
        return False


async def ensure_public_refs_available(
    *,
    local_paths: list[Path],
    urls: list[str],
    user_id: int,
) -> list[str]:
    if len(local_paths) != len(urls):
        return urls

    async def wait_public_url_available(url: str, attempts: int = 5, delay_sec: float = 1.6) -> bool:
        for _ in range(attempts):
            ok = await asyncio.to_thread(_public_image_url_available_sync, url)
            if ok:
                return True
            await asyncio.sleep(delay_sec)
        return False

    verified: list[str] = []
    for local_path, url in zip(local_paths, urls, strict=True):
        ok = await wait_public_url_available(url)
        if ok:
            verified.append(url)
            continue

        LOGGER.warning(
            "public_ref_unavailable user_id=%s path=%s url=%s -> reupload",
            user_id,
            local_path,
            url,
        )
        replacement_url = await upload_file_to_catbox_with_retries(local_path, max_attempts=3)
        replacement_ok = await wait_public_url_available(replacement_url)
        if not replacement_ok:
            try:
                alt_url = await asyncio.to_thread(_upload_to_0x0_sync, local_path)
                alt_ok = await wait_public_url_available(alt_url)
                if alt_ok:
                    LOGGER.info(
                        "public_ref_alt_host_success user_id=%s path=%s host=0x0.st",
                        user_id,
                        local_path,
                    )
                    verified.append(alt_url)
                    continue
            except Exception as alt_exc:
                LOGGER.warning(
                    "public_ref_alt_host_failed user_id=%s path=%s host=0x0.st error=%s",
                    user_id,
                    local_path,
                    str(alt_exc)[:300],
                )
            try:
                tmp_url = await asyncio.to_thread(_upload_to_tmpfiles_sync, local_path)
                tmp_ok = await wait_public_url_available(tmp_url)
                if tmp_ok:
                    LOGGER.info(
                        "public_ref_alt_host_success user_id=%s path=%s host=tmpfiles.org",
                        user_id,
                        local_path,
                    )
                    verified.append(tmp_url)
                    continue
            except Exception as tmp_exc:
                LOGGER.warning(
                    "public_ref_alt_host_failed user_id=%s path=%s host=tmpfiles.org error=%s",
                    user_id,
                    local_path,
                    str(tmp_exc)[:300],
                )
            LOGGER.warning(
                "public_ref_still_unavailable user_id=%s path=%s replacement_url=%s",
                user_id,
                local_path,
                replacement_url,
            )
            # Best effort fallback: keep original URL and let NanoBanana decide.
            verified.append(url)
            continue
        verified.append(replacement_url)

    return verified


def _http_json_sync(
    *,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    query: dict[str, str] | None = None,
    timeout: int = 90,
) -> dict[str, Any]:
    final_url = url
    if query:
        final_url = f"{url}?{urllib.parse.urlencode(query)}"

    data_bytes = None
    request_headers = dict(headers or {})
    request_headers.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CurtainBot/1.0")
    if body is not None:
        data_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(final_url, data=data_bytes, method=method, headers=request_headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8", errors="replace")
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {raw[:800]}") from exc


async def submit_nanobanana_job(*, api_key: str, body: dict[str, Any]) -> dict[str, Any]:
    endpoint = os.getenv("NANOBANANA_API_ENDPOINT", DEFAULT_NANOBANANA_ENDPOINT)
    return await asyncio.to_thread(
        _http_json_sync,
        method="POST",
        url=endpoint,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        body=body,
    )


async def submit_nanobanana_job_with_retries(
    *,
    api_key: str,
    body: dict[str, Any],
    max_attempts: int = 3,
) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await submit_nanobanana_job(api_key=api_key, body=body)
        except Exception as exc:
            last_exc = exc
            error_text = str(exc)
            retryable = (
                "HTTP 403: error code: 1010" in error_text
                or "timed out" in error_text.lower()
                or "temporarily unavailable" in error_text.lower()
            )
            LOGGER.warning(
                "nanobanana_submit_failed attempt=%s/%s retryable=%s error=%s",
                attempt,
                max_attempts,
                retryable,
                error_text[:300],
            )
            if (not retryable) or attempt >= max_attempts:
                break
            await asyncio.sleep(2 * attempt)
    raise RuntimeError("nanobanana_submit_retries_exhausted") from last_exc


async def get_nanobanana_task(*, api_key: str, task_id: str) -> dict[str, Any]:
    endpoint = os.getenv("NANOBANANA_TASK_ENDPOINT", DEFAULT_NANOBANANA_TASK_ENDPOINT)
    return await asyncio.to_thread(
        _http_json_sync,
        method="GET",
        url=endpoint,
        headers={"Authorization": f"Bearer {api_key}"},
        query={"taskId": task_id},
    )


def extract_result_url(response_data: Any) -> str | None:
    if response_data is None:
        return None
    if isinstance(response_data, str):
        raw = response_data.strip()
        if raw.startswith(("http://", "https://")):
            return raw
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        return extract_result_url(parsed)
    if isinstance(response_data, list):
        for item in response_data:
            url = extract_result_url(item)
            if url:
                return url
        return None
    if isinstance(response_data, dict):
        for candidate in (
            response_data.get("resultImageUrl"),
            response_data.get("imageUrl"),
            response_data.get("url"),
            response_data.get("result"),
            response_data.get("images"),
            response_data.get("resultImages"),
            response_data.get("data"),
        ):
            url = extract_result_url(candidate)
            if url:
                return url
        return None
    return None


async def wait_for_nanobanana_result(*, api_key: str, task_id: str) -> tuple[str, dict[str, Any]]:
    max_wait = int(os.getenv("NANOBANANA_MAX_WAIT_SECONDS", "600"))
    interval = float(os.getenv("NANOBANANA_POLL_INTERVAL_SECONDS", "4"))
    started = time.monotonic()

    while True:
        status_response = await get_nanobanana_task(api_key=api_key, task_id=task_id)
        data = status_response.get("data", {}) if isinstance(status_response, dict) else {}
        status = str(data.get("status", "")).lower()
        result_url = extract_result_url(data.get("response") or data)

        if status in {"success", "succeed", "completed", "done"} and result_url:
            return result_url, status_response
        if status in {"failed", "error"}:
            raise RuntimeError(f"NanoBanana task failed: {json.dumps(status_response, ensure_ascii=False)[:1000]}")
        if result_url and not status:
            return result_url, status_response

        if time.monotonic() - started >= max_wait:
            raise TimeoutError(f"NanoBanana polling timeout for taskId={task_id}")
        await asyncio.sleep(interval)


async def generate_with_retries(
    *,
    api_key: str,
    body: dict[str, Any],
    user_id: int,
    max_attempts: int = 3,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            submit_response = await submit_nanobanana_job_with_retries(
                api_key=api_key,
                body=body,
                max_attempts=3,
            )
            task_id = submit_response.get("data", {}).get("taskId") if isinstance(submit_response, dict) else None
            if not task_id:
                raise RuntimeError(
                    f"NanoBanana response has no taskId: {json.dumps(submit_response, ensure_ascii=False)[:800]}"
                )

            result_url, final_status = await wait_for_nanobanana_result(api_key=api_key, task_id=task_id)
            return submit_response, result_url, final_status
        except Exception as exc:
            last_exc = exc
            LOGGER.warning(
                "generation_attempt_failed user_id=%s attempt=%s/%s error=%s",
                user_id,
                attempt,
                max_attempts,
                str(exc)[:350],
            )
            if attempt < max_attempts:
                await asyncio.sleep(4 * attempt)
    raise RuntimeError("generation_retries_exhausted") from last_exc


def save_debug_payload(payload: dict[str, Any], user_id: int) -> Path:
    path = PAYLOADS_DIR / f"simple_curtain_debug_{user_id}_{int(time.time())}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def choose_style_refs(preset: CurtainPreset) -> list[Path]:
    refs = list(preset.reference_images)
    if not refs:
        return []
    configured = (os.getenv("NANOBANANA_STYLE_REFS_COUNT") or "").strip()
    if configured:
        desired = max(3, min(5, int(configured)))
    else:
        # Default strategy: use all available references up to 5.
        desired = max(3, min(5, len(refs)))
    selected = refs[:desired]
    while len(selected) < desired:
        selected.append(refs[len(selected) % len(refs)])
    return selected


def build_wood_sash_style_refs(*, preset: CurtainPreset, source_refs: list[Path]) -> list[Path]:
    """Reduce layout leakage: keep only slat texture/tone from catalog refs."""
    STYLE_REFS_DIR.mkdir(parents=True, exist_ok=True)
    prepared: list[Path] = []

    for idx, source in enumerate(source_refs, start=1):
        with Image.open(source).convert("RGB") as image:
            w, h = image.size
            # Crop away frame/room context; keep central slat field only.
            box = (int(w * 0.22), int(h * 0.26), int(w * 0.78), int(h * 0.76))
            cropped = image.crop(box)
            out_path = STYLE_REFS_DIR / f"{preset.preset_id}_wood_sash_{idx}.jpg"
            cropped.save(out_path, format="JPEG", quality=95)
            prepared.append(out_path)
    return prepared


def build_roller_style_refs(*, preset: CurtainPreset, source_refs: list[Path]) -> list[Path]:
    """Keep only roller fabric/cassette cues and remove room-context leakage."""
    STYLE_REFS_DIR.mkdir(parents=True, exist_ok=True)
    prepared: list[Path] = []

    for idx, source in enumerate(source_refs, start=1):
        with Image.open(source).convert("RGB") as image:
            w, h = image.size
            # Prefer upper-middle area: cassette + fabric texture, avoid lower room details.
            box = (int(w * 0.20), int(h * 0.10), int(w * 0.80), int(h * 0.58))
            cropped = image.crop(box)
            out_path = STYLE_REFS_DIR / f"{preset.preset_id}_roller_{idx}.jpg"
            cropped.save(out_path, format="JPEG", quality=95)
            prepared.append(out_path)
    return prepared


def build_aluminum_style_refs(*, preset: CurtainPreset, source_refs: list[Path]) -> list[Path]:
    """Keep only mini-blind cues and minimize interior leakage from references."""
    STYLE_REFS_DIR.mkdir(parents=True, exist_ok=True)
    prepared: list[Path] = []

    for idx, source in enumerate(source_refs, start=1):
        with Image.open(source).convert("RGB") as image:
            w, h = image.size
            # Central slat area: avoids brick walls/furniture near frame edges.
            box = (int(w * 0.24), int(h * 0.18), int(w * 0.76), int(h * 0.80))
            cropped = image.crop(box)
            out_path = STYLE_REFS_DIR / f"{preset.preset_id}_aluminum_{idx}.jpg"
            cropped.save(out_path, format="JPEG", quality=95)
            prepared.append(out_path)
    return prepared


def build_generic_style_refs(*, preset: CurtainPreset, source_refs: list[Path]) -> list[Path]:
    """Default anti-leak prep: keep product texture/hardware cues, suppress room context."""
    STYLE_REFS_DIR.mkdir(parents=True, exist_ok=True)
    prepared: list[Path] = []

    for idx, source in enumerate(source_refs, start=1):
        with Image.open(source).convert("RGB") as image:
            w, h = image.size
            # Keep central product area; trim side walls/furniture/background from catalog shots.
            box = (int(w * 0.18), int(h * 0.14), int(w * 0.82), int(h * 0.86))
            cropped = image.crop(box)
            out_path = STYLE_REFS_DIR / f"{preset.preset_id}_generic_{idx}.jpg"
            cropped.save(out_path, format="JPEG", quality=95)
            prepared.append(out_path)
    return prepared


@ROUTER.message(Command("start"))
async def start_flow(message: Message, state: FSMContext) -> None:
    if not PRESETS:
        await message.answer("Каталог не загружен. Проверьте папку ReferenceStore / Референсы штор.")
        return

    data = await state.get_data()
    old_ids = [int(x) for x in (data.get("type_collage_message_ids") or []) if isinstance(x, int) or str(x).isdigit()]
    old_ids += [int(x) for x in (data.get("mount_example_message_ids") or []) if isinstance(x, int) or str(x).isdigit()]
    old_ids += [int(x) for x in (data.get("room_example_message_ids") or []) if isinstance(x, int) or str(x).isdigit()]
    old_ids += [int(x) for x in (data.get("color_preview_message_ids") or []) if isinstance(x, int) or str(x).isdigit()]
    old_ids += [int(x) for x in (data.get("ui_message_ids") or []) if isinstance(x, int) or str(x).isdigit()]
    if old_ids:
        await _safe_delete_messages(message.bot, chat_id=message.chat.id, message_ids=old_ids)

    await state.clear()
    await state.set_state(Flow.waiting_privacy)
    await state.update_data(
        session_id=str(int(time.time())),
        type_collage_message_ids=[],
        mount_example_message_ids=[],
        room_example_message_ids=[],
        color_preview_message_ids=[],
        ui_message_ids=[],
    )
    first_name = (message.from_user.first_name if message.from_user else "") or "ИМЯ"
    await message.answer(
        f"Здравствуйте, {first_name}, давайте сделаем визуализацию!\n\n"
        "Ознакомьтесь с нашей политикой конфиденциальности "
        f"({os.getenv('PRIVACY_POLICY_URL', 'https://your-domain.com/privacy')}) "
        "и дайте согласие на обработку персональных данных\n\n"
        "К сожалению, без этого никак(",
        reply_markup=build_privacy_keyboard(),
    )


@ROUTER.message(StateFilter(None))
async def start_from_any_message(message: Message, state: FSMContext) -> None:
    await start_flow(message, state)


@ROUTER.message(F.text == SECRET_RESET_CODE)
async def reset_generation_limit(message: Message, state: FSMContext) -> None:
    user = message.from_user
    if not user:
        await message.answer("Не удалось определить пользователя. Попробуйте /start.")
        return
    _set_user_progress(user.id, generations_used=0, verified=True)
    await message.answer(
        f"Секретный код принят.\nЛимит обновлен: доступно {MAX_FREE_GENERATIONS} генераций."
    )
    await start_flow(message, state)


async def _render_type_step(target_message: Message, state: FSMContext) -> None:
    await state.set_state(Flow.waiting_type)
    await _delete_tracked_messages(state, target_message.bot, chat_id=target_message.chat.id, key="ui_message_ids")
    await _send_type_collage(target_message, state)
    await _send_tracked_text(
        target_message,
        state,
        "Отлично, спасибо!\nТеперь выберите, какие жалюзи вы хотите визуализировать",
        reply_markup=build_type_keyboard(),
    )


def _mount_title(mode: str) -> str:
    return TXT_OPENING if mode == "opening" else TXT_SASH


def _forced_mount_mode(type_option: CurtainTypeOption) -> str | None:
    type_key = _normalize_type_key(type_option.title)
    if type_key in {"aluminum_venetian", "pleated"}:
        return "sash" if "sash" in type_option.mount_modes else (type_option.mount_modes[0] if type_option.mount_modes else None)
    if len(type_option.mount_modes) == 1:
        return type_option.mount_modes[0]
    return None


@ROUTER.callback_query(Flow.waiting_privacy, F.data == "privacy:agree")
async def accept_privacy(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Принято")
    if not callback.message:
        return
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _render_type_step(callback.message, state)


@ROUTER.callback_query(Flow.waiting_type, F.data.startswith("type:"))
async def pick_type(callback: CallbackQuery, state: FSMContext) -> None:
    type_id = callback.data.split(":", 1)[1]
    type_option = TYPE_OPTIONS.get(type_id)
    if not type_option:
        await callback.answer("Неизвестный тип штор.", show_alert=True)
        return

    if callback.message:
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="type_collage_message_ids")
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="ui_message_ids")

    await state.update_data(selected_type_id=type_option.type_id, selected_type_title=type_option.title)
    forced_mode = _forced_mount_mode(type_option)
    await callback.answer(f"Выбрано: {type_option.title}")
    if callback.message:
        try:
            await callback.message.delete()
        except Exception:
            pass
        if forced_mode in {"opening", "sash"}:
            mount_title = _mount_title(forced_mode)
            await state.update_data(mount_mode=forced_mode, mount_mode_title=mount_title)
            await state.set_state(Flow.waiting_color)
            await _send_color_previews(callback.message, state, type_id=type_option.type_id, mount_mode=forced_mode)
            await _send_tracked_text(
                callback.message,
                state,
                "Выберите цвет.",
                reply_markup=build_color_keyboard_v2(type_id=type_option.type_id, mount_mode=forced_mode),
            )
        else:
            await state.set_state(Flow.waiting_mount_mode)
            await _send_mount_mode_examples(callback.message, state, type_id=type_option.type_id)
            await _send_tracked_text(
                callback.message,
                state,
                "Выберите куда нужно установить изделия",
                reply_markup=build_mount_mode_keyboard_v2(type_id=type_option.type_id),
            )


@ROUTER.callback_query(Flow.waiting_mount_mode, F.data == "back:type")
@ROUTER.callback_query(Flow.waiting_color, F.data == "back:type")
async def back_to_type(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Назад")
    if not callback.message:
        return
    await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="color_preview_message_ids")
    await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="mount_example_message_ids")
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _render_type_step(callback.message, state)


@ROUTER.callback_query(Flow.waiting_mount_mode, F.data.startswith("mount:"))
async def pick_mount_mode(callback: CallbackQuery, state: FSMContext) -> None:
    mode = callback.data.split(":", 1)[1]
    if mode not in {"opening", "sash"}:
        await callback.answer("Неизвестный режим установки.", show_alert=True)
        return

    data = await state.get_data()
    type_id = str(data.get("selected_type_id") or "")
    type_option = TYPE_OPTIONS.get(type_id)
    if not type_option:
        await callback.answer("Сначала выберите тип штор.", show_alert=True)
        return

    if mode not in type_option.mount_modes:
        await callback.answer("Этот режим недоступен для выбранного типа.", show_alert=True)
        return

    mount_title = _mount_title(mode)
    await state.update_data(mount_mode=mode, mount_mode_title=mount_title)
    await state.set_state(Flow.waiting_color)
    await callback.answer(f"Выбрано: {mount_title}")
    if callback.message:
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="ui_message_ids")
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="mount_example_message_ids")
        try:
            await callback.message.delete()
        except Exception:
            pass
        await _send_color_previews(callback.message, state, type_id=type_id, mount_mode=mode)
        await _send_tracked_text(
            callback.message,
            state,
            "Шаг 3/3. Выберите цвет.",
            reply_markup=build_color_keyboard_v2(type_id=type_id, mount_mode=mode),
        )


@ROUTER.callback_query(Flow.waiting_color, F.data == "back:mount")
async def back_to_mount(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    type_id = str(data.get("selected_type_id") or "")
    type_option = TYPE_OPTIONS.get(type_id)
    if not type_option:
        await callback.answer("Сначала выберите тип штор.", show_alert=True)
        return

    forced_mode = _forced_mount_mode(type_option)
    await callback.answer("Назад")
    if not callback.message:
        return
    if forced_mode in {"opening", "sash"}:
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="color_preview_message_ids")
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="mount_example_message_ids")
        try:
            await callback.message.delete()
        except Exception:
            pass
        await _render_type_step(callback.message, state)
        return

    await state.set_state(Flow.waiting_mount_mode)
    await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="ui_message_ids")
    await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="color_preview_message_ids")
    await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="mount_example_message_ids")
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _send_mount_mode_examples(callback.message, state, type_id=type_id)
    await _send_tracked_text(
        callback.message,
        state,
        "Выберите куда нужно установить изделия",
        reply_markup=build_mount_mode_keyboard_v2(type_id=type_id),
    )


@ROUTER.callback_query(Flow.waiting_color, F.data.startswith("color:"))
async def pick_color(callback: CallbackQuery, state: FSMContext) -> None:
    preset_id = callback.data.split(":", 1)[1]
    preset = PRESETS.get(preset_id)
    if not preset:
        await callback.answer("Неизвестный цвет/вариант.", show_alert=True)
        return

    data = await state.get_data()
    if preset.type_id != data.get("selected_type_id") or preset.mount_mode != data.get("mount_mode"):
        await callback.answer("Выберите цвет из текущего списка.", show_alert=True)
        return

    await state.update_data(
        selected_preset_id=preset.preset_id,
        selected_preset_title=preset.display_title,
        prompt_version=PROMPT_VERSION_ITOG_1,
        prompt_version_title=PROMPT_VERSION_TITLE,
    )
    await state.set_state(Flow.waiting_room_photo)
    await callback.answer(f"Выбрано: {preset.color_title}")
    if callback.message:
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="ui_message_ids")
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="color_preview_message_ids")
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="mount_example_message_ids")
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer_photo(
            photo=FSInputFile(str(preset.preview_image)),
            caption=f"Выбрано: {preset.type_title} / {preset.mount_mode_title} / {preset.color_title}",
        )
        if EXAMPLE_ROOM_PHOTO_PATH.exists():
            room_example = await callback.message.answer_photo(
                photo=FSInputFile(str(EXAMPLE_ROOM_PHOTO_PATH)),
                caption=(
                    "Просьба присылать фото, где штор не видно "
                    "или они максимально задвинуты. Вот пример идеальной фотографии."
                ),
            )
            await _track_message_id(state, key="room_example_message_ids", message_id=room_example.message_id)
        await _send_tracked_text(
            callback.message,
            state,
            "Отправьте фото комнаты, где нужна установка.\n"
            "Можно обычным фото или файлом.",
        )


@ROUTER.callback_query(Flow.waiting_room_photo, F.data == "back:color")
async def back_to_color(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    type_id = str(data.get("selected_type_id") or "")
    mount_mode = str(data.get("mount_mode") or "")
    if type_id not in TYPE_OPTIONS or mount_mode not in {"opening", "sash"}:
        await callback.answer("Не удалось вернуться, нажмите /start", show_alert=True)
        return
    await state.set_state(Flow.waiting_color)
    await callback.answer("Назад")
    if callback.message:
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="ui_message_ids")
        try:
            await callback.message.delete()
        except Exception:
            pass
        await _send_color_previews(callback.message, state, type_id=type_id, mount_mode=mount_mode)
        await _send_tracked_text(
            callback.message,
            state,
            "Шаг 3/3. Выберите цвет.",
            reply_markup=build_color_keyboard_v2(type_id=type_id, mount_mode=mount_mode),
        )


@ROUTER.callback_query(F.data == "back:start")
async def back_to_start(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Возврат в начало")
    if callback.message:
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="ui_message_ids")
        await _delete_tracked_messages(state, callback.bot, chat_id=callback.message.chat.id, key="color_preview_message_ids")
        try:
            await callback.message.delete()
        except Exception:
            pass
        await start_flow(callback.message, state)


@ROUTER.callback_query(F.data == "offer:manager")
async def offer_manager(callback: CallbackQuery) -> None:
    await callback.answer()


@ROUTER.callback_query(F.data == "offer:callme")
async def offer_callme(callback: CallbackQuery) -> None:
    user = callback.from_user
    if not user:
        await callback.answer("Не удалось определить пользователя.", show_alert=True)
        return

    progress = _get_user_progress(user.id)
    deal_id = int(progress.get("crm_deal_id", 0) or 0)
    already_requested = bool(progress.get("callme_requested", False))
    if already_requested:
        if callback.message:
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=_replace_callme_button_with_done(callback.message.reply_markup)
                )
            except Exception:
                pass
        await callback.answer("Заявка уже оставлена. Скоро с вами свяжутся.", show_alert=False)
        return

    if deal_id <= 0:
        await callback.answer("Сделка не найдена. Сначала пройдите анкету.", show_alert=True)
        return

    try:
        result = await _mark_envy_callme(
            deal_id=deal_id,
            fallback_name=str(progress.get("crm_deal_name") or ""),
        )
        _set_user_progress(
            user.id,
            callme_requested=True,
            crm_employee_id=int(result.get("employee_id", progress.get("crm_employee_id", 0)) or 0),
            crm_deal_name=str(result.get("deal_name") or progress.get("crm_deal_name") or ""),
        )
        await callback.answer("Отлично, передали запрос на звонок.", show_alert=False)
        if callback.message:
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=_replace_callme_button_with_done(callback.message.reply_markup)
                )
            except Exception:
                pass
            await callback.message.answer("Вы оставили заявку на звонок. В ближайшее время с вами свяжутся.")
    except Exception:
        LOGGER.exception("offer_callme_failed user_id=%s deal_id=%s", user.id, deal_id)
        await callback.answer("Не удалось передать запрос на звонок. Попробуйте позже.", show_alert=True)


@ROUTER.callback_query(F.data == "offer:callme_done")
async def offer_callme_done(callback: CallbackQuery) -> None:
    await callback.answer("Заявка уже оставлена. Скоро с вами свяжутся.", show_alert=False)


@ROUTER.callback_query(F.data == "offer:more")
async def offer_more(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Продолжаем")
    if not callback.message:
        return
    await _render_type_step(callback.message, state)


@ROUTER.callback_query(F.data == "offer:limit")
async def offer_limit(callback: CallbackQuery) -> None:
    await callback.answer("Лимит генераций исчерпан", show_alert=True)


async def _send_final_result_with_offer(
    *,
    message: Message,
    result_path: Path | None,
    result_url: str,
    preset_title: str,
    remaining: int,
    callme_requested: bool,
) -> None:
    caption = "✅ Готово, вот ваша визуализация. Как вам?"
    if result_path and result_path.exists():
        await message.answer_photo(photo=FSInputFile(str(result_path)), caption=caption)
    else:
        await message.answer_photo(photo=result_url, caption=caption)

    await message.answer(
        "Напишите нам, чтобы узнать цену таких изделий или проконсультироваться - "
        "мы ответим на любые вопросы!\n\n"
        f"Наш контакт: @{MANAGER_USERNAME}\n\n"
        f"У вас есть еще {max(0, remaining)} попыток визуализации\n"
        "Посмотрите, как будут выглядеть другие изделия на ваших окнах",
        reply_markup=build_offer_keyboard(remaining=remaining, callme_requested=callme_requested),
    )


@ROUTER.message(Flow.waiting_room_photo, F.photo)
@ROUTER.message(Flow.waiting_room_photo, F.document)
async def process_room_photo(message: Message, state: FSMContext) -> None:
    if not PRESETS:
        await message.answer("Каталог не загружен. Перезапустите /start.")
        return
    await _delete_tracked_messages(state, message.bot, chat_id=message.chat.id, key="room_example_message_ids")
    await _delete_tracked_messages(state, message.bot, chat_id=message.chat.id, key="ui_message_ids")

    api_key = os.getenv("NANOBANANA_API_KEY")
    if not api_key:
        await message.answer("NANOBANANA_API_KEY не настроен.")
        return

    data = await state.get_data()
    session_id = data.get("session_id", str(int(time.time())))
    user_id = message.from_user.id if message.from_user else 0
    progress = _get_user_progress(user_id=user_id)
    generations_used = int(progress.get("generations_used", 0))
    is_verified = bool(progress.get("verified", False))
    room_path_base = UPLOADS_DIR / str(user_id) / session_id / "room"
    selected_preset_id = str(data.get("selected_preset_id") or "")
    preset = PRESETS.get(selected_preset_id)
    if preset is None:
        await message.answer("Сначала выберите тип/установку/цвет, затем отправьте фото заново через /start.")
        await state.clear()
        return

    if is_verified and generations_used >= MAX_FREE_GENERATIONS:
        await message.answer(
            "Закончился лимит (5 шт):\n\n"
            "К сожалению, лимиты на генерацию закончились, но наш менеджер может сделать еще!\n"
            "Заодно он посчитает примерную стоимость изделий, а так же проконсультирует по любому вопросу\n\n"
            "Вместе выбирать легче - напишите нам\n"
            f"@{MANAGER_USERNAME}",
            reply_markup=build_limit_keyboard(callme_requested=bool(progress.get("callme_requested", False))),
        )
        await state.clear()
        return

    try:
        room_path = await save_telegram_image(message, room_path_base)
    except ValueError:
        await message.answer("Нужно отправить изображение (фото или файл).")
        return

    show_first_facts = generations_used == 0
    processing = await message.answer(_build_processing_text(include_facts=show_first_facts))
    style_refs = choose_style_refs(preset)
    type_key = _normalize_type_key(preset.type_title)
    if type_key == "wood_venetian" and preset.mount_mode == "sash":
        style_refs = build_wood_sash_style_refs(preset=preset, source_refs=style_refs)
        LOGGER.info(
            "style_refs_prepared user_id=%s mode=wood_sash_slat_crop refs=%s",
            user_id,
            [str(p) for p in style_refs],
        )
    elif type_key == "roller":
        style_refs = build_roller_style_refs(preset=preset, source_refs=style_refs)
        LOGGER.info(
            "style_refs_prepared user_id=%s mode=roller_fabric_crop refs=%s",
            user_id,
            [str(p) for p in style_refs],
        )
    elif type_key == "aluminum_venetian":
        style_refs = build_aluminum_style_refs(preset=preset, source_refs=style_refs)
        LOGGER.info(
            "style_refs_prepared user_id=%s mode=aluminum_slat_crop refs=%s",
            user_id,
            [str(p) for p in style_refs],
        )
    else:
        style_refs = build_generic_style_refs(preset=preset, source_refs=style_refs)
        LOGGER.info(
            "style_refs_prepared user_id=%s mode=generic_anti_leak_crop refs=%s",
            user_id,
            [str(p) for p in style_refs],
        )
    local_refs = [room_path, *[ref.resolve() for ref in style_refs]]
    LOGGER.info(
        "generation_request user_id=%s preset=%s mount=%s refs=%s",
        user_id,
        preset.display_title,
        preset.mount_mode_title,
        [str(p) for p in style_refs],
    )
    request_aspect_ratio = choose_request_aspect_ratio(source_image_path=room_path)

    try:
        public_urls = await upload_refs_to_public_urls_with_retries(
            local_paths=local_refs,
            user_id=user_id,
            max_attempts=3,
        )
        if _is_true_env("REF_UPLOAD_VERIFY_URLS", default=False):
            public_urls = await ensure_public_refs_available(
                local_paths=local_refs,
                urls=public_urls,
                user_id=user_id,
            )
    except Exception:
        LOGGER.exception("reference_upload_failed user_id=%s", user_id)
        await message.answer("Не удалось загрузить референсы. Повторите /start.")
        try:
            await processing.delete()
        except Exception:
            pass
        await state.clear()
        return

    mount_mode = str(data.get("mount_mode") or "opening")
    prompt_version = str(data.get("prompt_version") or PROMPT_VERSION_ITOG_1)
    scene_lighting = _detect_scene_lighting(room_path)
    prompt = build_nanobanana_prompt(
        preset=preset,
        prompt_version=prompt_version,
        scene_lighting=scene_lighting,
    )
    request_body = {
        "prompt": prompt,
        "imageUrls": public_urls,
        "aspectRatio": request_aspect_ratio,
        "resolution": os.getenv("NANOBANANA_RESOLUTION", DEFAULT_RESOLUTION),
        "googleSearch": False,
        "outputFormat": "jpg",
    }
    callback_url = os.getenv("NANOBANANA_CALLBACK_URL")
    if callback_url:
        request_body["callBackUrl"] = callback_url

    try:
        submit_response, result_url, final_status = await generate_with_retries(
            api_key=api_key,
            body=request_body,
            user_id=user_id,
            max_attempts=3,
        )
    except Exception:
        LOGGER.exception("generation_failed user_id=%s", user_id)
        await message.answer("Генерация не удалась. Попробуйте снова через /start.")
        try:
            await processing.delete()
        except Exception:
            pass
        await state.clear()
        return

    ts = int(time.time())
    raw_result_path = GENERATED_DIR / f"raw_{user_id}_{ts}.jpg"
    normalized_result_path = GENERATED_DIR / f"final_{user_id}_{ts}.jpg"
    source_size: tuple[int, int] | None = None
    raw_size: tuple[int, int] | None = None
    normalized_ready = False
    try:
        await asyncio.to_thread(_download_image_sync, result_url, raw_result_path)
        source_size, raw_size = await asyncio.to_thread(
            _normalize_to_input_dimensions,
            source_input=room_path,
            generated_input=raw_result_path,
            destination=normalized_result_path,
        )
        normalized_ready = True
    except Exception:
        LOGGER.exception("postprocess_failed user_id=%s", user_id)

    # Guard against accidental scene replacement by model (different room/interior).
    if normalized_ready and normalized_result_path.exists():
        drifted = await asyncio.to_thread(
            _is_scene_drifted,
            source_input=room_path,
            generated_input=normalized_result_path,
        )
        if drifted:
            LOGGER.warning("scene_drift_detected user_id=%s -> retry_with_guard_prompt", user_id)
            guarded_body = dict(request_body)
            guarded_body["prompt"] = (
                f"{prompt}\n\n"
                "SCENE IDENTITY FAILSAFE (EMERGENCY):\n"
                "- Output MUST keep the same room as Ref1.\n"
                "- If unsure, keep Ref1 unchanged and only add blinds.\n"
                "- It is forbidden to generate another interior, walls, furniture, or layout.\n"
            )
            try:
                submit_response, result_url, final_status = await generate_with_retries(
                    api_key=api_key,
                    body=guarded_body,
                    user_id=user_id,
                    max_attempts=2,
                )
                ts_retry = int(time.time())
                raw_result_path = GENERATED_DIR / f"raw_retry_{user_id}_{ts_retry}.jpg"
                normalized_result_path = GENERATED_DIR / f"final_retry_{user_id}_{ts_retry}.jpg"
                await asyncio.to_thread(_download_image_sync, result_url, raw_result_path)
                source_size, raw_size = await asyncio.to_thread(
                    _normalize_to_input_dimensions,
                    source_input=room_path,
                    generated_input=raw_result_path,
                    destination=normalized_result_path,
                )
                normalized_ready = normalized_result_path.exists()
                if normalized_ready:
                    drifted_again = await asyncio.to_thread(
                        _is_scene_drifted,
                        source_input=room_path,
                        generated_input=normalized_result_path,
                    )
                    if drifted_again:
                        await message.answer(
                            "Не удалось сохранить исходную сцену без искажений. "
                            "Повторите генерацию, пожалуйста."
                        )
                        await state.clear()
                        return
            except Exception:
                LOGGER.exception("scene_drift_retry_failed user_id=%s", user_id)

    debug_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "selected_preset": preset.display_title,
        "selected_type": preset.type_title,
        "selected_color": preset.color_title,
        "prompt_version": prompt_version,
        "prompt_version_title": data.get("prompt_version_title") or PROMPT_VERSION_TITLE,
        "mount_mode": mount_mode,
        "mount_mode_title": data.get("mount_mode_title"),
        "room_photo": str(room_path),
        "preset_reference_images": [str(p) for p in preset.reference_images],
        "style_reference_images": [str(p) for p in style_refs],
        "reference_order": {
            "Ref1": str(room_path),
            "Ref2": str(style_refs[0]) if len(style_refs) > 0 else None,
            "Ref3": str(style_refs[1]) if len(style_refs) > 1 else None,
            "Ref4": str(style_refs[2]) if len(style_refs) > 2 else None,
        },
        "reference_public_urls": public_urls,
        "prompt": prompt,
        "nanobanana_submit_response": submit_response,
        "nanobanana_final_status_response": final_status,
        "result_url": result_url,
        "source_size": source_size,
        "request_aspect_ratio": request_aspect_ratio,
        "raw_result_size": raw_size,
        "raw_result_path": str(raw_result_path),
        "normalized_result_path": str(normalized_result_path) if normalized_ready else None,
    }
    debug_path = save_debug_payload(debug_payload, user_id=user_id)
    LOGGER.info("generation_success user_id=%s result_url=%s debug=%s", user_id, result_url, debug_path)

    try:
        await processing.delete()
    except Exception:
        pass

    preview_source = normalized_result_path if normalized_ready and normalized_result_path.exists() else raw_result_path

    if is_verified and generations_used < MAX_FREE_GENERATIONS:
        new_used = generations_used + 1
        _set_user_progress(user_id, generations_used=new_used, verified=True)
        remaining = MAX_FREE_GENERATIONS - new_used
        try:
            await _send_final_result_with_offer(
                message=message,
                result_path=preview_source if preview_source.exists() else None,
                result_url=result_url,
                preset_title=preset.display_title,
                remaining=remaining,
                callme_requested=bool(progress.get("callme_requested", False)),
            )
        except Exception:
            await message.answer("Не удалось отправить итоговое фото. Попробуйте /start.")
        await state.clear()
        return

    blurred_path = GENERATED_DIR / f"blur_{user_id}_{ts}.jpg"
    try:
        await asyncio.to_thread(_create_blurred_preview, preview_source, blurred_path)
        await message.answer_photo(
            photo=FSInputFile(str(blurred_path)),
            caption=(
                f"Демо-версия: {preset.display_title}\n\n"
                "Демо-версия готова.\n"
                "Чтобы защититься от ботов и спама, подтвердите, что вы человек — "
                "заполните короткую анкету.\n"
                "После анкеты отправим фото без блюра."
            ),
        )
        await message.answer("Заполните короткую анкету.\n\nКак вас зовут?")
    except Exception:
        await message.answer("Не удалось отправить демо-версию. Повторите /start.")
        await state.clear()
        return

    await state.update_data(
        pending_result_path=str(preview_source),
        pending_result_url=result_url,
        pending_preset_title=preset.display_title,
    )
    await state.set_state(Flow.waiting_name)


@ROUTER.message(Flow.waiting_name)
async def receive_name(message: Message, state: FSMContext) -> None:
    name = _validate_name_only(message.text or "")
    if not name:
        await message.answer("Введите только имя (например: Иван).")
        return
    await state.update_data(lead_name=name)
    await state.set_state(Flow.waiting_phone)
    await message.answer("Ваш номер телефона:")


@ROUTER.message(Flow.waiting_phone)
async def receive_phone(message: Message, state: FSMContext) -> None:
    phone_raw = (message.text or "").strip()
    parsed = _validate_name_and_phone("Тест", phone_raw)
    if not parsed:
        await message.answer("Введите корректный номер телефона (10-15 цифр, можно с +).")
        return
    _, phone = parsed
    await state.update_data(lead_phone=phone)
    await state.set_state(Flow.waiting_human_check)
    await message.answer(
        "Подтвердите, что вы человек. Нажмите кнопку ниже.",
        reply_markup=build_human_check_keyboard(),
    )


@ROUTER.callback_query(Flow.waiting_human_check, F.data == "human:ok")
async def confirm_human(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Принято")
    data = await state.get_data()
    user = callback.from_user
    user_id = user.id if user else 0

    name = str(data.get("lead_name") or "").strip()
    phone = str(data.get("lead_phone") or "").strip()
    if not name or not phone:
        await state.set_state(Flow.waiting_name)
        if callback.message:
            await callback.message.answer("Анкета заполнена не полностью. Как вас зовут?")
        return

    lead_payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "name": name,
        "phone": phone,
        "telegram_user_id": user_id,
        "telegram_username": user.username if user else None,
        "selected_preset": data.get("pending_preset_title"),
        "flow": "curtain_demo_verify",
    }
    try:
        _append_lead_record(lead_payload)
    except Exception:
        LOGGER.exception("lead_save_failed")

    progress = _get_user_progress(user_id)
    new_used = min(MAX_FREE_GENERATIONS, int(progress.get("generations_used", 0)) + 1)

    crm_deal_id = int(progress.get("crm_deal_id", 0) or 0)
    crm_client_id = int(progress.get("crm_client_id", 0) or 0)
    crm_employee_id = int(progress.get("crm_employee_id", 0) or 0)
    crm_deal_name = str(progress.get("crm_deal_name") or "")

    if crm_deal_id <= 0:
        try:
            crm_result = await _create_envy_lead_from_form(
                name=name,
                phone=phone,
                telegram_user_id=user_id,
                telegram_username=user.username if user else None,
            )
            if crm_result.get("ok"):
                crm_deal_id = int(crm_result.get("deal_id", 0) or 0)
                crm_client_id = int(crm_result.get("client_id", 0) or 0)
                crm_employee_id = int(crm_result.get("employee_id", 0) or 0)
                crm_deal_name = str(crm_result.get("deal_name") or "")
        except Exception:
            LOGGER.exception("envy_create_lead_failed user_id=%s", user_id)

    _set_user_progress(
        user_id,
        generations_used=new_used,
        verified=True,
        crm_deal_id=crm_deal_id if crm_deal_id > 0 else None,
        crm_client_id=crm_client_id if crm_client_id > 0 else None,
        crm_employee_id=crm_employee_id if crm_employee_id > 0 else None,
        crm_deal_name=crm_deal_name if crm_deal_name else None,
        callme_requested=False,
    )
    remaining = MAX_FREE_GENERATIONS - new_used

    result_path_raw = str(data.get("pending_result_path") or "")
    result_url = str(data.get("pending_result_url") or "")
    result_path = Path(result_path_raw) if result_path_raw else None
    if callback.message:
        try:
            await callback.message.delete()
        except Exception:
            pass
        try:
            await _send_final_result_with_offer(
                message=callback.message,
                result_path=result_path if result_path and result_path.exists() else None,
                result_url=result_url,
                preset_title=str(data.get("pending_preset_title") or ""),
                remaining=remaining,
                callme_requested=False,
            )
        except Exception:
            await callback.message.answer("Не удалось отправить итоговое фото. Попробуйте /start.")

    await state.clear()


@ROUTER.message(Flow.waiting_privacy)
async def waiting_privacy_fallback(message: Message) -> None:
    await message.answer("Нажмите «Согласен», чтобы продолжить.", reply_markup=build_privacy_keyboard())


@ROUTER.message(Flow.waiting_type)
async def waiting_type_fallback(message: Message, state: FSMContext) -> None:
    await _render_type_step(message, state)


@ROUTER.message(Flow.waiting_mount_mode)
async def waiting_mount_mode_fallback(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    type_id = str(data.get("selected_type_id") or "")
    if type_id not in TYPE_OPTIONS:
        await message.answer("Сначала выберите тип штор.", reply_markup=build_type_keyboard())
        return
    await message.answer(
        "Сначала выберите тип установки кнопкой ниже.",
        reply_markup=build_mount_mode_keyboard_v2(type_id=type_id),
    )


@ROUTER.message(Flow.waiting_color)
async def waiting_color_fallback(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    type_id = str(data.get("selected_type_id") or "")
    mount_mode = str(data.get("mount_mode") or "")
    if type_id not in TYPE_OPTIONS or mount_mode not in {"opening", "sash"}:
        await message.answer("Сначала выберите тип и установку.")
        return
    await message.answer(
        "Сначала выберите цвет кнопкой ниже.",
        reply_markup=build_color_keyboard_v2(type_id=type_id, mount_mode=mount_mode),
    )


@ROUTER.message(Flow.waiting_room_photo)
async def waiting_room_photo_fallback(message: Message) -> None:
    await message.answer("Сейчас нужно фото комнаты с окном. Отправьте фото или файл-изображение.")


@ROUTER.message(Flow.waiting_human_check)
async def waiting_human_check_fallback(message: Message) -> None:
    await message.answer("Нажмите кнопку «Я человек» для завершения проверки.")

async def main() -> None:
    load_dotenv()
    ensure_runtime_dirs()
    configure_logging()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    if not os.getenv("NANOBANANA_API_KEY"):
        raise RuntimeError("NANOBANANA_API_KEY is not set")

    global PRESETS, TYPE_OPTIONS, TYPE_START_PREVIEWS
    loaded_presets, loaded_types = load_reference_store_catalog(PROJECT_ROOT)
    preview_root = _resolve_preview_store_root(PROJECT_ROOT)
    loaded_presets = _apply_preview_overrides(
        loaded_presets,
        preview_root=preview_root,
    )
    allowed_type_keys = {"aluminum_venetian", "wood_venetian", "pleated", "roller"}
    allowed_type_ids = {
        type_id
        for type_id, option in loaded_types.items()
        if _normalize_type_key(option.title) in allowed_type_keys
    }
    PRESETS = {preset_id: preset for preset_id, preset in loaded_presets.items() if preset.type_id in allowed_type_ids}
    TYPE_OPTIONS = {type_id: option for type_id, option in loaded_types.items() if type_id in allowed_type_ids}
    TYPE_START_PREVIEWS = _build_type_start_previews(preview_root=preview_root, type_options=TYPE_OPTIONS)
    LOGGER.info("type_start_previews loaded=%s", {k: str(v) for k, v in TYPE_START_PREVIEWS.items()})
    LOGGER.info("catalog_loaded presets=%s types=%s", len(PRESETS), len(TYPE_OPTIONS))

    bot = Bot(token=token)
    dispatcher = Dispatcher()
    dispatcher.include_router(ROUTER)
    await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
