from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

REFERENCE_STORE_CANDIDATES = (
    "ReferenceStore",
    "Референсы штор",
)


@dataclass(frozen=True)
class CurtainPreset:
    preset_id: str
    type_id: str
    type_title: str
    mount_mode: str  # opening | sash
    mount_mode_title: str
    color_title: str
    preview_image: Path
    reference_images: tuple[Path, ...]

    @property
    def display_title(self) -> str:
        return f"{self.type_title} / {self.color_title}"


@dataclass(frozen=True)
class CurtainTypeOption:
    type_id: str
    title: str
    mount_modes: tuple[str, ...]


def _is_image_file(path: Path) -> bool:
    return path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}


def _pick_preview_image(images: list[Path]) -> Path:
    preferred: list[Path] = []
    for image in images:
        stem = image.stem.casefold().replace("ё", "е")
        if stem.startswith("прев") or stem.startswith("prev") or "preview" in stem:
            preferred.append(image)
    if preferred:
        return sorted(preferred, key=lambda p: p.name.casefold())[0]
    return images[0]


def _resolve_reference_store_root(project_root: Path) -> Path:
    for name in REFERENCE_STORE_CANDIDATES:
        candidate = project_root / name
        if candidate.exists() and candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "Reference store not found. Expected one of: "
        + ", ".join(str(project_root / name) for name in REFERENCE_STORE_CANDIDATES)
    )


def _normalize_mount_mode(folder_name: str) -> str | None:
    normalized = folder_name.casefold().replace("ё", "е")
    normalized = re.sub(r"[^a-zа-я0-9]+", " ", normalized).strip()
    if "проем" in normalized:
        return "opening"
    # Handle typo "стоврки" too.
    if "створк" in normalized or "стоврк" in normalized:
        return "sash"
    return None


def mount_mode_title(mount_mode: str) -> str:
    return "На оконный проем" if mount_mode == "opening" else "На каждую створку"


def load_reference_store_catalog(
    project_root: Path,
) -> tuple[dict[str, CurtainPreset], dict[str, CurtainTypeOption]]:
    root = _resolve_reference_store_root(project_root)
    type_dirs = sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name.casefold())

    presets: dict[str, CurtainPreset] = {}
    type_options: dict[str, CurtainTypeOption] = {}

    preset_counter = 1
    for type_index, type_dir in enumerate(type_dirs, start=1):
        type_id = f"t{type_index:02d}"
        mounts_for_type: set[str] = set()

        mount_dirs = sorted([p for p in type_dir.iterdir() if p.is_dir()], key=lambda p: p.name.casefold())
        for mount_dir in mount_dirs:
            mode = _normalize_mount_mode(mount_dir.name)
            if mode is None:
                continue
            mounts_for_type.add(mode)

            color_dirs = sorted([p for p in mount_dir.iterdir() if p.is_dir()], key=lambda p: p.name.casefold())
            for color_dir in color_dirs:
                images = sorted(
                    [p for p in color_dir.iterdir() if p.is_file() and _is_image_file(p)],
                    key=lambda p: p.name.casefold(),
                )
                if not images:
                    continue

                preview = _pick_preview_image(images)
                # Preview is also the first style reference for generation.
                ordered_refs = [preview] + [img for img in images if img != preview]
                preset_id = f"p{preset_counter:03d}"
                preset_counter += 1

                presets[preset_id] = CurtainPreset(
                    preset_id=preset_id,
                    type_id=type_id,
                    type_title=type_dir.name,
                    mount_mode=mode,
                    mount_mode_title=mount_mode_title(mode),
                    color_title=color_dir.name,
                    preview_image=preview,
                    reference_images=tuple(ordered_refs),
                )

        if mounts_for_type:
            mount_modes_ordered = tuple(m for m in ("opening", "sash") if m in mounts_for_type)
            type_options[type_id] = CurtainTypeOption(
                type_id=type_id,
                title=type_dir.name,
                mount_modes=mount_modes_ordered,
            )

    if not presets:
        raise RuntimeError(f"No presets found in reference store: {root}")
    return presets, type_options

