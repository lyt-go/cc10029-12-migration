"""版本化窗口结构清单：登记抽取窗口字段结构的版本定义、新旧字段名映射与启用状态。

增量窗口与格式适配共同使用本清单：
- 增量窗口按当前启用版本登记窗口，并按窗口登记版本产出对应字段结构的产物；
- 格式适配根据产物实际包含的字段，对照清单解析旧名和新名。

清单在部署时通过 activate_manifest 启用并落盘；未启用时默认历史 v1，
兼容历史 v1 输入与原来的恢复操作。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

KEY_FIELD = "record_key"  # 原记录键字段，跨版本稳定保留
DEFAULT_VERSION = "v1"  # 未启用清单时的历史默认版本，兼容历史 v1 输入

# 版本化窗口结构清单：每个版本登记完整字段序列、增量载荷字段与旧名→新名映射。
WINDOW_STRUCTURES: dict[str, dict[str, Any]] = {
    "v1": {
        "version": "v1",
        "key_field": KEY_FIELD,
        "fields": [KEY_FIELD, "batch_key", "line_no", "ext_a", "ext_b", "ext_c", "created_at"],
        "payload_fields": ["ext_a", "ext_b", "ext_c"],
        "renames": {},
    },
    "v2": {
        "version": "v2",
        "key_field": KEY_FIELD,
        "fields": [KEY_FIELD, "batch_key", "line_no", "ext_alpha", "ext_beta", "ext_gamma", "created_at"],
        "payload_fields": ["ext_alpha", "ext_beta", "ext_gamma"],
        "renames": {"ext_a": "ext_alpha", "ext_b": "ext_beta", "ext_c": "ext_gamma"},
    },
}


def _manifest_dir() -> Path:
    """清单落盘目录；延迟导入 DATA_ROOT 避免与 services 包初始化循环依赖。"""
    from app.services.base import DATA_ROOT
    return DATA_ROOT / "manifests"


def _manifest_file() -> Path:
    return _manifest_dir() / "window_structure_manifest.json"


def _active_file() -> Path:
    return _manifest_dir() / "active_version.json"


def known_versions() -> list[str]:
    """返回清单中已登记的全部结构版本。"""
    return sorted(WINDOW_STRUCTURES)


def structure_for(version: str) -> dict[str, Any]:
    """取出指定版本的结构定义；未知版本抛错，阻止未登记结构进入流水线。"""
    if version not in WINDOW_STRUCTURES:
        raise ValueError(f"未知窗口结构版本: {version}，清单已登记版本: {known_versions()}")
    return WINDOW_STRUCTURES[version]


def fields_for(version: str) -> list[str]:
    """指定版本一次窗口交付应采用的完整字段序列。"""
    return list(structure_for(version)["fields"])


def payload_fields_for(version: str) -> list[str]:
    """指定版本的增量载荷字段（版本间发生重命名的字段）。"""
    return list(structure_for(version)["payload_fields"])


def renames_for(version: str) -> dict[str, str]:
    """指定版本登记的旧名→新名映射。"""
    return dict(structure_for(version)["renames"])


def all_renames() -> dict[str, str]:
    """汇总清单全部版本的旧名→新名映射，供跨版本解析使用。"""
    merged: dict[str, str] = {}
    for version in known_versions():
        merged.update(WINDOW_STRUCTURES[version]["renames"])
    return merged


def active_version() -> str:
    """读取当前启用的窗口结构版本；未启用或状态损坏时回退历史默认 v1。"""
    try:
        payload = json.loads(_active_file().read_text(encoding="utf-8"))
        version = str(payload.get("active_version", ""))
        if version in WINDOW_STRUCTURES:
            return version
    except (OSError, json.JSONDecodeError):
        pass
    return DEFAULT_VERSION


def activate_manifest(version: str) -> dict[str, Any]:
    """部署时启用版本化窗口结构清单：落盘全量清单文件并切换启用版本。"""
    structure_for(version)  # 校验版本已登记
    manifest_dir = _manifest_dir()
    manifest_dir.mkdir(parents=True, exist_ok=True)
    enabled_at = time.time()
    manifest_payload = {
        "enabled_at": enabled_at,
        "active_version": version,
        "key_field": KEY_FIELD,
        "versions": WINDOW_STRUCTURES,
    }
    _manifest_file().write_text(
        json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _active_file().write_text(
        json.dumps({"active_version": version, "enabled_at": enabled_at},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "active_version": version,
        "versions": known_versions(),
        "manifest_path": str(_manifest_file()),
    }


def detect_version(field_names: Iterable[str]) -> str | None:
    """根据产物实际包含的字段识别结构版本。

    仅命中一个版本的载荷字段时返回该版本；同时命中多个版本返回 "mixed"
    （一次交付不应混版）；都未命中返回 None（非窗口产物字段）。
    """
    names = set(field_names)
    matched = [
        version for version in known_versions()
        if names & set(WINDOW_STRUCTURES[version]["payload_fields"])
    ]
    if len(matched) == 1:
        return matched[0]
    if len(matched) > 1:
        return "mixed"
    return None


def resolve_renames(field_names: Iterable[str]) -> dict[str, str]:
    """根据产物实际包含的字段解析旧名→新名映射，仅含产物中实际出现的旧名。"""
    names = set(field_names)
    return {old: new for old, new in all_renames().items() if old in names}


def migrate_record(record: dict[str, Any], target_version: str) -> dict[str, Any]:
    """按目标版本的新旧名映射迁移单条记录，保留原记录键及其余未登记字段。"""
    renames = renames_for(target_version)
    return {renames.get(key, key): value for key, value in record.items()}
