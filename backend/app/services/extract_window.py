"""增量窗口领域服务：维护按增量字段切分的抽取窗口边界、抽取执行与推进水位。

本模块负责根据源端登记信息创建抽取窗口，执行数据抽取并写入抽取产物文件，
完成后自动驱动传输批次创建，形成源端登记→窗口抽取→批次传输的贯通链路。

窗口字段结构按版本化窗口结构清单（app.window_manifest）登记与产出：
- 登记窗口时记录结构版本（默认取当前启用版本，未启用时为历史 v1）；
- 一次窗口交付中的字段采用同一版本，并保留原记录键 record_key；
- 恢复操作按窗口登记时的版本继续抽取，兼容历史 v1 输入；
- 已登记的窗口字段记录可迁移到 v2，迁移出错时沿原路径恢复。
"""
from __future__ import annotations

import json
import shutil
import time
from typing import Any

from app import window_manifest
from app.services.base import BaseModuleService
from app.store import store


class ExtractWindowService(BaseModuleService):
    MODULE = "extract_window"
    ENTITY = "抽取窗口"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["窗口编号", "源端编号", "增量字段", "窗口起点", "窗口终点", "抽取条数", "窗口类型", "推进水位"]
    REQUIRED_FIELDS = ["窗口编号", "源端编号", "增量字段"]
    STATUS_ORDER = ["草稿", "待抽取", "抽取中", "已闭合"]
    ACTION_RULES = {"启动抽取": "抽取中", "闭合窗口": "已闭合", "标记作废": "已闭合"}
    NEGATIVE_ACTIONS = ["标记作废"]

    # -- 版本化窗口结构清单 --

    def deploy_window_manifest(self, version: str = "v2") -> dict[str, Any]:
        """部署时启用版本化窗口结构清单：落盘清单文件、切换启用版本并写入部署事件日志。"""
        result = window_manifest.activate_manifest(version)
        self._write_jsonl(
            self._data_dir("events") / "window_manifest_log.jsonl",
            [{"action": "deploy_manifest", "active_version": result["active_version"],
              "versions": result["versions"], "at": time.time()}]
        )
        return result

    def create_window(self, source_code: str, count: int = 10,
                      schema_version: str | None = None) -> dict[str, Any]:
        """为指定源端创建增量抽取窗口并写入窗口配置快照，记录窗口边界、结构版本与初始水位。

        结构版本默认取当前启用的清单版本：切换前登记的窗口为 v1，
        新版上线后登记的后续窗口为新版本。
        """
        version = schema_version or window_manifest.active_version()
        window_manifest.structure_for(version)  # 校验版本已登记入清单
        sources = [r for r in store.rows("source_register") if r.get("源端编号") == source_code]
        if not sources:
            raise ValueError(f"源端编号 {source_code} 未登记，请先完成源端登记")
        entry, missing = self.create_entry({
            "窗口编号": f"EXTR-{source_code}-{time.time_ns()}",
            "源端编号": source_code,
            "增量字段": "updated_at",
            "窗口起点": time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time())),
            "窗口终点": time.strftime("%Y-%m-%dT23:59:59", time.localtime(time.time())),
            "抽取条数": str(count),
            "窗口类型": "增量抽取",
            "推进水位": "0",
        })
        if entry is None:
            raise ValueError(f"创建窗口失败: {missing}")
        # 登记窗口结构版本，后续抽取、恢复、迁移均以登记版本为准
        entry["结构版本"] = version
        # 写入窗口配置快照
        self._write_jsonl(
            self._data_dir("windows") / f"{entry['窗口编号']}_config.jsonl",
            [{"window_id": entry["id"], "window_code": entry["窗口编号"], "source": source_code,
              "start": entry["窗口起点"], "end": entry["窗口终点"], "count": count,
              "schema_version": version, "created_at": time.time()}]
        )
        return entry

    def _versioned_records(self, window_code: str, source_code: str, version: str,
                           count: int, start_line: int = 1) -> list[dict[str, Any]]:
        """按版本化窗口结构清单生成一次交付的记录。

        一次窗口交付中的字段采用同一版本（按清单字段序列输出），
        每条记录携带稳定的原记录键 record_key，迁移与恢复时保留。
        """
        fields = window_manifest.fields_for(version)
        payload_fields = window_manifest.payload_fields_for(version)
        records = []
        for i in range(count):
            line_no = start_line + i
            record: dict[str, Any] = {
                window_manifest.KEY_FIELD: f"{window_code}-{line_no:04d}",
                "batch_key": source_code,
                "line_no": line_no,
                "created_at": time.time(),
            }
            for pos, field in enumerate(payload_fields):
                if pos == len(payload_fields) - 1:
                    record[field] = line_no * 10
                else:
                    record[field] = f"val_{line_no}_{field}"
            records.append({field: record[field] for field in fields if field in record})
        return records

    def execute_extract(self, window_id: int, record_count: int = 10) -> dict[str, Any]:
        """执行增量窗口数据抽取，生成抽取产物文件并推进窗口水位。

        从已在 store 登记的源端信息中查找匹配记录，按窗口登记的结构版本
        生成同版本字段的数据行（保留原记录键），逐条写入抽取产物文件（jsonl），
        最后推进水位并自动触发传输批次创建。历史窗口无版本字段时按 v1 兼容处理。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        entry, msg = self.run_action(window_id, "启动抽取")
        if entry is None:
            return {"error": msg, "window_id": window_id}

        source_code = window["源端编号"]
        version = window.get("结构版本") or window_manifest.DEFAULT_VERSION
        # 生成抽取数据行：一次窗口交付中的字段采用同一版本，并保留原记录键
        records = self._versioned_records(window["窗口编号"], source_code, version, record_count)

        # 写入抽取产物文件——这是本模块的核心数据产物
        output_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        written = self._write_jsonl(output_file, records)

        # 推进窗口水位
        self._write_checkpoint(f"window_{window['窗口编号']}", {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "schema_version": version,
            "records_extracted": written,
            "last_extract_at": time.time(),
            "watermark": str(written),
        })

        result = {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "schema_version": version,
            "records_extracted": written,
            "artifact_path": str(output_file),
            "watermark": str(written),
        }

        # 抽取完成后自动触发传输批次创建
        try:
            tb_svc = self._safe_import("app.services.transfer_batch", "TransferBatchService")()
            batch_result = tb_svc.create_batch(window["窗口编号"], written)
            result["auto_batch"] = batch_result
        except Exception as e:
            result["auto_batch"] = {"error": str(e)}

        return result

    def recover_window(self, window_id: int) -> dict[str, Any]:
        """恢复未完成窗口的抽取：按窗口登记时的结构版本，从水位断点继续抽取。

        恢复操作兼容历史 v1 输入：即使部署已切换到新版本，按旧版本登记的窗口
        仍以登记版本字段结构沿原路径续写产物，旧数据与未完成窗口都能继续读取，
        原记录键按行号序列连续推进、不与已抽取记录冲突。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        version = window.get("结构版本") or window_manifest.DEFAULT_VERSION
        window_code = window["窗口编号"]
        artifact = self._data_dir("extracted") / f"{window_code}.jsonl"

        # 旧数据继续读取：已有产物与水位断点
        existing = self._read_jsonl(artifact)
        checkpoint = self._read_checkpoint(f"window_{window_code}") or {}
        watermark = int(checkpoint.get("watermark") or len(existing) or 0)
        target = int(str(window.get("抽取条数") or "0") or 0)
        remaining = max(target - watermark, 0)
        if remaining <= 0:
            return {
                "window_code": window_code,
                "schema_version": version,
                "recovered_records": 0,
                "watermark": str(watermark),
                "message": "窗口无待恢复记录",
            }

        # 按登记版本续抽剩余记录，原记录键从水位之后连续编号
        records = self._versioned_records(window_code, window["源端编号"], version,
                                          remaining, start_line=watermark + 1)
        written = self._write_jsonl(artifact, records)
        new_watermark = watermark + written

        self._write_checkpoint(f"window_{window_code}", {
            "window_code": window_code,
            "source_code": window["源端编号"],
            "schema_version": version,
            "records_extracted": new_watermark,
            "last_extract_at": time.time(),
            "watermark": str(new_watermark),
            "recovered": True,
        })
        self._write_jsonl(
            self._data_dir("events") / "window_recover_log.jsonl",
            [{"action": "recover", "window_code": window_code, "schema_version": version,
              "resumed_from": watermark, "recovered": written, "at": time.time()}]
        )
        return {
            "window_code": window_code,
            "schema_version": version,
            "recovered_records": written,
            "resumed_from": watermark,
            "watermark": str(new_watermark),
            "artifact_path": str(artifact),
        }

    def read_window_records(self, window_id: int) -> dict[str, Any]:
        """读取窗口已登记的字段记录，旧数据与未完成窗口都能继续读取。

        返回产物实际字段识别出的结构版本与窗口登记版本，供迁移与流水线核对使用。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        window_code = window["窗口编号"]
        artifact = self._data_dir("extracted") / f"{window_code}.jsonl"
        records = self._read_jsonl(artifact)
        actual_fields: set[str] = set()
        for rec in records:
            actual_fields.update(rec.keys())
        return {
            "window_code": window_code,
            "registered_version": window.get("结构版本") or window_manifest.DEFAULT_VERSION,
            "artifact_version": window_manifest.detect_version(actual_fields) if records else None,
            "record_count": len(records),
            "records": records,
            "artifact_path": str(artifact),
        }

    def migrate_window_records(self, window_id: int, target_version: str = "v2") -> dict[str, Any]:
        """把已登记的抽取窗口字段记录迁移到目标版本（默认 v2）。

        迁移按清单的新旧名映射重写产物文件，一次交付的字段统一为目标版本，
        并保留原记录键 record_key。迁移前先备份原产物；任一出错即沿原路径
        恢复（把备份内容写回原产物路径），旧数据仍可继续读取。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        window_manifest.structure_for(target_version)  # 校验目标版本已登记
        window_code = window["窗口编号"]
        source_version = window.get("结构版本") or window_manifest.DEFAULT_VERSION
        if source_version == target_version:
            return {"window_code": window_code, "schema_version": source_version,
                    "migrated": 0, "message": "窗口已是目标版本"}
        artifact = self._data_dir("extracted") / f"{window_code}.jsonl"
        records = self._read_jsonl(artifact)
        if not records:
            return {"window_code": window_code, "schema_version": source_version,
                    "migrated": 0, "message": "窗口无已登记字段记录，保持原版本"}

        # 迁移前备份原产物，供出错时沿原路径恢复
        backup_file = self._data_dir("migration_backup") / f"{window_code}_{source_version}.jsonl"
        shutil.copyfile(artifact, backup_file)
        original_text = artifact.read_text(encoding="utf-8")

        migrated_keys: list[Any] = []
        try:
            # 截断原文件并逐条写回迁移后的记录；任一条校验失败即抛错回滚
            with open(artifact, "w", encoding="utf-8") as f:
                for rec in records:
                    key = rec.get(window_manifest.KEY_FIELD)
                    if not key:
                        raise ValueError(
                            f"记录缺少原记录键 {window_manifest.KEY_FIELD}，无法保留记录键迁移")
                    migrated = window_manifest.migrate_record(rec, target_version)
                    f.write(json.dumps(migrated, ensure_ascii=False, default=str) + "\n")
                    migrated_keys.append(key)
        except Exception as exc:
            # 沿原路径恢复：把迁移前内容写回原产物路径，旧数据继续可读
            artifact.write_text(original_text, encoding="utf-8")
            self._write_jsonl(
                self._data_dir("events") / "window_migration_log.jsonl",
                [{"action": "migrate_rollback", "window_code": window_code,
                  "from": source_version, "to": target_version, "error": str(exc),
                  "at": time.time()}]
            )
            return {"error": f"迁移失败已沿原路径恢复: {exc}", "window_code": window_code,
                    "schema_version": source_version, "backup_path": str(backup_file)}

        # 迁移成功：推进窗口登记版本与水位断点中的结构版本
        window["结构版本"] = target_version
        checkpoint = self._read_checkpoint(f"window_{window_code}") or {"window_code": window_code}
        checkpoint["schema_version"] = target_version
        self._write_checkpoint(f"window_{window_code}", checkpoint)
        self._write_jsonl(
            self._data_dir("events") / "window_migration_log.jsonl",
            [{"action": "migrate", "window_code": window_code, "from": source_version,
              "to": target_version, "records": len(migrated_keys), "at": time.time()}]
        )
        return {
            "window_code": window_code,
            "schema_version": target_version,
            "migrated": len(migrated_keys),
            "record_keys": migrated_keys,
            "backup_path": str(backup_file),
            "artifact_path": str(artifact),
        }

    def close_window(self, window_id: int) -> dict[str, Any]:
        """闭合抽取窗口，生成窗口产物摘要（记录条数、字节总量、校验值），写入闭合事件日志。"""
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}

        entry, msg = self.run_action(window_id, "闭合窗口")
        # 从产物文件读取统计信息
        artifact_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))

        # 写入闭合事件日志，记录抽取产物的完整统计
        self._write_jsonl(
            self._data_dir("events") / "window_close_log.jsonl",
            [{"action": "close", "window_code": window["窗口编号"], "record_count": len(records),
              "checksum": checksum, "closed_at": time.time()}]
        )

        return {
            "window_code": window["窗口编号"],
            "status": entry["status"] if entry else "已闭合",
            "record_count": len(records),
            "checksum": checksum,
            "message": msg,
        }
