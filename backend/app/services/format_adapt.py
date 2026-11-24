"""格式适配领域服务：维护源端记录格式新旧版本的结构差异识别与兼容适配切换。

本模块识别源端记录格式的结构差异字段（新增/删除/重命名），在源端格式版本升级时
创建适配版本记录并写入适配产物文件，确保下游目标端能兼容新旧格式。

格式适配与增量窗口共同使用版本化窗口结构清单（app.window_manifest）：
执行适配时根据产物实际包含的字段，对照清单解析旧名和新名并完成重命名，
保留原记录键。
"""
from __future__ import annotations

import time
from typing import Any

from app import window_manifest
from app.services.base import BaseModuleService
from app.store import store


class FormatAdaptService(BaseModuleService):
    MODULE = "format_adapt"
    ENTITY = "适配版本"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["适配编号", "记录格式", "旧版结构", "新版结构", "差异字段", "兼容方式", "切换批次", "适配版本"]
    REQUIRED_FIELDS = ["适配编号", "记录格式", "旧版结构"]
    STATUS_ORDER = ["草稿", "待适配", "已适配", "已退役"]
    ACTION_RULES = {"提交适配": "待适配", "确认适配": "已适配", "标记退役": "已退役"}
    NEGATIVE_ACTIONS = ["标记退役"]

    def detect_format_diff(self, old_structure: str, new_structure: str) -> dict[str, Any]:
        """识别源端记录格式新旧版本间的差异字段（新增/删除/重命名）。"""
        old_fields = set(old_structure.split(","))
        new_fields = set(new_structure.split(","))
        added = list(new_fields - old_fields)
        removed = list(old_fields - new_fields)
        renamed = [] if len(added) == len(removed) else list(old_fields & new_fields)
        return {"added": added, "removed": removed, "renamed": renamed,
                "has_diff": len(added) + len(removed) > 0}

    def create_adaptation(self, format_name: str, old_structure: str, new_structure: str) -> dict[str, Any]:
        """为格式版本变更创建适配版本记录，识别差异字段并写入适配事件日志。"""
        diff = self.detect_format_diff(old_structure, new_structure)
        adapt_code = f"FORM-{format_name}-{int(time.time())}"
        compat_mode = "前向兼容" if len(diff["added"]) <= len(diff["removed"]) else "版本切换"
        values = {
            "适配编号": adapt_code,
            "记录格式": format_name,
            "旧版结构": old_structure,
            "新版结构": new_structure,
            "差异字段": ",".join(diff["added"] + diff["removed"]),
            "兼容方式": compat_mode,
            "切换批次": "",
            "适配版本": "v2.0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建格式适配失败: {missing}")
        self._write_jsonl(
            self._data_dir("events") / "format_adapt_log.jsonl",
            [{"action": "create_adaptation", "adapt_code": adapt_code, "format": format_name,
              "old": old_structure, "new": new_structure, "compat_mode": compat_mode,
              "at": time.time()}]
        )
        return entry

    def execute_adaptation(self, adapt_id: int, batch_code: str) -> dict[str, Any]:
        """执行格式适配：按产物实际包含的字段对照版本化窗口结构清单解析旧名和新名。

        本方法是数据交换平台格式兼容的关键环节：
        读取批次传输产物文件，与增量窗口共同使用版本化窗口结构清单，
        根据产物实际包含的字段把旧名记录转换为新名结构（保留原记录键），
        输出适配后的产物文件供下游目标端消费。
        """
        adapt_entry = self.get_entry(adapt_id)
        if adapt_entry is None:
            return {"error": f"适配版本 {adapt_id} 不存在"}

        entry, msg = self.run_action(adapt_id, "提交适配")
        if entry is None:
            return {"error": msg, "adapt_id": adapt_id}

        old_fields = adapt_entry["旧版结构"].split(",")
        new_fields = adapt_entry["新版结构"].split(",")

        # 读取批次产物文件
        batch_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = self._read_jsonl(batch_file)
        if not records:
            records = self._simulate_records(batch_code, count=5, field_prefix="fmt")

        # 根据产物实际包含的字段，对照版本化窗口结构清单解析旧名→新名
        actual_fields: set[str] = set()
        for rec in records:
            actual_fields.update(rec.keys())
        resolved_renames = window_manifest.resolve_renames(actual_fields)
        artifact_version = window_manifest.detect_version(actual_fields)

        # 执行新旧格式转换
        adapted = []
        for rec in records:
            new_rec = {}
            # 清单解析出的旧名字段改为新名，其余字段（含原记录键）原样保留
            for key, value in rec.items():
                new_rec[resolved_renames.get(key, key)] = value
            # 适配条目声明的旧版结构→新版结构（兼容历史适配行为）
            for i, new_f in enumerate(new_fields):
                if new_f in new_rec:
                    continue
                if i < len(old_fields) and old_fields[i] in rec:
                    new_rec[new_f] = rec[old_fields[i]]
                elif new_f not in rec:
                    new_rec[new_f] = f"default_{new_f}"
            new_rec["_adapt_version"] = adapt_entry["适配版本"]
            adapted.append(new_rec)

        # 写入适配产物文件
        output_file = self._data_dir("adapted") / f"{adapt_entry['适配编号']}_{batch_code}.jsonl"
        written = self._write_jsonl(output_file, adapted)

        entry["切换批次"] = batch_code
        final_entry, _ = self.run_action(adapt_id, "确认适配")

        return {
            "adapt_code": adapt_entry["适配编号"],
            "batch_code": batch_code,
            "records_adapted": written,
            "artifact_path": str(output_file),
            "artifact_version": artifact_version,
            "resolved_renames": resolved_renames,
        }