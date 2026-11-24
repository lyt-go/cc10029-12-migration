"""集成测试：抽取窗口字段记录 v2 迁移与版本化窗口结构清单。

场景：同一源端有两个窗口——第一个按 v1 登记但尚未输出，第二个在新版上线后登记，
旧窗这时收到恢复请求。验证：
- 部署时启用版本化窗口结构清单，兼容历史 v1 输入与原来的恢复操作；
- 旧数据和未完成窗口都能继续读取，迁移出错时可以沿原路径恢复；
- 一次窗口交付中的字段采用同一版本并保留原记录键；
- 增量窗口与格式适配共同使用版本化窗口结构清单，
  格式适配根据产物实际包含的字段解析旧名和新名；
- 同一条交换流水线核对两份产物的字段、键与版本关系，并检查一次失败后的重新读入。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app import window_manifest
from app.services.extract_window import ExtractWindowService
from app.services.format_adapt import FormatAdaptService
from app.services.source_register import SourceRegisterService
from app.services.transfer_batch import TransferBatchService
from app.store import store


@pytest.fixture(autouse=True)
def reset_state():
    """每个测试前后重置 store 与清单启用版本（回到历史默认 v1），确保测试隔离。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    window_manifest.activate_manifest(window_manifest.DEFAULT_VERSION)
    yield
    window_manifest.activate_manifest(window_manifest.DEFAULT_VERSION)
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }


def _register_source(source_code: str) -> None:
    """登记并确认一个源端，供创建抽取窗口使用。"""
    svc = SourceRegisterService()
    entry = svc.register_source({
        "源端编号": source_code, "系统名称": "版本迁移测试源端", "数据域": "订单域",
        "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "10万",
        "抽取方式": "增量抽取", "登记游标": "0",
    })
    svc.confirm_register(entry["id"])


class TestWindowManifest:
    """版本化窗口结构清单：部署启用、版本识别与新旧名解析。"""

    def test_default_version_is_v1_and_activate_persists(self):
        """未启用时默认历史 v1；部署启用后切换到新版本并落盘清单。"""
        assert window_manifest.active_version() == "v1"
        result = window_manifest.activate_manifest("v2")
        assert result["active_version"] == "v2"
        assert window_manifest.active_version() == "v2"
        payload = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
        assert payload["active_version"] == "v2"
        assert set(payload["versions"]) == {"v1", "v2"}
        with pytest.raises(ValueError):
            window_manifest.activate_manifest("v9")

    def test_detect_and_resolve_by_actual_fields(self):
        """根据产物实际包含的字段识别版本并解析旧名→新名。"""
        assert window_manifest.detect_version({"record_key", "ext_a", "ext_b"}) == "v1"
        assert window_manifest.detect_version({"record_key", "ext_alpha"}) == "v2"
        assert window_manifest.detect_version({"ext_a", "ext_alpha"}) == "mixed"
        assert window_manifest.detect_version({"other"}) is None
        assert window_manifest.resolve_renames({"ext_a", "other"}) == {"ext_a": "ext_alpha"}
        assert window_manifest.resolve_renames({"ext_alpha"}) == {}
        migrated = window_manifest.migrate_record(
            {"record_key": "k1", "ext_a": 1, "keep": 2}, "v2")
        assert migrated == {"record_key": "k1", "ext_alpha": 1, "keep": 2}


class TestVersionedWindowRegistration:
    """切换前登记旧窗，切换后创建后续窗口。"""

    def test_register_windows_around_manifest_switch(self):
        """同一源端两个窗口：切换前登记的为 v1，部署启用 v2 后登记的为 v2。"""
        source = "SOUR-V2SWITCH"
        _register_source(source)
        ew = ExtractWindowService()

        # 切换前：第一个窗口按 v1 登记（尚未输出）
        w1 = ew.create_window(source, count=12)
        assert w1["结构版本"] == "v1"

        # 部署时启用版本化窗口结构清单
        deploy = ew.deploy_window_manifest("v2")
        assert deploy["active_version"] == "v2"
        assert Path(deploy["manifest_path"]).exists()
        assert window_manifest.active_version() == "v2"

        # 切换后：第二个窗口按当前启用版本登记
        w2 = ew.create_window(source, count=12)
        assert w2["结构版本"] == "v2"
        assert w1["窗口编号"] != w2["窗口编号"]

    def test_delivery_uses_single_version_and_keeps_record_keys(self):
        """一次窗口交付中的字段采用同一版本并保留原记录键。"""
        source = "SOUR-V2DELIVER"
        _register_source(source)
        ew = ExtractWindowService()
        w1 = ew.create_window(source, count=10)
        result = ew.execute_extract(w1["id"], record_count=10)
        assert result["records_extracted"] == 10
        assert result["schema_version"] == "v1"

        records = ew._read_jsonl(Path(result["artifact_path"]))
        v1_fields = set(window_manifest.fields_for("v1"))
        keys = []
        for rec in records:
            assert set(rec.keys()) <= v1_fields  # 交付内字段同属一个版本
            keys.append(rec["record_key"])
        assert len(set(keys)) == len(keys)
        assert keys == [f"{w1['窗口编号']}-{i:04d}" for i in range(1, 11)]


class TestOldWindowRecoveryAfterSwitch:
    """新版上线后，旧窗收到恢复请求：兼容历史 v1 输入与原来的恢复操作。"""

    def test_recover_v1_window_after_v2_deployed(self):
        """同一源端两个窗口：旧窗 v1 未抽完，新版上线后旧窗按 v1 恢复续抽。"""
        source = "SOUR-V2RECOVER"
        _register_source(source)
        ew = ExtractWindowService()

        # 切换前：旧窗按 v1 登记，先抽一部分（尚未完成）
        w1 = ew.create_window(source, count=15)
        first = ew.execute_extract(w1["id"], record_count=10)
        assert first["records_extracted"] == 10

        # 部署启用 v2 清单，切换后登记第二个窗口并完成抽取
        ew.deploy_window_manifest("v2")
        w2 = ew.create_window(source, count=8)
        assert w2["结构版本"] == "v2"
        second = ew.execute_extract(w2["id"], record_count=8)
        assert second["schema_version"] == "v2"

        # 旧窗这时收到恢复请求：按登记时的 v1 继续抽取剩余 5 条
        recover = ew.recover_window(w1["id"])
        assert recover["schema_version"] == "v1"
        assert recover["resumed_from"] == 10
        assert recover["recovered_records"] == 5
        assert recover["watermark"] == "15"

        # 旧数据与未完成窗口都能继续读取：旧窗产物全部为 v1 字段，记录键连续
        read_back = ew.read_window_records(w1["id"])
        assert read_back["record_count"] == 15
        assert read_back["registered_version"] == "v1"
        assert read_back["artifact_version"] == "v1"
        keys = [rec["record_key"] for rec in read_back["records"]]
        assert keys == [f"{w1['窗口编号']}-{i:04d}" for i in range(1, 16)]
        for rec in read_back["records"]:
            assert "ext_a" in rec and "ext_alpha" not in rec

        # 新窗产物为 v2 字段
        read_new = ew.read_window_records(w2["id"])
        assert read_new["artifact_version"] == "v2"
        for rec in read_new["records"]:
            assert "ext_alpha" in rec and "ext_a" not in rec


class TestMigrateWindowRecordsToV2:
    """把已经登记的抽取窗口字段记录迁移到 v2。"""

    def test_migrate_v1_records_to_v2_keeps_record_keys(self):
        """迁移后字段统一为 v2 结构，原记录键逐条保留。"""
        source = "SOUR-V2MIGRATE"
        _register_source(source)
        ew = ExtractWindowService()
        w1 = ew.create_window(source, count=10)
        ew.execute_extract(w1["id"], record_count=10)
        before = ew.read_window_records(w1["id"])
        before_keys = [rec["record_key"] for rec in before["records"]]

        ew.deploy_window_manifest("v2")
        migration = ew.migrate_window_records(w1["id"], target_version="v2")
        assert "error" not in migration
        assert migration["migrated"] == 10
        assert migration["record_keys"] == before_keys  # 保留原记录键
        assert Path(migration["backup_path"]).exists()

        after = ew.read_window_records(w1["id"])
        assert after["artifact_version"] == "v2"
        assert after["registered_version"] == "v2"
        assert [rec["record_key"] for rec in after["records"]] == before_keys
        v2_fields = set(window_manifest.fields_for("v2"))
        for rec in after["records"]:
            assert set(rec.keys()) <= v2_fields
            # 字段名已迁移为新名，取值仍为原记录内容
            assert rec["ext_alpha"] == f"val_{rec['line_no']}_ext_a"

        # 已迁移窗口再次迁移为幂等
        again = ew.migrate_window_records(w1["id"], target_version="v2")
        assert again["migrated"] == 0

    def test_migration_error_rolls_back_along_original_path(self):
        """迁移出错时沿原路径恢复，旧数据可重新读入，排除故障后可重新迁移。"""
        source = "SOUR-V2ROLLBACK"
        _register_source(source)
        ew = ExtractWindowService()
        w1 = ew.create_window(source, count=8)
        result = ew.execute_extract(w1["id"], record_count=8)
        artifact = Path(result["artifact_path"])

        # 模拟一条损坏记录（丢失原记录键）进入产物
        with open(artifact, "a", encoding="utf-8") as f:
            f.write(json.dumps({"batch_key": source, "line_no": 99, "ext_a": "broken"},
                               ensure_ascii=False) + "\n")
        pre_migration_lines = artifact.read_text(encoding="utf-8").splitlines()
        assert len(pre_migration_lines) == 9

        ew.deploy_window_manifest("v2")
        migration = ew.migrate_window_records(w1["id"], target_version="v2")
        assert "error" in migration
        assert "沿原路径恢复" in migration["error"]

        # 沿原路径恢复：产物内容与迁移前完全一致
        assert artifact.read_text(encoding="utf-8").splitlines() == pre_migration_lines

        # 一次失败后的重新读入：旧数据仍可读取，登记版本未被部分推进
        read_back = ew.read_window_records(w1["id"])
        assert read_back["record_count"] == 9
        assert read_back["artifact_version"] == "v1"
        assert read_back["registered_version"] == "v1"
        good_keys = [rec["record_key"] for rec in read_back["records"] if "record_key" in rec]
        assert good_keys == [f"{w1['窗口编号']}-{i:04d}" for i in range(1, 9)]

        # 排除损坏记录后重新迁移成功，记录键保留
        artifact.write_text("\n".join(pre_migration_lines[:8]) + "\n", encoding="utf-8")
        retry = ew.migrate_window_records(w1["id"], target_version="v2")
        assert "error" not in retry
        assert retry["migrated"] == 8
        assert retry["record_keys"] == good_keys


class TestFormatAdaptResolvesActualFields:
    """格式适配与增量窗口共同使用版本化窗口结构清单。"""

    def test_adaptation_resolves_old_and_new_names_from_artifact(self):
        """格式适配根据产物实际包含的字段解析旧名和新名。"""
        source = "SOUR-V2ADAPT"
        _register_source(source)
        ew = ExtractWindowService()
        tb = TransferBatchService()
        fa = FormatAdaptService()

        # v1 产物（切换前登记并抽取）
        w1 = ew.create_window(source, count=6)
        e1 = ew.execute_extract(w1["id"], record_count=6)
        # v2 产物（部署启用 v2 后登记并抽取）
        ew.deploy_window_manifest("v2")
        w2 = ew.create_window(source, count=6)
        e2 = ew.execute_extract(w2["id"], record_count=6)

        # 同一交换流水线：两个批次分别传输，产物进入 batches/
        b1 = tb.start_transfer(e1["auto_batch"]["batch_id"], chunk_size=3)
        b2 = tb.start_transfer(e2["auto_batch"]["batch_id"], chunk_size=3)

        # 对 v1 批次产物：根据产物实际包含的字段解析出旧名→新名
        adapt1 = fa.create_adaptation("order_format", "ext_a,ext_b,ext_c",
                                      "ext_alpha,ext_beta,ext_gamma")
        out1 = fa.execute_adaptation(adapt1["id"], b1["batch_code"])
        assert out1["artifact_version"] == "v1"
        assert out1["resolved_renames"] == {
            "ext_a": "ext_alpha", "ext_b": "ext_beta", "ext_c": "ext_gamma"}
        adapted1 = fa._read_jsonl(Path(out1["artifact_path"]))
        assert len(adapted1) == 6
        for rec in adapted1:
            assert "ext_alpha" in rec and "ext_a" not in rec
            assert rec["record_key"].startswith(w1["窗口编号"])  # 原记录键保留

        # 对 v2 批次产物：产物已为新名，无需重命名，记录原样保留
        adapt2 = fa.create_adaptation("order_format", "ext_a,ext_b,ext_c",
                                      "ext_alpha,ext_beta,ext_gamma")
        out2 = fa.execute_adaptation(adapt2["id"], b2["batch_code"])
        assert out2["artifact_version"] == "v2"
        assert out2["resolved_renames"] == {}
        adapted2 = fa._read_jsonl(Path(out2["artifact_path"]))
        assert len(adapted2) == 6
        for rec in adapted2:
            assert "ext_alpha" in rec
            assert rec["record_key"].startswith(w2["窗口编号"])


class TestSamePipelineTwoArtifacts:
    """同一条交换流水线核对两份产物的字段、键与版本关系。"""

    def test_pipeline_verifies_fields_keys_and_versions_of_both_artifacts(self):
        source = "SOUR-V2PIPE"
        _register_source(source)
        ew = ExtractWindowService()
        tb = TransferBatchService()

        # 切换前登记旧窗（v1）并抽取；部署启用 v2 后创建后续窗口并抽取
        w1 = ew.create_window(source, count=10)
        e1 = ew.execute_extract(w1["id"], record_count=10)
        ew.deploy_window_manifest("v2")
        w2 = ew.create_window(source, count=10)
        e2 = ew.execute_extract(w2["id"], record_count=10)

        # 同一条交换流水线：两个批次分别传输
        b1 = tb.start_transfer(e1["auto_batch"]["batch_id"], chunk_size=4)
        b2 = tb.start_transfer(e2["auto_batch"]["batch_id"], chunk_size=4)
        recs1 = tb._read_jsonl(Path(b1["artifact_path"]))
        recs2 = tb._read_jsonl(Path(b2["artifact_path"]))
        assert len(recs1) == 10 and len(recs2) == 10

        # 核对两份产物的字段版本关系：各自保持登记版本的字段结构
        v1_fields = set(window_manifest.fields_for("v1"))
        v2_fields = set(window_manifest.fields_for("v2"))
        transfer_meta = {"batch_code", "chunk_index", "transferred_at"}
        for rec in recs1:
            assert set(rec.keys()) <= v1_fields | transfer_meta
        for rec in recs2:
            assert set(rec.keys()) <= v2_fields | transfer_meta

        # 核对记录键：两窗口各自的原记录键在传输后保留且互不重叠
        keys1 = {rec["record_key"] for rec in recs1}
        keys2 = {rec["record_key"] for rec in recs2}
        assert keys1 == {f"{w1['窗口编号']}-{i:04d}" for i in range(1, 11)}
        assert keys2 == {f"{w2['窗口编号']}-{i:04d}" for i in range(1, 11)}
        assert keys1.isdisjoint(keys2)

        # 核对版本关系：窗口登记版本与产物实际字段版本一致
        assert ew.get_entry(w1["id"])["结构版本"] == "v1"
        assert ew.get_entry(w2["id"])["结构版本"] == "v2"
        assert ew.read_window_records(w1["id"])["artifact_version"] == "v1"
        assert ew.read_window_records(w2["id"])["artifact_version"] == "v2"

        # 旧窗迁移到 v2 后：两份产物字段版本对齐，记录键各自保留
        migration = ew.migrate_window_records(w1["id"], target_version="v2")
        assert "error" not in migration
        migrated = ew.read_window_records(w1["id"])
        assert migrated["artifact_version"] == "v2"
        assert migrated["registered_version"] == "v2"
        assert {rec["record_key"] for rec in migrated["records"]} == keys1
