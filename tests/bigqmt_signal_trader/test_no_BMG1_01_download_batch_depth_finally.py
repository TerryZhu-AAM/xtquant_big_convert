"""[BMG5-02 / BMG1-01 回归锁] _download_batch_depth 重入计数 try/finally 兜底.

背景 (55426b3, BMG1-01 修复): download_history_data2 批轮询内的 get_market_data_ex
是同步 RPC, 可抛异常 (超时/断连/QMT 抖动); 修复前计数只增不减 (无 finally) → 一次
异常即泄漏为永久正数 → _heal_adjusted 的 none-read majority-missing 自愈
(xtquant_compat 判 ``_download_batch_depth > 0`` 即 return) 被永久抑制 → 桥静默退化
为无自愈形态, 无任何告警。

55426b3 修复本体 = try/finally 包住批循环 (异常路径计数归位), 但该笔 pathspec 恰
1 源码文件、无随行回归锁 (BMG5 审查窗实测两仓 tests/ 对 _download_batch_depth
零命中) — 本文件补锁防回归。

锁咬合性 (红证, BMG5 闭环轮, 内存变异零磁盘触碰):
- 删除 finally 块   → test_exception_path_restores_depth_to_zero 转红 (depth 残留 1);
- 删除循环前 +1 行 → 同测转红 (depth_during_get 为 None, 守卫在场被钉死)。
"""
import unittest

from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


class _FakeRow:
    """最小 duck-type: 批轮询只读 df.shape[0] 判批内就绪."""

    shape = (3, 6)


class DownloadBatchDepthGuardTest(unittest.TestCase):
    def _make_host(self, behavior):
        host = BigQmtXtData.__new__(BigQmtXtData)
        # download_server_raw / _local_cache 桩: 令函数走「cache 启用」批轮询路径,
        # 服务端下载视为已成功 — 本锁只钉批轮询计数语义, 零 redis/QMT 依赖。
        host.download_server_raw = lambda *a, **k: {"finished": len(a[0])}
        host._local_cache = lambda: object()
        observed = {"calls": 0, "depth_during_get": None}

        def fake_get_market_data_ex(*a, **k):
            observed["calls"] += 1
            observed["depth_during_get"] = getattr(host, "_download_batch_depth", None)
            return behavior(*a, **k)

        host.get_market_data_ex = fake_get_market_data_ex
        return host, observed

    def test_exception_path_restores_depth_to_zero(self):
        """RPC 异常必须穿透且计数归位 — 泄漏即自愈被永久抑制 (BMG1-01 事故形态)."""

        def boom(*a, **k):
            raise RuntimeError("qmt rpc boom (模拟超时/断连)")

        host, observed = self._make_host(boom)
        with self.assertRaises(RuntimeError):
            host.download_history_data2(["600028.SH", "603588.SH"], period="1d")
        self.assertEqual(
            observed["depth_during_get"], 1,
            "批轮询期间 _download_batch_depth 必须为 1 — 守卫计数缺失则下载中触发 "
            "_heal_adjusted 服务端双重下载 (上游 #47 形态)",
        )
        self.assertEqual(
            getattr(host, "_download_batch_depth", 0), 0,
            "异常穿透后计数必须归零 — 无 finally 时泄漏为永久正数, none-read "
            "majority-missing 自愈被永久抑制 (55426b3/BMG1-01, BMG5-02 补锁)",
        )

    def test_normal_path_returns_and_leaves_depth_zero(self):
        def all_ready(*a, **k):
            return {"600028.SH": _FakeRow(), "603588.SH": _FakeRow()}

        host, observed = self._make_host(all_ready)
        result = host.download_history_data2(["600028.SH", "603588.SH"], period="1d")
        self.assertEqual(result, {"finished": 2, "total": 2})
        self.assertEqual(observed["calls"], 1)
        self.assertEqual(
            getattr(host, "_download_batch_depth", 0), 0,
            "正常完成同样必须归零 — finally 对成功/异常双向兜底",
        )


if __name__ == "__main__":
    unittest.main()
