# -*- coding: utf-8 -*-
"""km-data-sink-mcp 压测脚本

四个阶段：
  A. SQLite 单进程：吞吐（串行 save）、大文档边界（5MB）、错误路径（INVALID_TABLE/CONTENT_TOO_LARGE）、
     list_documents 分页、get_document 往返
  B. SQLite 多进程并发：N 个独立 server 进程同时写同一 db 文件（WAL + busy_timeout + 重试）
  C. 远程 MySQL：串行写入吞吐 + 覆盖 upsert，测后清理临时表

用法：
  python scripts/stress_test.py              # 全部阶段
  python scripts/stress_test.py A            # 仅 SQLite 单进程
  python scripts/stress_test.py B            # 仅多进程
  python scripts/stress_test.py C            # 仅 MySQL（需 SINK_DB_PASSWORD）
"""
import json
import os
import random
import statistics
import string
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
PY = sys.executable
JAR = "target/km-data-sink-mcp.jar"


# ---------------- MCP stdio 客户端 ----------------

class McpClient:
    """newline-delimited JSON-RPC 客户端（SDK 2.1.0 stdio 帧格式）。"""

    def __init__(self, config_path, tag="", init_wait=0):
        self.proc = subprocess.Popen(
            ["java", "-jar", JAR, "--config", config_path],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            bufsize=0)
        self._id = 0
        self.tag = tag
        if init_wait:
            # MySQL 远程建池较慢（实测约 16s），等 server 就绪再握手
            print(f"  [{tag}] 等待 server 初始化 {init_wait}s ...", flush=True)
            time.sleep(init_wait)
        self._init()

    def _init(self):
        self.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                    "clientInfo": {"name": "stress", "version": "1.0"}})
        self.proc.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')

    def request(self, method, params):
        self._id += 1
        rid = self._id
        body = json.dumps({"jsonrpc": "2.0", "id": rid, "method": method,
                           "params": params}, ensure_ascii=False).encode("utf-8")
        self.proc.stdin.write(body + b"\n")
        line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f"[{self.tag}] stdout closed at id={rid}")
        resp = json.loads(line.decode("utf-8"))
        if resp.get("id") != rid:
            raise RuntimeError(f"[{self.tag}] response id mismatch: {resp.get('id')} != {rid}")
        return resp

    def call(self, tool, arguments):
        return self.request("tools/call", {"name": tool, "arguments": arguments})

    def close(self):
        try:
            self.proc.kill()
        except Exception:
            pass


def save_args(table, task, run_id, biz_date, fmt, content, meta=None):
    a = {"connection": "default", "table": table, "task_name": task, "run_id": run_id,
         "business_date": biz_date, "format": fmt, "content": content}
    if meta:
        a["meta"] = meta
    return a


def measure(fn, n):
    """执行 n 次返回 (成功数, 失败数, 时延列表 ms, 总耗时 s)"""
    lat, ok, fail = [], 0, 0
    t0 = time.perf_counter()
    for _ in range(n):
        try:
            r = fn()
            ok += 1 if r else 0
        except Exception:
            fail += 1
        lat.append((time.perf_counter() - t0) * 1000)
    total = time.perf_counter() - t0
    # 逐次时延 = 相邻差
    deltas = [lat[0]] + [lat[i] - lat[i - 1] for i in range(1, len(lat))]
    return ok, fail, deltas, total


def pctl(deltas, p):
    if not deltas:
        return 0
    s = sorted(deltas)
    return s[min(len(s) - 1, int(len(s) * p / 100))]


def report(name, ok, fail, deltas, total):
    print(f"  [{name}] 成功={ok} 失败={fail} 总耗时={total:.2f}s "
          f"吞吐={ok / total:.1f} ops/s "
          f"延迟 p50={pctl(deltas, 50):.0f}ms p95={pctl(deltas, 95):.0f}ms "
          f"p99={pctl(deltas, 99):.0f}ms max={pctl(deltas, 100):.0f}ms")


# ---------------- 配置生成 ----------------

def write_sqlite_config(path, db_path, page_size=100, max_retry=5, backoff=300):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"""
connections:
  default:
    type: sqlite
    path: {db_path}
    pool_size: 1
    sqlite:
      busy_timeout_ms: 5000
      wal_mode: true
content:
  max_size_mb: 5
db:
  max_retry: {max_retry}
  retry_backoff_ms: {backoff}
  query_timeout_s: 30
  pool_size: 5
query:
  default_page_size: 20
  max_page_size: {page_size}
tables:
  auto_create: true
  allowlist:
    - name: stress_doc
      mode: overwrite
      columns:
        - column: task_name
          type: VARCHAR(128)
          source: task_name
          nullable: false
        - column: business_date
          type: DATE
          source: business_date
          nullable: false
        - column: format
          type: VARCHAR(8)
          source: format
          nullable: false
        - column: content
          type: LONG_TEXT
          source: content
          nullable: false
        - column: title
          type: TEXT
          source: meta.title
          nullable: true
      upsert_key:
        - task_name
        - business_date
        - format
    - name: stress_hist
      mode: history
      columns:
        - column: task_name
          type: VARCHAR(128)
          source: task_name
          nullable: false
        - column: run_id
          type: VARCHAR(128)
          source: run_id
          nullable: false
        - column: format
          type: VARCHAR(8)
          source: format
          nullable: false
        - column: content
          type: LONG_TEXT
          source: content
          nullable: false
        - column: created_at
          type: TIMESTAMP
          source: system.now
          nullable: false
      upsert_key:
        - task_name
        - run_id
        - format
""")


def write_mysql_config(path):
    host = os.environ.get("E2E_MYSQL_HOST", "127.0.0.1")
    port = os.environ.get("E2E_MYSQL_PORT", "3306")
    database = os.environ.get("E2E_MYSQL_DB", "mp_editor")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"""
connections:
  default:
    type: mysql
    host: {host}
    port: {port}
    database: {database}
    username: root
    password_env: SINK_DB_PASSWORD
    pool_size: 5
    query_timeout_s: 30
content:
  max_size_mb: 5
db:
  max_retry: 3
  retry_backoff_ms: 200
  query_timeout_s: 30
  pool_size: 5
query:
  default_page_size: 20
  max_page_size: 100
tables:
  auto_create: true
  allowlist:
    - name: mcp_stress_tmp
      mode: overwrite
      columns:
        - column: task_name
          type: VARCHAR(128)
          source: task_name
          nullable: false
        - column: business_date
          type: DATE
          source: business_date
          nullable: false
        - column: format
          type: VARCHAR(8)
          source: format
          nullable: false
        - column: content
          type: LONG_TEXT
          source: content
          nullable: false
        - column: title
          type: TEXT
          source: meta.title
          nullable: true
      upsert_key:
        - task_name
        - business_date
        - format
""")


def rand_content(kb):
    """生成约 kb KB 的伪随机 HTML 内容"""
    chunk = "".join(random.choices(string.ascii_letters + string.digits, k=64))
    lines = (chunk + "<br>\n") * max(1, kb * 1024 // 68)
    return f"<html><body><h1>stress</h1><p>{lines}</p></body></html>"[:kb * 1024]


# ---------------- Phase A：SQLite 单进程 ----------------

def phase_a():
    print("=" * 72)
    print("Phase A: SQLite 单进程（吞吐 / 大文档 / 错误路径 / 查询）")
    print("=" * 72)
    db = os.path.abspath("target/stress-a.db")
    cfg = "target/stress-a.yaml"
    for p in (db, db + "-wal", db + "-shm"):
        if os.path.exists(p):
            os.remove(p)
    write_sqlite_config(cfg, db.replace("\\", "/"))

    c = McpClient(cfg, "A")
    try:
        # A1: overwrite 插入吞吐（200 个不同业务键 → 200 行，纯 INSERT 路径）
        content8 = rand_content(8)
        lat, ok, fail = [], 0, 0
        t0 = time.perf_counter()
        for i in range(200):
            biz = f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}"
            s = time.perf_counter()
            sc = c.call("save_document", save_args(
                "stress_doc", "a1", f"a1_{i:04d}", biz, "html", content8, {"title": "a1"})
            )["result"]["structuredContent"]
            lat.append((time.perf_counter() - s) * 1000)
            ok += 1 if sc["success"] else 0
            fail += 0 if sc["success"] else 1
        total = time.perf_counter() - t0
        report("A1 insert-8KB x200 (overwrite, 200 键)", ok, fail, lat, total)

        # A1b: 同键覆盖吞吐（200 次更新同一行，SELECT+UPDATE 路径）
        lat, ok, fail = [], 0, 0
        t0 = time.perf_counter()
        for i in range(200):
            s = time.perf_counter()
            sc = c.call("save_document", save_args(
                "stress_doc", "a1cov", "a1cov_fixed", "2026-11-19", "html",
                content8 + f"<p>v{i}</p>", {"title": "a1cov"})
            )["result"]["structuredContent"]
            lat.append((time.perf_counter() - s) * 1000)
            ok += 1 if sc["success"] else 0
        total = time.perf_counter() - t0
        report("A1b overwrite-8KB x200 (同键更新)", ok, fail, lat, total)

        # A2: history 吞吐（200 次不同 run_id → 200 行新增）
        lat, ok, fail = [], 0, 0
        t0 = time.perf_counter()
        for i in range(200):
            s = time.perf_counter()
            sc = c.call("save_document", save_args(
                "stress_hist", "a2", f"a2_{i:04d}", "2026-11-19", "html", content8)
            )["result"]["structuredContent"]
            lat.append((time.perf_counter() - s) * 1000)
            ok += 1 if sc["success"] else 0
            fail += 0 if sc["success"] else 1
        total = time.perf_counter() - t0
        report("A2 history-8KB x200 (200 run_id)", ok, fail, lat, total)

        # A3: 大文档边界 —— 5MB 应成功、5MB+1 应被拒
        big5 = rand_content(5 * 1024)          # ~5MB（脚本字符≈字节，ASCII）
        r = c.call("save_document", save_args(
            "stress_doc", "a3big", "a3big_1", "2026-11-19", "html", big5))
        sc = r["result"]["structuredContent"]
        print(f"  [A3 5MB 文档] success={sc['success']} code={sc['code']} "
              f"耗时={sc.get('data', {}).get('affected_rows', '')}"
              if sc["success"] else f"  [A3 5MB 文档] 被拒: {sc['code']}")
        assert sc["success"], "恰好 5MB 应该成功"

        big5x = rand_content(5 * 1024 + 2)     # >5MB
        t0 = time.perf_counter()
        r = c.call("save_document", save_args(
            "stress_doc", "a3big", "a3big_2", "2026-11-19", "html", big5x))
        dt = (time.perf_counter() - t0) * 1000
        sc = r["result"]["structuredContent"]
        assert not sc["success"] and sc["code"] == "CONTENT_TOO_LARGE", sc
        print(f"  [A3 5MB+ 超限拒绝] CONTENT_TOO_LARGE 校验耗时 {dt:.0f}ms（应极快，不落库）")

        # A4: 错误路径吞吐（非法表名，应快速失败不落库）
        ok, fail, d, t = measure(
            lambda: (lambda sc: sc if not sc["success"] else (_ for _ in ()).throw(AssertionError(sc)))(
                c.call("save_document", save_args(
                    "not_in_allowlist", "a4", "a4_1", "2026-11-19", "html", "x"))
                ["result"]["structuredContent"]), 100)
        report("A4 INVALID_TABLE x100", ok, fail, d, t)

        # A5: get_document 往返（读回 A1 的 8KB 文档 ×100）
        def get_doc():
            return c.call("get_document", {"connection": "default", "table": "stress_doc",
                                           "task_name": "a1", "business_date": "2026-11-19",
                                           "format": "html"})["result"]["structuredContent"]["success"]
        ok, fail, d, t = measure(get_doc, 100)
        report("A5 get_document-8KB x100", ok, fail, d, t)

        # A6: list_documents 分页（单页 100 条 ×50）
        def list_docs():
            return c.call("list_documents", {"connection": "default", "table": "stress_hist",
                                             "page": 1, "page_size": 100}
                          )["result"]["structuredContent"]["success"]
        ok, fail, d, t = measure(list_docs, 50)
        report("A6 list_documents x50", ok, fail, d, t)

        # 库内实际行数核对：a1 200 行（200 个业务键）+ a1cov 1 行 + a3big 1 行 = 202
        lst = c.call("list_documents", {"connection": "default", "table": "stress_doc",
                                        "page": 1, "page_size": 1})["result"]["structuredContent"]
        print(f"  [数据核对] stress_doc 总行数={lst['data']['total']}（预期 202：a1×200 + a1cov×1 + a3big×1）")
        lst2 = c.call("list_documents", {"connection": "default", "table": "stress_hist",
                                         "page": 1, "page_size": 1})["result"]["structuredContent"]
        print(f"  [数据核对] stress_hist 总行数={lst2['data']['total']}（预期 200）")
    finally:
        c.close()


# ---------------- Phase B：SQLite 多进程并发 ----------------

def phase_b(n_procs=6, per_proc=30):
    print("=" * 72)
    print(f"Phase B: SQLite 多进程并发（{n_procs} 进程 × {per_proc} 次写，WAL + busy_timeout + 重试）")
    print("=" * 72)
    db = os.path.abspath("target/stress-b.db")
    cfg = "target/stress-b.yaml"
    for p in (db, db + "-wal", db + "-shm"):
        if os.path.exists(p):
            os.remove(p)
    write_sqlite_config(cfg, db.replace("\\", "/"))

    results = [None] * n_procs
    errors = []
    barrier = threading.Barrier(n_procs)

    def worker(idx):
        try:
            c = McpClient(cfg, f"B{idx}")
            barrier.wait(timeout=60)   # 所有进程就绪后同时开写
            lat, ok, fail = [], 0, 0
            t0 = time.perf_counter()
            for i in range(per_proc):
                s = time.perf_counter()
                r = c.call("save_document", save_args(
                    "stress_hist", f"proc{idx}", f"proc{idx}_r{i}",
                    "2026-11-19", "html", rand_content(4), {"title": f"p{idx}"}))
                dt = (time.perf_counter() - s) * 1000
                lat.append(dt)
                sc = r["result"]["structuredContent"]
                if sc["success"]:
                    ok += 1
                else:
                    fail += 1
                    errors.append(f"proc{idx}: {sc['code']} {sc['message'][:80]}")
            total = time.perf_counter() - t0
            results[idx] = (ok, fail, lat, total)
            c.close()
        except Exception as e:
            errors.append(f"proc{idx} crashed: {e}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_procs)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0

    total_ok = sum(r[0] for r in results if r)
    total_fail = sum(r[1] for r in results if r)
    all_lat = [x for r in results if r for x in r[2]]
    print(f"  并发写完成: 成功={total_ok}/{n_procs * per_proc} 失败={total_fail} "
          f"墙钟={wall:.2f}s 总吞吐={total_ok / wall:.1f} ops/s "
          f"延迟 p50={pctl(all_lat, 50):.0f}ms p95={pctl(all_lat, 95):.0f}ms p99={pctl(all_lat, 99):.0f}ms")
    for i, r in enumerate(results):
        if r:
            print(f"    proc{i}: ok={r[0]} fail={r[1]} p50={pctl(r[2], 50):.0f}ms max={pctl(r[2], 100):.0f}ms")
    if errors:
        print(f"  [错误明细] 前 5 条:")
        for e in errors[:5]:
            print(f"    - {e}")

    # 数据一致性核对
    c = McpClient(cfg, "B-verify")
    try:
        r = c.call("list_documents", {"connection": "default", "table": "stress_hist",
                                      "page": 1, "page_size": 100})["result"]["structuredContent"]
        if not r.get("success"):
            raise RuntimeError("list_documents failed: " + json.dumps(r, ensure_ascii=False)[:200])
        total = r["data"]["total"]
        expect = n_procs * per_proc
        status = "一致" if total == expect else f"不一致（预期 {expect}）"
        print(f"  [一致性核对] 库内 stress_hist 行数={total} 预期={expect} → {status}")
    finally:
        c.close()


# ---------------- Phase C：远程 MySQL ----------------

def phase_c(n=60):
    print("=" * 72)
    print(f"Phase C: 远程 MySQL（{os.environ.get('E2E_MYSQL_HOST', '127.0.0.1')}/{os.environ.get('E2E_MYSQL_DB', 'mp_editor')}，临时表 mcp_stress_tmp，{n} 次写）")
    print("=" * 72)
    if not os.environ.get("SINK_DB_PASSWORD"):
        raise SystemExit("请先设置环境变量 SINK_DB_PASSWORD（MySQL root 密码）")
    cfg = "target/stress-c.yaml"
    write_mysql_config(cfg)

    c = McpClient(cfg, "C", init_wait=25)
    try:
        content4 = rand_content(4)
        # C1: 首次插入（不同业务键）
        ok, fail, d, t = measure(
            lambda: c.call("save_document", save_args(
                "mcp_stress_tmp", "c1", f"c1_{random.randint(0, 1_000_000)}",
                "2026-11-19", "html", content4, {"title": "c1"}))
            ["result"]["structuredContent"]["success"], n)
        report("C1 insert-4KB x60", ok, fail, d, t)

        # C2: 覆盖更新（同业务键，先查后写 = SELECT+UPDATE 两次往返）
        ok, fail, d, t = measure(
            lambda: c.call("save_document", save_args(
                "mcp_stress_tmp", "c2", f"c2_{random.randint(0, 1_000_000)}",
                "2026-11-19", "html", content4, {"title": "c2"}))
            ["result"]["structuredContent"]["success"], n)
        report("C2 insert-4KB x60 (键2)", ok, fail, d, t)

        r = c.call("save_document", save_args(
            "mcp_stress_tmp", "c2", "c2_fixed", "2026-11-19", "html",
            content4 + "<p>upd</p>", {"title": "c2"}))["result"]["structuredContent"]
        print(f"  [C2 覆盖验证] operation={r['data']['operation']}（应为 update，先查后写=2 次网络往返）")

        lst = c.call("list_documents", {"connection": "default", "table": "mcp_stress_tmp",
                                        "page": 1, "page_size": 1})["result"]["structuredContent"]
        print(f"  [数据核对] mcp_stress_tmp 总行数={lst['data']['total']}（预期 {n}：c1 与 c2 各 n 行）")
    finally:
        c.close()
    print("  [清理提示] mcp_stress_tmp 为临时压测表，如需删除：DROP TABLE mcp_stress_tmp;"
          " DELETE FROM mcp_task_execution WHERE table_name='mcp_stress_tmp';")


# ---------------- main ----------------

if __name__ == "__main__":
    phase = sys.argv[1].upper() if len(sys.argv) > 1 else "ABC"
    os.makedirs("target", exist_ok=True)
    if "A" in phase:
        phase_a()
    if "B" in phase:
        phase_b()
    if "C" in phase:
        phase_c()
    print("\n=== STRESS DONE ===")
