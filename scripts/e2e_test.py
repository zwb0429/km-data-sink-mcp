# -*- coding: utf-8 -*-
"""E2E 端到端验证：模拟 MCP 客户端经 stdio 与 km-data-sink-mcp 对话。

前提：先执行 `mvn package`（产出 target/km-data-sink-mcp.jar）。
帧格式：SDK 2.1.0 stdio 为 newline-delimited JSON-RPC。
运行：python scripts/e2e_test.py
"""
import json, subprocess, threading, time, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)

# 每次运行使用独立临时 DB
TEST_DB = os.path.join("target", "e2e-test.db")
CONFIG = os.path.join("target", "e2e-config.yaml")
os.makedirs("target", exist_ok=True)
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

with open(CONFIG, "w", encoding="utf-8") as f:
    f.write(f"""
connections:
  local:
    type: sqlite
    path: {TEST_DB.replace(chr(92), '/')}
    pool_size: 1
    sqlite:
      busy_timeout_ms: 5000
      wal_mode: true
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
    - name: daily_report
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
      upsert_key:
        - task_name
        - business_date
        - format
""")

proc = subprocess.Popen(
    ["java", "-jar", "target/km-data-sink-mcp.jar", "--config", CONFIG],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    bufsize=0)


def send(obj):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    proc.stdin.write(body + b"\n")


def read_frame():
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError("stdout closed")
    return json.loads(line.decode("utf-8"))


try:
    # 1. initialize
    send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
          "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                     "clientInfo": {"name": "e2e-test", "version": "1.0"}}})
    init = read_frame()
    print("INIT OK:", init["result"]["serverInfo"]["name"], init["result"]["serverInfo"]["version"])

    # 2. initialized 通知 + tools/list
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    tools = read_frame()
    tool_names = sorted(t["name"] for t in tools["result"]["tools"])
    print("TOOLS:", tool_names)
    assert tool_names == sorted(["start_execution", "save_document", "get_document",
                                 "list_documents", "get_execution", "test_connection",
                                 "list_connections"]), "工具列表不完整"

    # 3. test_connection
    send({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
        "name": "test_connection", "arguments": {"connection": "local"}}})
    tc = read_frame()["result"]["structuredContent"]
    assert tc["success"], "test_connection 失败: " + tc["message"]
    print("TEST_CONNECTION OK")

    # 4. save_document（覆盖模式）
    content = "<html><body><h1>E2E 日报</h1><p>hello</p></body></html>"
    send({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {
        "name": "save_document",
        "arguments": {"connection": "local", "table": "daily_report",
                      "task_name": "e2e_task", "run_id": "e2e_task_20261117_150000",
                      "business_date": "2026-11-17", "format": "html",
                      "content": content}}})
    save = read_frame()["result"]["structuredContent"]
    print("SAVE:", save["code"], save["data"]["document_id"][:8], save["data"]["operation"])
    assert save["success"] and save["code"] == "SUCCESS", "save 失败: " + save["message"]
    doc_id = save["data"]["document_id"]

    # 5. 重复 save（覆盖）→ update + document_id 稳定
    send({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {
        "name": "save_document",
        "arguments": {"connection": "local", "table": "daily_report",
                      "task_name": "e2e_task", "run_id": "e2e_task_20261117_160000",
                      "business_date": "2026-11-17", "format": "html",
                      "content": content + "<p>updated</p>"}}})
    save2 = read_frame()["result"]["structuredContent"]
    assert save2["success"] and save2["data"]["operation"] == "update", "应为 update"
    assert save2["data"]["document_id"] == doc_id, "document_id 应稳定"
    print("OVERWRITE OK (update, document_id stable)")

    # 6. list_documents
    send({"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {
        "name": "list_documents", "arguments": {"connection": "local", "table": "daily_report"}}})
    lst = read_frame()["result"]["structuredContent"]
    assert lst["success"] and lst["data"]["total"] == 1, "总数应为 1"
    assert "content" not in lst["data"]["items"][0], "列表不应含全文"
    print("LIST OK: total=1, 无全文")

    # 7. get_document（业务键）
    send({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {
        "name": "get_document",
        "arguments": {"connection": "local", "table": "daily_report",
                      "task_name": "e2e_task", "business_date": "2026-11-17",
                      "format": "html"}}})
    doc = read_frame()["result"]["structuredContent"]
    assert doc["success"] and doc["data"]["document_id"] == doc_id, "get_document 失败"
    assert doc["data"]["content"] == content + "<p>updated</p>", "content 应为最新"
    print("GET_DOCUMENT OK (原文一致)")

    # 8. get_execution
    send({"jsonrpc": "2.0", "id": 8, "method": "tools/call", "params": {
        "name": "get_execution",
        "arguments": {"connection": "local", "task_name": "e2e_task",
                      "run_id": "e2e_task_20261117_150000"}}})
    exe = read_frame()["result"]["structuredContent"]
    assert exe["success"] and exe["data"]["status"] == "SUCCESS", "执行记录应为 SUCCESS"
    print("GET_EXECUTION OK:", exe["data"]["status"])

    # 9. 非白名单表 → INVALID_TABLE + isError
    send({"jsonrpc": "2.0", "id": 9, "method": "tools/call", "params": {
        "name": "save_document",
        "arguments": {"connection": "local", "table": "evil_table",
                      "task_name": "t", "run_id": "r", "business_date": "2026-11-17",
                      "format": "html", "content": "x"}}})
    bad_raw = read_frame()
    bad = bad_raw["result"]["structuredContent"]
    assert bad["success"] is False and bad["code"] == "INVALID_TABLE", "应拒绝非白名单表"
    assert bad_raw["result"].get("isError") is True, "isError 应为 true"
    print("INVALID_TABLE OK (isError=true)")

    # 10. list_connections（脱敏）
    send({"jsonrpc": "2.0", "id": 10, "method": "tools/call", "params": {
        "name": "list_connections", "arguments": {}}})
    lc = read_frame()["result"]["structuredContent"]
    conns = lc["data"]["connections"]
    assert any(c["name"] == "local" for c in conns), "连接列表缺 local"
    assert "password" not in json.dumps(conns).lower(), "不应含密码"
    print("LIST_CONNECTIONS OK (脱敏)")

    # 11. start_execution 状态机：重复 RUNNING
    send({"jsonrpc": "2.0", "id": 11, "method": "tools/call", "params": {
        "name": "start_execution",
        "arguments": {"connection": "local", "task_name": "e2e_state",
                      "run_id": "state_run1"}}})
    se = read_frame()["result"]["structuredContent"]
    assert se["success"], "start_execution 失败: " + se["message"]
    send({"jsonrpc": "2.0", "id": 12, "method": "tools/call", "params": {
        "name": "start_execution",
        "arguments": {"connection": "local", "task_name": "e2e_state",
                      "run_id": "state_run1"}}})
    se2 = read_frame()["result"]["structuredContent"]
    assert not se2["success"] and se2["code"] == "EXECUTION_ALREADY_RUNNING", "应报 ALREADY_RUNNING"
    print("EXECUTION STATE OK (ALREADY_RUNNING)")

    # 12. get_document 不完整条件 → INVALID_QUERY_CONDITION
    send({"jsonrpc": "2.0", "id": 13, "method": "tools/call", "params": {
        "name": "get_document",
        "arguments": {"connection": "local", "table": "daily_report",
                      "task_name": "e2e_task"}}})
    iq = read_frame()["result"]["structuredContent"]
    assert not iq["success"] and iq["code"] == "INVALID_QUERY_CONDITION", "应报条件不完整"
    print("INVALID_QUERY_CONDITION OK")

    print("\n=== E2E ALL PASS ===")
finally:
    try:
        proc.kill()
    except Exception:
        pass
