# -*- coding: utf-8 -*-
"""MySQL 真实端到端验证：通过 MCP stdio 向 mp_editor 库写临时表并查询验证。
完成后删除临时表与执行记录表中本测试的记录。"""
import json, subprocess, threading, time, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)

PASSWORD = os.environ.get("SINK_DB_PASSWORD")
if not PASSWORD:
    raise SystemExit("请先设置环境变量 SINK_DB_PASSWORD（MySQL root 密码）")
os.environ["SINK_DB_PASSWORD"] = PASSWORD

HOST = os.environ.get("E2E_MYSQL_HOST", "127.0.0.1")
PORT = os.environ.get("E2E_MYSQL_PORT", "3306")
DATABASE = os.environ.get("E2E_MYSQL_DB", "mp_editor")

CFG = "target/e2e-mysql-config.yaml"
with open(CFG, "w", encoding="utf-8") as f:
    f.write(f"""
connections:
  default:
    type: mysql
    host: {HOST}
    port: {PORT}
    database: {DATABASE}
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
    - name: mcp_e2e_tmp
      mode: overwrite
      comment: MCP E2E 临时表（验证表注释）
      columns:
        - column: task_name
          type: VARCHAR(128)
          source: task_name
          nullable: false
          comment: 任务名称
        - column: business_date
          type: DATE
          source: business_date
          nullable: false
          comment: 业务日期
        - column: format
          type: VARCHAR(8)
          source: format
          nullable: false
          comment: 内容格式
        - column: content
          type: LONG_TEXT
          source: content
          nullable: false
          comment: 文档正文
        - column: title
          type: TEXT
          source: meta.title
          nullable: true
          comment: 文档标题
      upsert_key:
        - task_name
        - business_date
        - format
""")

proc = subprocess.Popen(
    ["java", "-jar", "target/km-data-sink-mcp.jar", "--config", CFG],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    bufsize=0)

errs = []
def er():
    try:
        for line in proc.stderr:
            errs.append(line)
    except (TypeError, ValueError):
        pass  # stderr 已关闭
threading.Thread(target=er, daemon=True).start()


def send(obj):
    proc.stdin.write(json.dumps(obj, ensure_ascii=False).encode("utf-8") + b"\n")


def read_frame():
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError("stdout closed; errs=" + b"".join(errs).decode("utf-8", "replace")[-600:])
    return json.loads(line.decode("utf-8"))


try:
    # initialize（等待 MySQL 建池，stderr 提示约 16s）
    send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
          "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                     "clientInfo": {"name": "e2e-mysql", "version": "1.0"}}})
    init = read_frame()
    print("INIT OK:", init["result"]["serverInfo"]["name"], init["result"]["serverInfo"]["version"])

    send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    # test_connection
    send({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
        "name": "test_connection", "arguments": {"connection": "default"}}})
    tc = read_frame()["result"]["structuredContent"]
    assert tc["success"], tc["message"]
    print("TEST_CONNECTION OK:", tc["data"]["database"][:30])

    # save_document（覆盖模式，首次 insert）
    content = "<html><body><h1>MySQL E2E 日报</h1><p>MySQL 端到端验证通过</p></body></html>"
    send({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
        "name": "save_document",
        "arguments": {"connection": "default", "table": "mcp_e2e_tmp",
                      "task_name": "mysql_e2e", "run_id": "mysql_e2e_20261118_100000",
                      "business_date": "2026-11-18", "format": "html",
                      "content": content, "meta": {"title": "MySQL E2E"}}}})
    save = read_frame()["result"]["structuredContent"]
    print("SAVE:", save["code"], save["data"]["operation"], "doc=" + save["data"]["document_id"][:8])
    assert save["success"] and save["code"] == "SUCCESS" and save["data"]["operation"] == "insert"

    # save_document（相同业务键，不同 run_id → update，document_id 稳定）
    doc_id = save["data"]["document_id"]
    send({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {
        "name": "save_document",
        "arguments": {"connection": "default", "table": "mcp_e2e_tmp",
                      "task_name": "mysql_e2e", "run_id": "mysql_e2e_20261118_110000",
                      "business_date": "2026-11-18", "format": "html",
                      "content": content + "<p>updated</p>", "meta": {"title": "MySQL E2E v2"}}}})
    save2 = read_frame()["result"]["structuredContent"]
    print("OVERWRITE:", save2["data"]["operation"], "doc_id stable =", save2["data"]["document_id"] == doc_id)
    assert save2["data"]["operation"] == "update" and save2["data"]["document_id"] == doc_id

    # get_document（业务键查询）
    send({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {
        "name": "get_document",
        "arguments": {"connection": "default", "table": "mcp_e2e_tmp",
                      "task_name": "mysql_e2e", "business_date": "2026-11-18",
                      "format": "html"}}})
    doc = read_frame()["result"]["structuredContent"]
    assert doc["success"] and doc["data"]["content"] == content + "<p>updated</p>"
    print("GET_DOCUMENT OK (content 原文一致, title =", doc["data"]["title"], ")")

    # list_documents
    send({"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {
        "name": "list_documents", "arguments": {"connection": "default", "table": "mcp_e2e_tmp"}}})
    lst = read_frame()["result"]["structuredContent"]
    assert lst["success"] and lst["data"]["total"] == 1
    print("LIST OK: total =", lst["data"]["total"])

    # get_execution（T27：不同 run_id 各有执行记录）
    send({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {
        "name": "get_execution",
        "arguments": {"connection": "default", "task_name": "mysql_e2e",
                      "run_id": "mysql_e2e_20261118_100000"}}})
    e1 = read_frame()["result"]["structuredContent"]
    send({"jsonrpc": "2.0", "id": 8, "method": "tools/call", "params": {
        "name": "get_execution",
        "arguments": {"connection": "default", "task_name": "mysql_e2e",
                      "run_id": "mysql_e2e_20261118_110000"}}})
    e2 = read_frame()["result"]["structuredContent"]
    assert e1["success"] and e2["success"]
    assert e1["data"]["status"] == "SUCCESS" and e2["data"]["status"] == "SUCCESS"
    assert e1["data"]["document_id"] == e2["data"]["document_id"] == doc_id
    print("GET_EXECUTION OK: 两次执行均 SUCCESS，关联同一 document_id")

    print("\n=== MYSQL E2E ALL PASS ===")
finally:
    try:
        proc.kill()
    except Exception:
        pass
