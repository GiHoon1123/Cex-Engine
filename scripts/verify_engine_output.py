"""이 실행 소유의 디비·카프카·엔진만 사용해 실제 출력 순서와 실패 차단을 검증한다."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import tempfile
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT.parent / ".local"
DOCKER = shutil.which("docker") or "/usr/local/bin/docker"
prefix = "cex-output-" + uuid4().hex[:10]
pg, kafka = prefix + "-pg", prefix + "-kafka"
processes = []
evidence = {"containers": [pg, kafka], "checks": []}
journal_directory = tempfile.TemporaryDirectory(prefix="cex-process-journal-")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def docker(*args, **kwargs):
    return subprocess.run([DOCKER, *args], check=True, text=True, **kwargs)


def sql(statement):
    return subprocess.check_output([DOCKER, "exec", pg, "psql", "-U", "root", "-d", "cex",
        "-v", "ON_ERROR_STOP=1", "-At", "-c", statement], text=True).strip()


def http(path, body=None, method=None):
    request = Request(f"http://127.0.0.1:{engine_port}" + path,
        data=None if body is None else json.dumps(body).encode(), method=method,
        headers={"Content-Type": "application/json"})
    try:
        response = urlopen(request, timeout=2)
    except HTTPError as error:
        response = error
    with response:
        return response.status, json.loads(response.read())


def until(probe, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if probe():
                return
        except (URLError, OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        time.sleep(0.1)
    raise AssertionError("isolated output verification timed out")


def submit(oid, user, side, amount):
    sql(f"INSERT INTO orders(id,user_id,order_type,order_side,base_mint,quote_mint,price,amount) "
        f"VALUES({oid},{user},'{side}','limit','SOL','USDT',100,{amount}); "
        f"INSERT INTO order_submissions VALUES({oid},{user},'{side}','limit','SOL','USDT',100,{amount},NULL)")
    status, receipt = http("/api/cex/orders", {"order_id": oid, "user_id": user,
        "order_type": side, "order_side": "limit", "base_mint": "SOL", "quote_mint": "USDT",
        "price": "100", "amount": str(amount)})
    assert status == 202 and receipt["order_id"] == str(oid), (status, receipt)


try:
    ARTIFACTS.mkdir(exist_ok=True)
    docker("run", "--detach", "--rm", "--name", pg, "--publish", "127.0.0.1::5432",
        "--tmpfs", "/var/lib/postgresql/data", "-e", "POSTGRES_USER=root", "-e", "POSTGRES_PASSWORD=1234",
        "-e", "POSTGRES_DB=cex", "postgres:16", stdout=subprocess.DEVNULL)
    until(lambda: subprocess.run([DOCKER, "exec", pg, "pg_isready", "-h", "127.0.0.1", "-U", "root", "-d", "cex"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0)
    database_address = subprocess.check_output([DOCKER, "port", pg, "5432/tcp"], text=True).strip()
    sql("CREATE TABLE user_balances(id BIGSERIAL PRIMARY KEY,user_id BIGINT,mint_address TEXT,available NUMERIC,locked NUMERIC); "
        "INSERT INTO user_balances(user_id,mint_address,available,locked) VALUES(1,'USDT',1000,0),(2,'SOL',10,0); "
        "CREATE TABLE orders(id BIGINT PRIMARY KEY,user_id BIGINT,order_type TEXT,order_side TEXT,base_mint TEXT,quote_mint TEXT,"
        "price NUMERIC,amount NUMERIC,filled_amount NUMERIC DEFAULT 0,filled_quote_amount NUMERIC DEFAULT 0,status TEXT DEFAULT 'pending',"
        "created_at TIMESTAMPTZ DEFAULT now(),updated_at TIMESTAMPTZ DEFAULT now()); "
        "CREATE TABLE order_submissions(order_id BIGINT UNIQUE,user_id BIGINT,order_type TEXT,order_side TEXT,base_mint TEXT,quote_mint TEXT,"
        "price NUMERIC(30,9),amount NUMERIC(30,9),quote_amount NUMERIC(30,9)); " + (ROOT / "schema/order_receipts.sql").read_text())
    kafka_port, engine_port = free_port(), free_port()
    envs = {"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "EXTERNAL://:9092,INTERNAL://:29092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"EXTERNAL://127.0.0.1:{kafka_port},INTERNAL://localhost:29092",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER", "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0"}
    args = ["run", "--detach", "--rm", "--name", kafka, "--publish", f"127.0.0.1:{kafka_port}:9092"]
    for key, value in envs.items():
        args += ["--env", f"{key}={value}"]
    docker(*args, "apache/kafka:3.9.1", stdout=subprocess.DEVNULL)
    until(lambda: subprocess.run([DOCKER, "exec", kafka, "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "localhost:29092", "--list"], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, timeout=10).returncode == 0, 60)
    docker("exec", kafka, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:29092",
        "--create", "--topic", "order-events", "--partitions", "1", "--replication-factor", "1", stdout=subprocess.DEVNULL)
    env = os.environ.copy()
    env.update(DATABASE_HOST=database_address, ENGINE_HTTP_ADDRESS=f"127.0.0.1:{engine_port}",
        KAFKA_BOOTSTRAP_SERVERS=f"127.0.0.1:{kafka_port}", ENGINE_DATA_DIR=journal_directory.name)
    engine_log = ARTIFACTS / "engine-output-process.log"
    with engine_log.open("w") as log:
        engine = subprocess.Popen([str(ROOT / "target/debug/cex-engine")], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        processes.append(engine)
    until(lambda: engine.poll() is not None or http("/api/health")[0] == 200)
    assert engine.poll() is None, "isolated engine failed to start; see engine-output-process.log"
    submit(1, 2, "sell", 3)
    until(lambda: "output_completed order_id=1 " in engine_log.read_text())
    submit(2, 1, "buy", 1)
    until(lambda: "output_completed order_id=2 " in engine_log.read_text())
    # 저장된 체결은 플랫폼 테이블이 아직 pending이어도 자체 저널로 복원한다.
    # 디비 잔액도 의도적으로 바꿔 복구가 그 값에 기대지 않는지 확인한다.
    engine.kill()
    engine.wait(timeout=5)
    sql("UPDATE user_balances SET available=0,locked=99")
    docker("pause", pg, stdout=subprocess.DEVNULL)
    with engine_log.open("a") as log:
        engine = subprocess.Popen([str(ROOT / "target/debug/cex-engine")], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        processes.append(engine)
    until(lambda: engine.poll() is not None or http("/api/health")[0] == 200)
    assert engine.poll() is None, "WAL recovery failed"
    docker("unpause", pg, stdout=subprocess.DEVNULL)
    duplicate, _ = http("/api/cex/orders", {"order_id":2,"user_id":1,"order_type":"buy","order_side":"limit",
        "base_mint":"SOL","quote_mint":"USDT","price":"100","amount":"1"})
    assert duplicate == 200
    status, cancelled = http("/api/cex/orders/1?user_id=2", method="DELETE")
    assert status == 200 and cancelled["status"] == "cancelled", (status, cancelled)
    assert Decimal(str(cancelled["filled_amount"])) == 1 and Decimal(str(cancelled["amount"])) == 3
    evidence["checks"].append({"case":"sigkill_recovery_preserves_partial_order_and_deduplication","duplicate_http":200,"remaining":2})
    records = ARTIFACTS / "engine-output-kafka-records.log"
    with records.open("w") as output:
        consumer = subprocess.Popen([DOCKER, "exec", kafka, "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server", "localhost:29092", "--topic", "order-events", "--from-beginning",
            "--max-messages", "2", "--timeout-ms", "15000"], stdout=output, stderr=subprocess.DEVNULL)
        processes.append(consumer)
    consumer.wait(timeout=25)
    assert consumer.returncode == 0
    events = [json.loads(line) for line in records.read_text().splitlines() if line.startswith("{")]
    assert [event["event_type"] for event in events] == ["trade_executed", "order_cancelled"], events
    trade = events[0]
    assert (trade["buy_order_id"], trade["sell_order_id"]) == (2, 1)
    assert Decimal(str(trade["amount"])) == 1 and Decimal(str(trade["buyer_quote_available"])) == 900
    assert Decimal(str(trade["seller_base_locked"])) == 2
    evidence["checks"].append({"case": "ordered_trade_and_cancel", "events": len(events)})
    assert sql("SELECT count(*) FROM orders WHERE status='pending' AND filled_amount=0") == "2"
    evidence["checks"].append({"case": "engine_does_not_update_platform_orders", "unchanged_orders": 2})
    docker("pause", kafka, stdout=subprocess.DEVNULL)
    submit(3, 2, "sell", 1)
    until(lambda: "output_completed order_id=3 " in engine_log.read_text())
    submit(4, 1, "buy", 1)
    failure_started = time.monotonic()
    until(lambda: http("/api/health")[0] == 503, 45)
    failure_seconds = round(time.monotonic() - failure_started, 3)
    assert "output_delivery_failed" in engine_log.read_text()
    assert "output_completed order_id=4 " not in engine_log.read_text()
    evidence["checks"].append({"case": "broker_failure_closes_engine", "health": 503, "observed_seconds": failure_seconds})
    # 브로커 장애 중에도 HTTP 성공한 결과는 저널에 남아 있어야 한다.
    engine.kill()
    engine.wait(timeout=5)
    docker("unpause", kafka, stdout=subprocess.DEVNULL)
    with engine_log.open("a") as log:
        engine = subprocess.Popen([str(ROOT / "target/debug/cex-engine")], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        processes.append(engine)
    until(lambda: engine.poll() is not None or http("/api/health")[0] == 200,45)
    assert engine.poll() is None
    until(lambda: "output_completed order_id=4 " in engine_log.read_text())
    recovered_records = ARTIFACTS / "engine-wal-recovered-records.log"
    with recovered_records.open("w") as output:
        consumer = subprocess.Popen([DOCKER,"exec",kafka,"/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server","localhost:29092","--topic","order-events","--from-beginning",
            "--max-messages","3","--timeout-ms","15000"],stdout=output,stderr=subprocess.DEVNULL)
        processes.append(consumer)
    consumer.wait(timeout=25)
    assert consumer.returncode == 0
    recovered_events = [json.loads(line) for line in recovered_records.read_text().splitlines() if line.startswith("{")]
    assert [event["event_type"] for event in recovered_events] == ["trade_executed","order_cancelled","trade_executed"]
    assert recovered_events[0]["event_id"] == events[0]["event_id"]
    assert len({event["event_id"] for event in recovered_events}) == 3
    assert recovered_events[-1]["buy_order_id"] == 4
    assert Decimal(str(recovered_events[-1]["buyer_quote_available"])) == 800
    evidence["checks"].append({"case":"durable_unpublished_trade_recovered_after_broker_failure","events":3,"buyer_quote_available":800})
    # 정상 종료도 큐와 저널을 닫아 성공으로 끝나는지 확인한다.
    engine.terminate()
    engine.wait(timeout=15)
    assert engine.returncode == 0, "graceful shutdown failed"
    evidence["checks"].append({"case":"sigterm_drains_journal_and_output","exit":0})
    print(json.dumps(evidence, indent=2), flush=True)
finally:
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    for container in [kafka, pg]:
        subprocess.run([DOCKER, "rm", "--force", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    journal_directory.cleanup()
    evidence["cleanup_completed"] = True
    (ARTIFACTS / "engine-output-process.json").write_text(json.dumps(evidence, indent=2))
