"""새 Java JAR와 Rust 바이너리를 임시 PostgreSQL에 연결하고 엔진 응답을 유실시킨다.

Docker postgres:16, Java 21, 두 프로젝트의 빌드 결과가 필요하다.
기존 서비스·DB·Kafka에 접근하지 않고 이 실행이 생성한 컨테이너·프로세스만 정리한다.
"""
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


ROOT = Path(__file__).resolve().parent.parent.parent
ARTIFACTS = ROOT / ".local"
DOCKER = shutil.which("docker") or "/usr/local/bin/docker"
container = "cex-receipt-check-" + uuid4().hex[:12]
processes = []
proxy = None
evidence = {"container": container, "checks": []}
secret = secrets.token_hex(32)


def sql(statement):
    return subprocess.check_output([DOCKER, "exec", container, "psql", "-U", "root", "-d", "cex",
                                    "-v", "ON_ERROR_STOP=1", "-At", "-c", statement], text=True).strip()


def port():
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        return reserve.getsockname()[1]


def http(url, body=None, headers=None):
    request = Request(url, data=None if body is None else json.dumps(body).encode(),
                      headers={"Content-Type": "application/json", **(headers or {})})
    try:
        response = urlopen(request, timeout=8)
    except HTTPError as error:
        response = error
    with response:
        return response.status, json.loads(response.read())


def start(command, log_name, env=None):
    path = ARTIFACTS / log_name
    with path.open("w") as log:
        process = subprocess.Popen(command, env=env, cwd=ROOT / "Cex-Engine", stdout=log, stderr=subprocess.STDOUT)
    processes.append(process)
    return process, path


def ready(process, url, log):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"시작 실패: {log}")
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"시작 시간 초과: {log}")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def jwt():
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()
    unsigned = encode({"alg": "HS256", "typ": "JWT"}) + "." + encode({"sub": "101", "exp": int(time.time()) + 600})
    signature = base64.urlsafe_b64encode(hmac.new(secret.encode(), unsigned.encode(), hashlib.sha256).digest()).rstrip(b"=").decode()
    return unsigned + "." + signature


try:
    ARTIFACTS.mkdir(exist_ok=True)
    subprocess.run([DOCKER, "run", "--detach", "--rm", "--name", container,
                    "-e", "POSTGRES_USER=root", "-e", "POSTGRES_PASSWORD=1234", "-e", "POSTGRES_DB=cex",
                    "-p", "127.0.0.1::5432", "--tmpfs", "/var/lib/postgresql/data", "postgres:16"],
                   check=True, stdout=subprocess.DEVNULL)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        probe = subprocess.run([DOCKER, "exec", container, "pg_isready", "-U", "root", "-d", "cex"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if probe.returncode == 0:
            try:
                sql("SELECT 1")
                break
            except subprocess.CalledProcessError:
                pass
        time.sleep(0.2)
    else:
        raise RuntimeError("임시 PostgreSQL 시작 시간 초과")
    database_address = subprocess.check_output([DOCKER, "port", container, "5432/tcp"], text=True).strip()
    engine_port = port()
    forwarded = []
    forwarded_event = threading.Event()

    class LoseResponse(BaseHTTPRequestHandler):
        lose_response = True

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            result = http(f"http://127.0.0.1:{engine_port}/api/cex/orders", body)
            forwarded.append(result)
            forwarded_event.set()
            # 실제 엔진의 영속 접수 응답을 받은 뒤 플랫폼에 전달하지 않는다.
            # 플랫폼은 읽기 제한 300ms로 결과 불명을 반환해야 한다.
            if self.lose_response:
                time.sleep(1)
                self.close_connection = True
            else:
                payload = json.dumps(result[1]).encode()
                self.send_response(result[0])
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        def log_message(self, *_):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), LoseResponse)
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    platform_port = port()
    java = Path(os.environ.get("JAVA_HOME", "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home")) / "bin/java"
    platform, platform_log = start([str(java), "-jar", str(ROOT / "Cex-Platform-Service/build/libs/cex-0.0.1-SNAPSHOT.jar"),
        f"--spring.datasource.url=jdbc:postgresql://{database_address}/cex", f"--server.port={platform_port}",
        "--server.address=127.0.0.1", f"--jwt.secret={secret}", "--bot.bot-count=0",
        "--bot.binance-ws-url=ws://127.0.0.1:1", "--spring.kafka.bootstrap-servers=127.0.0.1:1",
        "--spring.kafka.listener.auto-startup=false", "--spring.kafka.producer.properties.max.block.ms=500",
        f"--engine.http.url=http://127.0.0.1:{proxy.server_port}", "--engine.http.timeout=300",
        "--spring.jpa.hibernate.ddl-auto=update"], "durable-acceptance-platform.log")
    ready(platform, f"http://127.0.0.1:{platform_port}/swagger-ui/index.html", platform_log)
    sql((ROOT / "Cex-Engine/schema/order_receipts.sql").read_text())
    sql("INSERT INTO user_balances(user_id,mint_address,available,locked,created_at,updated_at) VALUES(101,'USDT',1000,0,now(),now())")
    with tempfile.TemporaryDirectory(prefix="cex-receipt-wal-") as wal:
        env = os.environ.copy()
        env.update({"DATABASE_HOST": database_address, "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
                    "ENGINE_HTTP_ADDRESS": f"127.0.0.1:{engine_port}", "ENGINE_DATA_DIR": wal})
        binary = str(ROOT / "Cex-Engine/target/debug/cex-engine")
        engine, engine_log = start([binary], "durable-acceptance-engine.log", env)
        ready(engine, f"http://127.0.0.1:{engine_port}/api/health", engine_log)
        key = str(uuid4())
        headers = {"Authorization": "Bearer " + jwt(), "Idempotency-Key": key}
        payload = {"orderType": "buy", "orderSide": "limit", "baseMint": "SOL", "price": "100", "amount": "2"}
        status, uncertain = http(f"http://127.0.0.1:{platform_port}/api/cex/orders", payload, headers)
        assert status == 202, (status, uncertain)
        assert forwarded_event.wait(5)
        assert forwarded[0][0] == 202 and forwarded[0][1]["receipt_status"] == "accepted", forwarded
        order_id = int(uncertain["orderId"])
        engine_payload = {"order_id": order_id, "user_id": 101, "order_type": "buy", "order_side": "limit",
                          "base_mint": "SOL", "price": "100.00", "amount": "2.000"}
        for _ in range(5):
            repeated, receipt = http(f"http://127.0.0.1:{engine_port}/api/cex/orders", engine_payload)
            assert repeated == 200 and receipt == forwarded[0][1], receipt
        repeated, order = http(f"http://127.0.0.1:{platform_port}/api/cex/orders", payload, headers)
        assert repeated == 200 and int(order["order"]["id"]) == order_id
        conflict, _ = http(f"http://127.0.0.1:{engine_port}/api/cex/orders", dict(engine_payload, amount="3"))
        assert conflict == 409
        deadline = time.monotonic() + 5
        while "output_completed" not in engine_log.read_text() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert engine_log.read_text().count("output_completed") == 1
        assert sql("SELECT count(*) FROM engine_order_receipts") == "1"
        assert sql("SELECT count(*) FROM order_submissions") == "1"
        assert sql("SELECT count(*) FROM orders") == "1"
        assert sql("SELECT available::text||'|'||locked::text FROM user_balances WHERE user_id=101") == "800.000000000|200.000000000"
        assert len(forwarded) == 1
        evidence["checks"].append({"case": "lost_response_with_real_java_and_rust", "platform_initial": status,
            "platform_replay": repeated, "engine_replays": 5, "engine_conflict": conflict,
            "matching_commands_processed": 1, "receipts": 1, "available": "800", "locked": "200"})
        # 두 번째 새 주문은 실제 Rust 접수 응답을 Java가 검증해 201을 반환하는지 확인한다.
        LoseResponse.lose_response = False
        success, second = http(f"http://127.0.0.1:{platform_port}/api/cex/orders", dict(payload, amount="1"),
                               {**headers, "Idempotency-Key": str(uuid4())})
        assert success == 201, (success, second)
        assert sql("SELECT count(*) FROM engine_order_receipts") == "2"
        deadline = time.monotonic() + 5
        while engine_log.read_text().count("output_completed") != 2 and time.monotonic() < deadline:
            time.sleep(0.1)
        assert engine_log.read_text().count("output_completed") == 2
        assert sql("SELECT available::text||'|'||locked::text FROM user_balances WHERE user_id=101") == "700.000000000|300.000000000"
        assert len(forwarded) == 2
        evidence["checks"].append({"case": "java_verifies_real_rust_receipt", "platform_http": success,
                                    "receipts": 2, "available": "700", "locked": "300"})
        stop(engine)
        restarted, restart_log = start([binary], "durable-acceptance-restart.log", env)
        ready(restarted, f"http://127.0.0.1:{engine_port}/api/health", restart_log)
        replayed, receipt = http(f"http://127.0.0.1:{engine_port}/api/cex/orders", engine_payload)
        assert replayed == 200 and receipt == forwarded[0][1]
        assert "output_completed" not in restart_log.read_text()
        evidence["checks"].append({"case": "restart_replays_wal_and_deduplicates", "health": 200,
                                    "duplicate_http": replayed, "duplicate_execution": False})
finally:
    for process in reversed(processes):
        stop(process)
    if proxy is not None:
        proxy.shutdown()
        proxy.server_close()
    subprocess.run([DOCKER, "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    evidence["container_removed"] = subprocess.run([DOCKER, "inspect", container], stdout=subprocess.DEVNULL,
                                                   stderr=subprocess.DEVNULL).returncode != 0
    (ARTIFACTS / "durable-acceptance-process.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
print(json.dumps(evidence, ensure_ascii=False, indent=2))
