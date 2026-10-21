"""주문 안전성 회귀를 이 실행 소유 PostgreSQL 컨테이너에만 연결한다.

SQLx 0.6은 실패한 테스트 DB를 남기며 _sqlx_test_N 이름을 서버 전체에서 쓴다.
관리자 DB만 UUID로 바꿔도 이름이 충돌할 수 있어 서버 자체를 격리한다.
공유 5432를 쓰는 기존 통합 테스트는 실행하지 않고 전체 컴파일만 확인한다.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT.parent / ".local"
DOCKER = shutil.which("docker") or "/usr/local/bin/docker"
CARGO = shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo")
container = "cex-order-safety-" + uuid4().hex[:12]
results = {"container": container, "checks": []}


def docker(*args, **kwargs):
    return subprocess.run([DOCKER, *args], check=True, text=True, **kwargs)


try:
    ARTIFACTS.mkdir(exist_ok=True)
    docker("run", "--detach", "--rm", "--name", container,
           "--publish", "127.0.0.1::5432", "--tmpfs", "/var/lib/postgresql/data",
           "--env", "POSTGRES_USER=root", "--env", "POSTGRES_PASSWORD=1234",
           "--env", "POSTGRES_DB=cex_test", "postgres:16", stdout=subprocess.DEVNULL)
    deadline = time.monotonic() + 45
    while True:
        # 초기화 임시 서버는 Unix socket만 연다. TCP 준비 뒤에만 테스트를 진행한다.
        ready = subprocess.run([DOCKER, "exec", container, "pg_isready", "-h", "127.0.0.1",
                                "-U", "root", "-d", "cex_test"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if ready.returncode == 0:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("isolated PostgreSQL readiness timed out")
        time.sleep(0.2)
    published = subprocess.check_output([DOCKER, "port", container, "5432/tcp"], text=True).strip()
    port = published.rsplit(":", 1)[1]
    env = os.environ.copy()
    env.update(DATABASE_URL=f"postgres://root:1234@127.0.0.1:{port}/cex_test",
               KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:1")
    selected = [
        ("request", ["test", "--locked", "--lib", "request_decimals"]),
        ("repository", ["test", "--locked", "--lib", "exact_amount_tests", "--", "--test-threads=1"]),
        ("settlement", ["test", "--locked", "--lib", "settlement_"]),
        ("receipt", ["test", "--locked", "--lib", "order_receipt::tests", "--", "--test-threads=1"]),
        ("startup", ["test", "--locked", "--lib", "startup_tests", "--", "--test-threads=1"]),
        ("reservation", ["test", "--locked", "--lib", "reservation_tests"]),
        ("routes", ["test", "--locked", "--lib", "routes::tests"]),
        ("compile", ["test", "--locked", "--no-run"]),
        ("build", ["build", "--locked"]),
    ]
    for case, arguments in selected:
        log_path = ARTIFACTS / f"review-rust-{case}.log"
        with log_path.open("w") as log:
            completed = subprocess.run([CARGO, *arguments], cwd=ROOT, env=env,
                                       stdout=log, stderr=subprocess.STDOUT)
        results["checks"].append({"case": case, "exit": completed.returncode})
        print(f"{case}: exit {completed.returncode}", flush=True)
        if completed.returncode:
            raise RuntimeError(f"{case} failed; see {log_path}")
finally:
    # 이 실행이 만든 이름만 제거한다. 실패한 SQLx 하위 DB도 컨테이너와 함께 제거한다.
    removed = subprocess.run([DOCKER, "rm", "--force", container],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    results["container_removed"] = removed.returncode == 0
    (ARTIFACTS / "review-rust-results.json").write_text(json.dumps(results, indent=2))
