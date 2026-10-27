# =====================================================
# 거래 엔진 다단계 컨테이너 빌드
# =====================================================
# 1단계: 빌드
FROM rust:1.90-bookworm AS builder

WORKDIR /app

# 시스템 의존성 설치 (빌드용)
RUN apt-get update && apt-get install -y \
    pkg-config \
    cmake \
    libssl-dev \
    ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 소스 코드 복사 및 빌드
COPY . .

# Rust 의존성과 애플리케이션을 최적화 모드로 함께 빌드한다.
# GitHub Actions의 cache-to/cache-from을 사용하면 의존성 계층을 재사용할 수 있다.
RUN cargo build --release

# =====================================================
# 2단계: 실행 환경
FROM debian:bookworm-slim

WORKDIR /app

# 런타임 의존성만 설치
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin cex \
    && mkdir -p /app/data \
    && chown cex:cex /app/data

# 바이너리 복사
COPY --from=builder /app/target/release/cex-engine /usr/local/bin/cex-engine

# 환경 변수
ENV RUST_ENV=prod
ENV RUST_LOG=info

# 포트 노출
EXPOSE 3002

# 컨테이너 내부에서만 확인하며 외부 공개 여부는 Compose 네트워크가 결정한다.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3002/api/health || exit 1

# 실행 권한을 비특권 사용자로 낮춘다. 코어 고정과 실시간 우선순위 권한은 Compose가 제한적으로 부여한다.
USER cex
CMD ["cex-engine"]

