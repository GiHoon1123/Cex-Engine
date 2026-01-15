# =====================================================
# Multi-stage Dockerfile for CEX Backend
# =====================================================
# Stage 1: Build
FROM rust:latest as builder

WORKDIR /app

# 시스템 의존성 설치 (빌드용)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 소스 코드 복사 및 빌드
COPY . .

# Rust 빌드 (의존성 + 애플리케이션)
# GitHub Actions의 cache-to/cache-from으로 의존성 캐싱 처리
# Docker 레이어 캐싱으로 의존성 빌드 결과 재사용
RUN cargo build --release

# =====================================================
# Stage 2: Runtime
FROM debian:bookworm-slim

WORKDIR /app

# 런타임 의존성만 설치
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# 바이너리 복사
COPY --from=builder /app/target/release/cex-engine /usr/local/bin/cex-engine

# 환경 변수
ENV RUST_ENV=prod
ENV RUST_LOG=info

# 포트 노출
EXPOSE 3002

# 헬스체크 (선택적)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3002/api/health || exit 1

# 실행
# 주의: 코어 고정 및 실시간 스케줄링을 위해
# docker-compose.yml에서 cpuset_cpus와 cap_add 설정 필요
CMD ["cex-engine"]

