use sqlx::PgPool;
use anyhow::{Context, Result};

// 데이터베이스 연결 풀
// 역할: NestJS의 Database connection 같은 것
// Database connection pool for PostgreSQL
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    // 데이터베이스 연결 생성
    // Create database connection
    // db_url: PostgreSQL 연결 문자열 (예: "postgresql://root:1234@localhost/cex")
    pub async fn new(db_url: &str) -> Result<Self> {
        // PostgreSQL 연결 풀 생성
        // Create PostgreSQL connection pool
        let pool = PgPool::connect(db_url)
            .await
            .context("Failed to connect to database")?;

        Ok(Self { pool })
    }

    /// 재시작 복구는 엔진 저널만 읽는다. HTTP 어댑터의 디비 연결은 실제
    /// 요청 시 열어 플랫폼 디비 장애가 엔진 상태 재생 자체를 막지 않게 한다.
    pub fn lazy(db_url: &str) -> Result<Self> {
        Ok(Self { pool: sqlx::postgres::PgPoolOptions::new().connect_lazy(db_url)? })
    }

    // 연결 풀 반환
    // Get connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // 테이블 생성 (초기화) - 마이그레이션 실행
    // Create tables (initialization) - Run migrations
    // migrations/ 폴더의 모든 .sql 파일을 순서대로 실행
    pub async fn initialize(&self) -> Result<()> {
        // 마이그레이션 자동 실행
        // Run migrations from migrations/ folder
        sqlx::migrate!("./migrations")
            .run(self.pool())
            .await
            .context("Failed to run database migrations")?;
    
        println!("Database migrations completed successfully");
        Ok(())
    }

    /// 벤치마크/테스트용 No-op Database (실제 연결 없음)
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn noop(pool: PgPool) -> Self {
        Self { pool }
    }
}
