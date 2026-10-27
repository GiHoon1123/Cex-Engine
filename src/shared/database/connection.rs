use anyhow::{Context, Result};
use sqlx::PgPool;

// 데이터베이스 연결 풀
// 타입스크립트 서버의 연결 풀처럼 복제 가능한 핸들만 보관한다.
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    // 전달받은 전체 연결 문자열로 즉시 연결한다. 자격 증명 기본값은 두지 않는다.
    pub async fn new(db_url: &str) -> Result<Self> {
        // 최초 연결까지 확인하므로 초기화 도구와 통합 검사에서 사용한다.
        let pool = PgPool::connect(db_url)
            .await
            .context("데이터베이스에 연결하지 못했습니다")?;

        Ok(Self { pool })
    }

    /// 재시작 복구는 엔진 저널만 읽는다. HTTP 어댑터의 디비 연결은 실제
    /// 요청 시 열어 플랫폼 디비 장애가 엔진 상태 재생 자체를 막지 않게 한다.
    pub fn lazy(db_url: &str) -> Result<Self> {
        Ok(Self { pool: sqlx::postgres::PgPoolOptions::new().connect_lazy(db_url)? })
    }

    // 저장소가 같은 풀을 재사용하도록 참조를 반환한다.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // 초기 설치 도구에서 마이그레이션 파일을 순서대로 실행한다.
    pub async fn initialize(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(self.pool())
            .await
            .context("데이터베이스 마이그레이션에 실패했습니다")?;
    
        println!("데이터베이스 마이그레이션을 완료했습니다");
        Ok(())
    }

    /// 벤치마크와 단위 테스트가 실제 연결 없이 주입하는 디비 핸들이다.
    #[cfg(any(test, feature = "bench_mode"))]
    pub fn noop(pool: PgPool) -> Self {
        Self { pool }
    }
}
