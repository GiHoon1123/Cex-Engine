use crate::shared::database::{Database, UserRepository, RefreshTokenRepository};
use crate::domains::auth::models::{User, SignupRequest, SigninRequest, RefreshTokenCreate};
use crate::domains::auth::services::JwtService;
use crate::shared::errors::AuthError;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::{rand_core::OsRng, SaltString};
use chrono::{Duration, Utc};

// 인증 서비스
// 역할: NestJS의 Service 같은 것
// AuthService: handles authentication business logic
#[derive(Clone)]
pub struct AuthService {
    db: Database,
    jwt_service: JwtService,
}

impl AuthService {
    // 생성자
    pub fn new(db: Database) -> Self {
        // JWT Service는 AppState에서 주입받아야 하는데, 순환 참조를 피하기 위해
        // 여기서는 임시로 생성 (실제로는 AppState에서 주입받아야 함)
        // JWT_SECRET 하드코딩 (배포 서버용)
        let jwt_secret = "cex-engine-jwt-secret-key-2024-production".to_string();
        let jwt_service = JwtService::new(jwt_secret);
        
        Self { 
            db,
            jwt_service,
        }
    }

    // JWT Service 주입을 위한 생성자
    pub fn with_jwt_service(db: Database, jwt_service: JwtService) -> Self {
        Self {
            db,
            jwt_service,
        }
    }

    // 회원가입 (비즈니스 로직)
    pub async fn signup(
        &self,
        request: SignupRequest,
    ) -> Result<User, AuthError> {
        // Repository 생성 (Service 내부에서)
        let user_repo = UserRepository::new(self.db.pool().clone());

        // 1. 이메일 중복 확인
        let existing_user = user_repo
            .get_user_by_email(&request.email)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to check email existence: {}", e)))?;
        
        if existing_user.is_some() {
            return Err(AuthError::EmailAlreadyExists { email: request.email });
        }

        // 2. 비밀번호 해싱
        let password_hash = Self::hash_password(&request.password)?;

        // 3. 사용자 생성
        let user = user_repo
            .create_user(
                &request.email,
                &password_hash,
                request.username.as_deref(),
            )
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to create user: {}", e)))?;

        Ok(user)
    }

    // 로그인 (비즈니스 로직)
    // Returns: (User, refresh_token)
    pub async fn signin(
        &self,
        request: SigninRequest,
    ) -> Result<(User, String), AuthError> {
        // Repository 생성 (Service 내부에서)
        let user_repo = UserRepository::new(self.db.pool().clone());

        // 1. 이메일로 사용자 조회
        let user = user_repo
            .get_user_by_email(&request.email)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to fetch user: {}", e)))?;

        let user = match user {
            Some(u) => u,
            None => return Err(AuthError::InvalidCredentials),
        };

        // 2. 비밀번호 검증
        Self::verify_password(&request.password, &user.password_hash)?;

        // 3. 이전 Refresh Token들 무효화 (새 로그인 시 기존 세션 종료)
        let refresh_token_repo = RefreshTokenRepository::new(self.db.pool().clone());
        refresh_token_repo
            .revoke_all_for_user(user.id)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to revoke previous tokens: {}", e)))?;

        // 4. 새 Refresh Token 생성 및 저장
        let refresh_token = self.create_refresh_token(user.id).await?;

        Ok((user, refresh_token))
    }

    /// Refresh Token 생성 및 DB 저장
    /// Create and store refresh token
    pub async fn create_refresh_token(&self, user_id: u64) -> Result<String, AuthError> {
        let refresh_token_repo = RefreshTokenRepository::new(self.db.pool().clone());

        // 1. Refresh Token 생성 (랜덤 문자열)
        let refresh_token = self.jwt_service.generate_refresh_token();
        
        // 2. Token 해싱 (DB 저장용)
        let token_hash = self.jwt_service.hash_refresh_token(&refresh_token);

        // 3. 만료 시간 설정 (7일)
        let expires_at = Utc::now() + Duration::days(7);

        // 4. DB에 저장
        let _ = refresh_token_repo
            .create(RefreshTokenCreate {
                user_id,  // u64 유지 (repository에서 i64로 변환)
                token_hash,
                expires_at,
            })
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to create refresh token: {}", e)))?;

        // 5. 원본 토큰 반환 (해싱 전)
        Ok(refresh_token)
    }

    /// Refresh Token 검증 및 새 Access Token 발급
    /// Verify refresh token and issue new access token
    pub async fn refresh_access_token(&self, refresh_token: &str) -> Result<(String, String), AuthError> {
        let refresh_token_repo = RefreshTokenRepository::new(self.db.pool().clone());

        // 1. Refresh Token 해싱
        let token_hash = self.jwt_service.hash_refresh_token(refresh_token);

        // 2. DB에서 조회
        let stored_token = refresh_token_repo
            .find_by_token_hash(&token_hash)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to find refresh token: {}", e)))?;

        let stored_token = match stored_token {
            Some(t) => t,
            None => return Err(AuthError::InvalidToken),
        };

        // 3. 토큰 유효성 검증
        if stored_token.revoked {
            return Err(AuthError::InvalidToken); // 무효화된 토큰
        }

        if stored_token.expires_at < Utc::now() {
            return Err(AuthError::InvalidToken); // 만료된 토큰
        }

        // 4. 사용자 정보 조회
        let user_repo = UserRepository::new(self.db.pool().clone());
        let user = user_repo
            .get_user_by_id(stored_token.user_id as u64)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to fetch user: {}", e)))?
            .ok_or(AuthError::InvalidToken)?;

        // 5. 새 Access Token 생성
        let access_token = self.jwt_service.generate_access_token(user.id, user.email.clone())?;

        // 6. 기존 Refresh Token 무효화 (새 토큰 생성 전에 먼저 무효화)
        refresh_token_repo
            .revoke(&token_hash)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to revoke old refresh token: {}", e)))?;

        // 7. 새 Refresh Token 생성 (Rotation - 보안 강화)
        // 기존 토큰을 먼저 무효화한 후 새 토큰 생성하여, 새 토큰이 무효화되는 것을 방지
        let new_refresh_token = self.create_refresh_token(user.id).await?;

        Ok((access_token, new_refresh_token))
    }

    /// 로그아웃 - Refresh Token 무효화
    /// Logout - Revoke refresh token
    pub async fn logout(&self, refresh_token: &str) -> Result<(), AuthError> {
        let refresh_token_repo = RefreshTokenRepository::new(self.db.pool().clone());

        // Refresh Token 해싱
        let token_hash = self.jwt_service.hash_refresh_token(refresh_token);

        // 토큰 무효화
        refresh_token_repo
            .revoke(&token_hash)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to revoke refresh token: {}", e)))?;

        Ok(())
    }

    /// 사용자의 모든 Refresh Token 무효화 (모든 기기에서 로그아웃)
    /// Revoke all refresh tokens for user (logout from all devices)
    pub async fn logout_all_devices(&self, user_id: u64) -> Result<(), AuthError> {
        let refresh_token_repo = RefreshTokenRepository::new(self.db.pool().clone());

        refresh_token_repo
            .revoke_all_for_user(user_id)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to revoke all refresh tokens: {}", e)))?;

        Ok(())
    }

    fn hash_password(password: &str) -> Result<String, AuthError> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();

        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| AuthError::PasswordHashingFailed(format!("Failed to hash password: {}", e)))?
            .to_string();

        Ok(password_hash)
    }

    fn verify_password(password: &str, password_hash: &str) -> Result<(), AuthError> {
        let parsed_hash = PasswordHash::new(password_hash)
            .map_err(|e| AuthError::PasswordVerificationFailed(format!("Invalid password hash: {}", e)))?;

        let argon2 = Argon2::default();
        
        argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| AuthError::InvalidCredentials)?;

        Ok(())
    }

    pub async fn get_user_info(&self, user_id:u64) -> Result<User, AuthError> {
        let user_repo = UserRepository::new(self.db.pool().clone());

        let user = user_repo
            .get_user_by_id(user_id)
            .await
            .map_err(|e| AuthError::DatabaseError(format!("Failed to fetch user: {}", e)))?
            .ok_or(AuthError::InvalidToken)?; // 사용자가 없으면 InvalidToken 에러
            
        Ok(user)
    }
}