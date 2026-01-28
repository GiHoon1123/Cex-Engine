// Shared module
pub mod middleware;
pub mod clients;
pub mod database;
pub mod errors;
pub mod services;
pub mod utils;
pub mod handlers;
pub mod kafka;

pub use middleware::*;
pub use clients::*;
pub use database::*;
pub use errors::*;
pub use services::*;
pub use utils::*;
pub use handlers::*;
pub use kafka::*;

