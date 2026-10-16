//! Errors that cannot be fixed by replaying a network operation.
#[derive(Debug)]
pub struct NonRetryableError(pub String);
impl std::fmt::Display for NonRetryableError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}
impl std::error::Error for NonRetryableError {}
