pub mod commands;
pub mod flag;
pub mod ping;
pub mod platform;
pub mod request;
pub mod response;
pub mod server;
pub mod watch;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
