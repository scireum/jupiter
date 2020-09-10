use std::sync::atomic::{AtomicBool, Ordering};

pub struct FlagListener {
    rx: tokio::sync::broadcast::Receiver<bool>
}

impl FlagListener {
    pub async fn expect(&mut self) -> Option<bool> {
        self.rx.recv().await.ok()
    }
}

pub struct Flag {
    state: AtomicBool,
    tx: tokio::sync::broadcast::Sender<bool>
}

impl Flag {
    pub fn new(initial_value: bool) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(1);
        Flag { state: AtomicBool::new(initial_value), tx }
    }

    pub fn listener(&self) -> FlagListener {
        FlagListener { rx: self.tx.subscribe() }
    }

    pub fn change(&self, new_value: bool) {
        self.state.store(new_value, Ordering::SeqCst);
        let _ = self.tx.send(new_value);
    }

    pub fn read(&self) -> bool {
        self.state.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use crate::flag::Flag;

    #[test]
    fn toggling_works() {
        let flag = Flag::new(true);
        assert_eq!(flag.read(), true);
        flag.change(false);
        assert_eq!(flag.read(), false);
    }

    #[test]
    fn toggling_works_across_threads() {
        tokio_test::block_on(async move {
            let outer_flag = Flag::new(true);
            let mut outer_listener = outer_flag.listener();

            let inner_flag = Flag::new(true);
            let mut inner_listener = inner_flag.listener();

            tokio::spawn(async move {
                outer_listener.expect().await;
                inner_flag.change(false);
                assert_eq!(inner_flag.read(), false);
            });

            outer_flag.change(false);
            inner_listener.expect().await;
            assert_eq!(outer_flag.read(), false);
        });
    }
}
