use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::flag::Flag;
use simplelog::{ConfigBuilder, LevelFilter, SimpleLogger};

pub struct Platform {
    services: Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>,
    pub is_running: Flag,
}

impl Platform {
    pub fn new() -> Arc<Self> {
        Arc::new(Platform {
            services: Mutex::new(HashMap::new()),
            is_running: Flag::new(true),
        })
    }

    pub fn register<T>(&self, service: Arc<T>)
    where
        T: Any + Send + Sync,
    {
        let name = std::any::type_name::<T>().to_owned();
        self.services.lock().unwrap().insert(name, service);
    }

    pub fn find<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        let name = std::any::type_name::<T>();
        let services = self.services.lock().unwrap();
        services
            .get(name)
            .and_then(|entry| entry.clone().downcast::<T>().ok())
    }

    pub fn require<T>(&self) -> Arc<T>
    where
        T: Any + Send + Sync,
    {
        match self.find::<T>() {
            Some(service) => service,
            None => panic!(
                "A required component ({}) was not available in the platform registry!",
                std::any::type_name::<T>()
            ),
        }
    }

    pub fn init_logging(&self) {
        static DATE_FORMAT: &str = "[%Y-%m-%dT%H:%M:%S%.3f]";
        if let Err(error) = SimpleLogger::init(
            LevelFilter::Debug,
            ConfigBuilder::new()
                .set_time_format_str(DATE_FORMAT)
                .set_thread_level(LevelFilter::Trace)
                .set_target_level(LevelFilter::Error)
                .set_location_level(LevelFilter::Error)
                .build(),
        ) {
            panic!("Failed to initialize logging system: {}", error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::platform::Platform;

    struct Foo {
        value: i32,
    }

    struct Bar {
        value: i32,
    }

    #[test]
    fn registering_services_works() {
        let platform = Platform::new();
        platform.register(Arc::new(Foo { value: 42 }));
        platform.register(Arc::new(Bar { value: 32 }));

        let foo = platform.find::<Foo>();
        assert_eq!(foo.is_some(), true);
        assert_eq!(foo.unwrap().value, 42);

        let bar = platform.require::<Bar>();
        assert_eq!(bar.value, 32);

        assert_eq!(platform.find::<i32>(), None);
    }

    #[test]
    #[should_panic]
    fn requiring_an_unknown_service_panics() {
        let platform = Platform::new();
        platform.require::<i32>();
    }
}
