use std::time::Instant;

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Benchmark, Criterion};
use tokio::time::Duration;

use jupiter::commands::CommandDictionary;
use jupiter::ping;
use jupiter::platform::Platform;
use jupiter::request::Request;
use jupiter::response::Response;
use jupiter::server::Server;
use jupiter::watch::{Average, Watch};

fn watch_benchmarks(c: &mut Criterion) {
    c.bench_function("applying a watch", |b| {
        b.iter(|| {
            let watch = Watch::start();
            black_box(3 + 4);
            black_box(watch.micros());
        })
    });

    c.bench_function("recording average duration", |b| {
        let avg = Average::new();
        b.iter(|| {
            let watch = Watch::start();
            black_box(3 + 4);
            avg.add(watch.micros());
        });

        black_box(avg);
    });
}

fn request_benchmarks(c: &mut Criterion) {
    c.bench_function("parse simple request", |b| {
        b.iter(|| {
            let request = &mut BytesMut::from("*2\r\n$10\r\ntest.hello\r\n");
            Request::parse(black_box(request)).unwrap();
        })
    });

    c.bench_function("parse partial request", |b| {
        b.iter(|| {
            let request = &mut BytesMut::from("*2\r\n$10\r\ntes");
            Request::parse(black_box(request)).unwrap();
        })
    });

    c.bench_function("parse short partial request", |b| {
        b.iter(|| {
            let request = &mut BytesMut::from("*2\r");
            Request::parse(black_box(request)).unwrap();
        })
    });
}

fn response_benchmarks(c: &mut Criterion) {
    c.bench_function("simple OK", |b| {
        b.iter(|| {
            let mut response = Response::new();
            response.ok().unwrap();
            let bytes = response.complete().unwrap();
            black_box(bytes);
        })
    });

    c.bench_function("array response", |b| {
        b.iter(|| {
            let mut response = Response::new();
            response.array(3).unwrap();
            response.number(10).unwrap();
            response.simple("Hello World").unwrap();
            response.bulk("Hello World").unwrap();
            let bytes = response.complete().unwrap();
            black_box(bytes);
        })
    });

    c.bench_function("response with dynamic allocation", |b| {
        b.iter(|| {
            let mut response = Response::new();
            response.bulk("X".repeat(16_000).as_str()).unwrap();
            let bytes = response.complete().unwrap();
            black_box(bytes);
        })
    });
}

fn commands_benchmarks(c: &mut Criterion) {
    c.bench_function("execute unknown command", |b| {
        b.iter_custom(|iters| {
            tokio_test::block_on(async {
                let platform = Platform::new();
                let commands = CommandDictionary::install(&platform);
                ping::install(&platform);
                let mut dispatcher = commands.dispatcher();

                let start = Instant::now();
                for _i in 0..iters {
                    let request_string = &mut BytesMut::from("*1\r\n$4\r\nTEST\r\n");
                    let request = Request::parse(request_string).unwrap().unwrap();
                    dispatcher.invoke(black_box(request)).await.unwrap();
                }
                start.elapsed()
            })
        })
    });

    c.bench_function("execute simple command", |b| {
        b.iter_custom(|iters| {
            tokio_test::block_on(async {
                let platform = Platform::new();
                let commands = CommandDictionary::install(&platform);
                ping::install(&platform);
                let mut dispatcher = commands.dispatcher();

                let start = Instant::now();
                for _i in 0..iters {
                    let request_string = &mut BytesMut::from("*1\r\n$4\r\nPING\r\n");
                    let request = Request::parse(request_string).unwrap().unwrap();
                    dispatcher.invoke(black_box(request)).await.unwrap();
                }
                start.elapsed()
            })
        })
    });
}

fn server_benchmarks(c: &mut Criterion) {
    c.bench(
        "server_benchmarks",
        Benchmark::new("invoke server with PING", |b| {
            b.iter_custom(|iters| {
                tokio_test::block_on(async {
                    let platform = Platform::new();
                    CommandDictionary::install(&platform);
                    ping::install(&platform);
                    Server::fork_and_await(&Server::install(&platform)).await;

                    tokio::task::spawn_blocking(move || {
                        let client = redis::Client::open("redis://127.0.0.1:2410/").unwrap();
                        let mut con = client.get_connection().unwrap();

                        let start = Instant::now();
                        for _i in 0..iters {
                            let _: String = redis::cmd("PING").query(&mut con).unwrap();
                        }
                        platform.is_running.change(false);
                        start.elapsed()
                    })
                    .await
                    .unwrap()
                })
            })
        })
        .measurement_time(Duration::from_secs(15))
        .sample_size(25),
    );
}

criterion_group!(
    benches,
    watch_benchmarks,
    request_benchmarks,
    response_benchmarks,
    commands_benchmarks,
    server_benchmarks
);
criterion_main!(benches);
