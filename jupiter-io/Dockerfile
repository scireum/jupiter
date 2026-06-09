FROM rust:1.92-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY jupiter-rs ./jupiter-rs
COPY jupiter-io ./jupiter-io

WORKDIR /src/jupiter-io
RUN cargo build --release

FROM rust:1.92-slim

RUN mkdir /jupiter
COPY --from=builder /src/jupiter-io/target/release/jupiter-io /jupiter/jupiter-io
RUN chmod +x /jupiter/jupiter-io

VOLUME /jupiter/config
VOLUME /jupiter/repository

EXPOSE 2410/tcp
WORKDIR /jupiter
CMD ["/jupiter/jupiter-io"]
