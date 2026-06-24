# syntax=docker/dockerfile:1

FROM rust:1.92-slim AS builder

RUN rm -f /etc/apt/apt.conf.d/docker-clean && \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends libssl-dev pkg-config

WORKDIR /src
COPY jupiter-rs ./jupiter-rs
COPY jupiter-io ./jupiter-io

WORKDIR /src/jupiter-io
ARG TARGETARCH
RUN --mount=type=cache,id=jupiter-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=jupiter-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=jupiter-target-${TARGETARCH},target=/src/jupiter-io/target,sharing=locked \
    cargo build --release && \
    cp /src/jupiter-io/target/release/jupiter-io /usr/local/bin/jupiter-io

FROM rust:1.92-slim

RUN mkdir /jupiter
COPY --from=builder /usr/local/bin/jupiter-io /jupiter/jupiter-io
RUN chmod +x /jupiter/jupiter-io

VOLUME /jupiter/config
VOLUME /jupiter/repository

EXPOSE 2410/tcp
WORKDIR /jupiter
CMD ["/jupiter/jupiter-io"]
