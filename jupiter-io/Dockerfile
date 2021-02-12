FROM rust:1.50-slim

RUN mkdir /jupiter
COPY /jupiter-io/target/release/jupiter-io /jupiter/jupiter-io
RUN chmod +x /jupiter/jupiter-io

VOLUME /jupiter/config
VOLUME /jupiter/repository

EXPOSE 2410/tcp
WORKDIR /jupiter
CMD ["/jupiter/jupiter-io"]
