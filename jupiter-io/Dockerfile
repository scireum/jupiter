FROM rust:1.49-slim

RUN mkdir /jupiter
COPY target/release/jupiter-io /jupiter/jupiter-io
RUN chmod +x /jupiter/jupiter-io

VOLUME /jupiter/settings.yml
VOLUME /jupiter/repository

EXPOSE 2410/tcp
CMD ["/jupiter/jupiter-io"]
