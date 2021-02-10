FROM rust:1.49-slim

RUN mkdir /jupiter
COPY /jupiter-io/target/release/jupiter-io /jupiter/jupiter-io
RUN chmod +x /jupiter/jupiter-io

RUN touch /jupiter/settings.yml

VOLUME /jupiter/settings.yml
VOLUME /jupiter/repository

EXPOSE 2410/tcp
WORKDIR /jupiter
CMD ["/jupiter/jupiter-io"]
