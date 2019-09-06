FROM rust:latest as builder

WORKDIR /usr/src/bioyino
COPY . .

RUN apt-get update && apt-get install capnproto -y

RUN cargo build --release

FROM debian:stretch

WORKDIR /

COPY --from=builder /usr/src/bioyino/target/release/bioyino /usr/bin/bioyino
COPY --from=builder /usr/src/bioyino/config.toml /etc/bioyino/bioyino.toml

CMD ["bioyino"]
