# syntax=docker/dockerfile:1

FROM 304971447450.dkr.ecr.eu-west-3.amazonaws.com/public/library/debian:trixie-slim

RUN useradd --create-home --user-group log-consolidator-checker-rs

WORKDIR /app

ADD --chown log-consolidator-checker-rs:log-consolidator-checker-rs \
    --chmod 755 target/release/log-consolidator-checker-rust .

USER log-consolidator-checker-rs

ENTRYPOINT ["./log-consolidator-checker-rust"]
