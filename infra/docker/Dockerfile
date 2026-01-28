# syntax=docker/dockerfile:1

FROM 304971447450.dkr.ecr.eu-west-3.amazonaws.com/public/library/debian:trixie-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --user-group bucket-scrapper

WORKDIR /app

ADD --chown=bucket-scrapper:bucket-scrapper \
    --chmod=755 target/release/bucket-scrapper .

USER bucket-scrapper

ENTRYPOINT ["./bucket-scrapper"]
