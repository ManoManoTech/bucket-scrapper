# syntax=docker/dockerfile:1

FROM debian:trixie-slim

ARG TARGETARCH

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --user-group bucket-scrapper

WORKDIR /app

COPY --chown=bucket-scrapper:bucket-scrapper \
    --chmod=755 bucket-scrapper-${TARGETARCH} ./bucket-scrapper

# Runtime control client. Not the entrypoint — run it against a live scrapper
# (started with --control-socket) via `docker exec <container> ./bsctl …`.
COPY --chown=bucket-scrapper:bucket-scrapper \
    --chmod=755 bsctl-${TARGETARCH} ./bsctl

USER bucket-scrapper

ENTRYPOINT ["./bucket-scrapper"]
