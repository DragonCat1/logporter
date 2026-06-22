# Build image
FROM rust:1-alpine AS build
WORKDIR /logporter
RUN apk add --no-cache musl-dev
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      rustup target add aarch64-unknown-linux-musl && \
      cargo build --release --target aarch64-unknown-linux-musl && \
      cp target/aarch64-unknown-linux-musl/release/logporter /usr/local/bin/logporter; \
    else \
      cargo build --release && \
      cp target/release/logporter /usr/local/bin/logporter; \
    fi

# Final image
FROM alpine:3.20
COPY --from=build /usr/local/bin/logporter /usr/local/bin/
ENTRYPOINT ["logporter"]
