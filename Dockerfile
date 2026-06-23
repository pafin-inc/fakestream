FROM rust:1.90-slim AS build
ARG TARGETARCH
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-musl ;; \
      arm64) target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    echo "$target" >/tmp/rust-target; \
    rustup target add "$target"
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN set -eux; \
    target="$(cat /tmp/rust-target)"; \
    cargo build --release --target "$target"; \
    cp "target/$target/release/fakestream" /fakestream

FROM scratch
COPY --from=build /fakestream /fakestream
EXPOSE 4567
USER 65534:65534
ENTRYPOINT ["/fakestream"]
