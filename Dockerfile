# Build the single static-ish binary (the web/ frontend is embedded at compile
# time via rust-embed, so the runtime image carries no extra files).
FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/void /usr/local/bin/void
ENV HOST=0.0.0.0 \
    PORT=8080
EXPOSE 8080
CMD ["void"]
