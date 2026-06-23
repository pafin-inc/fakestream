.PHONY: build image run test fmt lint

build:
	cargo build --release

test:
	cargo test

fmt:
	cargo fmt

lint:
	cargo clippy --all-targets -- -D warnings

image:
	docker build -t fakestream .

run: build
	./target/release/fakestream
