.PHONY: all build check fmt clean

BINS := analyze_freshness insert_data main prepare_data tici
BIN_DIR := bin

all: build

build:
	cargo build --bins
	mkdir -p $(BIN_DIR)
	cp $(addprefix target/debug/,$(BINS)) $(BIN_DIR)/

check:
	cargo check --bins

fmt:
	cargo fmt

clean:
	cargo clean
	rm -rf $(BIN_DIR)
