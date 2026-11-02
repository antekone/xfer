.DEFAULT_GOAL := all

CARGO ?= cargo
GROFF ?= groff
GZIP ?= gzip
MAN_DIR := target/man

.PHONY: all release man

all: release man

release:
	$(CARGO) build --release --bin xfer

man: $(MAN_DIR)/xfer.1.gz $(MAN_DIR)/xfer.txt

$(MAN_DIR):
	mkdir -p $@

$(MAN_DIR)/xfer.1.gz: docs/xfer.1 | $(MAN_DIR)
	$(GZIP) -n -c $< > $@.tmp
	mv $@.tmp $@

$(MAN_DIR)/xfer.txt: docs/xfer.1 | $(MAN_DIR)
	$(GROFF) -Wall -Tutf8 -man $< > $@.tmp
	mv $@.tmp $@
