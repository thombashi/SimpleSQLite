PACKAGE := SimpleSQLite
AUTHOR := thombashi
BUILD_WORK_DIR := _work
DOCS_DIR := docs
BUILD_PKG_DIR := $(BUILD_WORK_DIR)/$(PACKAGE)


.PHONY: build-remote
build-remote:
	@rm -rf $(BUILD_WORK_DIR)/
	@mkdir -p $(BUILD_WORK_DIR)
	@cd $(BUILD_WORK_DIR) && \
		git clone https://github.com/$(AUTHOR)/$(PACKAGE).git && \
		cd $(PACKAGE) && \
		tox -e build
	ls -lh $(BUILD_PKG_DIR)/dist/*

.PHONY: build
build:
	@make clean
	@tox -e build
	ls -lh dist/*

.PHONY: check
check:
	python setup.py check
	codespell $(PACKAGE) docs sample test -q2 --check-filenames --ignore-words-list te -x "test/data/python - Wiktionary.html"
	travis lint
	pylama

.PHONY: clean
clean:
	@tox -e clean
	@rm -rf $(BUILD_WORK_DIR)

.PHONY: docs
docs:
	@tox -e docs

.PHONY: idocs
idocs:
	@pip install --upgrade .
	@make docs

.PHONY: fmt
fmt:
	@tox -e fmt

.PHONY: readme
readme:
	@cd $(DOCS_DIR) && tox -e readme

.PHONY: release
release:
	@cd $(BUILD_PKG_DIR) && tox -e release
	@make clean

.PHONY: setup
setup:
	@pip install --upgrade .[dev] tox
