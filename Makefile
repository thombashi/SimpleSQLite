PYTHON := python3

PACKAGE := SimpleSQLite
AUTHOR := thombashi
BUILD_WORK_DIR := _work
DOCS_DIR := docs
PKG_BUILD_DIR := $(BUILD_WORK_DIR)/$(PACKAGE)


.PHONY: build-remote
build-remote: clean
	@mkdir -p $(BUILD_WORK_DIR)
	@cd $(BUILD_WORK_DIR) && \
		git clone https://github.com/$(AUTHOR)/$(PACKAGE).git --depth 1 && \
		cd $(PACKAGE) && \
		$(PYTHON) -m tox -e build
	ls -lh $(PKG_BUILD_DIR)/dist/*

.PHONY: build
build: clean
	@$(PYTHON) -m tox -e build
	ls -lh dist/*

.PHONY: check
check:
	@$(PYTHON) -m tox -e lint

.PHONY: clean
clean:
	@rm -rf $(BUILD_WORK_DIR)
	@$(PYTHON) -m tox -e clean

.PHONY: docs
docs:
	@$(PYTHON) -m tox -e docs

.PHONY: run-sample
run-sample:
	find sample/ -name "*.py" | \grep -v sample_create_table_from_gs.py | xargs -I{} $(PYTHON) {}

.PHONY: idocs
idocs:
	@$(PYTHON) -m pip install -q --disable-pip-version-check --upgrade .
	@$(MAKE) docs

.PHONY: fmt
fmt:
	@$(PYTHON) -m tox -e fmt

.PHONY: readme
readme:
	@$(PYTHON) -m tox -e readme

.PHONY: release
release:
	cd $(PKG_BUILD_DIR) && $(PYTHON) setup.py release --sign --verbose
	$(MAKE) clean

.PHONY: setup-ci
setup-ci:
	$(PYTHON) -m pip install -q --disable-pip-version-check --upgrade pip
	$(PYTHON) -m pip install -q --disable-pip-version-check --upgrade tox

.PHONY: setup-dev
setup-dev: setup-ci
	$(PYTHON) -m pip install -q --disable-pip-version-check --upgrade -e .[test]
	-$(PYTHON) -m pip check
