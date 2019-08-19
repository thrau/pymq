VENV_BIN = python3.7 -m venv
VENV_DIR ?= .venv

VENV_ACTIVATE = . $(VENV_DIR)/bin/activate


venv: $(VENV_DIR)/bin/activate

$(VENV_DIR)/bin/activate: requirements.txt requirements-dev.txt
	test -d .venv || $(VENV_BIN) .venv
	$(VENV_ACTIVATE); pip install -Ur requirements.txt
	$(VENV_ACTIVATE); pip install -Ur requirements-dev.txt
	touch $(VENV_DIR)/bin/activate

clean:
	rm -rf build/
	rm -rf .eggs/
	rm -rf *.egg-info/

build: venv
	$(VENV_ACTIVATE); python setup.py build

test: venv
	$(VENV_ACTIVATE); python setup.py test

dist: venv
	$(VENV_ACTIVATE); python setup.py sdist bdist_wheel

install: venv
	$(VENV_ACTIVATE); python setup.py install

deploy: venv test dist
	$(VENV_ACTIVATE); pip install --upgrade twine; twine upload dist/*

clean-dist: clean
	rm -rf dist/

.PHONY: clean clean-dist
