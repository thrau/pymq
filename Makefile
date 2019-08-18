.PHONY: dist-clean

VENV_BIN = python3.7 -m venv

venv: .venv/bin/activate

.venv/bin/activate: requirements.txt
	test -d .venv || $(VENV_BIN) .venv
	. .venv/bin/activate; pip install -Ur requirements.txt
	touch .venv/bin/activate

test: venv
	. .venv/bin/activate; python -m unittest discover -s tests/ -v

dist: venv
	. .venv/bin/activate; python setup.py sdist bdist_wheel

install: venv
	. .venv/bin/activate; python setup.py install

dist-clean:
	rm -rf dist/
	rm -rf *.egg-info/
