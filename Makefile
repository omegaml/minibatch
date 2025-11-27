.PHONY: dist

install:
	pip install -U -e .[all,dev] --ignore-installed

dist:
	mkdir -p dist
	rm -rf dist/*
	python -m build --sdist --wheel

pypi-test: dist
	twine check dist/*
	twine upload --repository testpypi-omegaml dist/*
	@echo now test your package!

pypi-prod: lint test dist
	twine check dist/*
	twine upload --repository pypi-omegaml dist/*

pypitest:
    # run this in a new conda env
	pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ minibatch[all]

clean:
	rm -rf ./dist
	rm -rf ./build

lint:
	flake8 --exclude "build/*,dist/*" && echo CONGRATULATIONS all is OK

test:
	docker compose up -d
	pytest -s -v

bumppatch:
	bumpversion patch

bumpminor:
	bumpversion minor

bumpbuild:
	bumpversion build
