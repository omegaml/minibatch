.PHONY: dist

dist:
	mkdir -p dist
	rm -rf dist/*
	python setup.py sdist bdist_wheel

pypi: dist
	twine check dist/*
	twine upload --repository testpypi dist/*
	@echo now test your package!

pypi-prod: lint test dist
	twine check dist/*
	twine upload --repository pypi dist/*

clean:
	rm -rf ./dist
	rm -rf ./build

lint:
	flake8 && echo CONGRATULATIONS all is OK

test:
	docker-compose up -d
	nosetests -s -v

bumppatch:
	bumpversion patch

bumpminor:
	bumpversion minor

bumpbuild:
	bumpversion build
