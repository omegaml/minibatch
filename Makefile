.PHONY: dist

dist:
	mkdir -p dist
	rm -rf dist/*
	python setup.py sdist bdist_wheel

pypi: dist
	twine upload --repository testpypi dist/*
	@echo now test your package!

pypi-prod: dist
	twine upload --repository pypi dist/*


