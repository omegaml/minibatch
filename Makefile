pypi:
	mkdir -p dist
	rm -rf dist/*
	python setup.py sdist bdist_wheel
	twine upload --repository testpypi dist/*
	@echo now test your package!

pypi-prod:
	twine upload --repository pypi dist/*


