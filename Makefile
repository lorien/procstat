.PHONY: all clean upload

clean:
	find -name '*.pyc' -delete
	find -name '*.swp' -delete

release:
	git push; git push --tags; rm dist/*; python3 setup.py clean sdist; twine upload dist/*
