# mcap.dev documentation site

To generate the docs site, run from the root of the repo:

```
PIPENV_PIPFILE=docs/Pipfile pipenv install
PIPENV_PIPFILE=docs/Pipfile pipenv run mkdocs build
```

To run the development server:

```
PIPENV_PIPFILE=docs/Pipfile pipenv install
PIPENV_PIPFILE=docs/Pipfile pipenv run mkdocs serve
```
