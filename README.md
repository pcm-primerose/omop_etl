# OMOP ETL 

Note:
Update README file 091025

This project contains all code related to the pre-processing and ETL pipeline 
for getting data from eCRF systems to harmonized PRIME-ROSE variables and finally to the OMOP CDM.

### Getting Started
This project is in the early stages of development. It uses Poetry for dependency management and version control, and includes automated formatting and linting with black and flake8 via pre-commit hooks.

### Installation

#### Clone the Repository:


```bash
git clone git@github.com:pcm-primerose/omop_etl.git
cd omop_etl
```

### Running the Project
Use UV to run and manage project dependancies:

```bash
uv run <command>
```

For example:

```bash
uv run python3 pytest  # Run the tests
uv run pre-commit run --all-files  # Run pre-commit hooks
uv run python3 some_file.py # Run a specific file 
```

### Contributing
Follow these steps to contribute:

#### Create a New Branch:

```bash
git checkout -b your-branch-name
```

#### Make Your Changes:

Ensure your code works well and is easy to read (ideally write tests and make it clean, modular, extensible).
Always use type-hints! 

Run pre-commit hooks locally to validate:

```bash
poetry run pre-commit run --all-files
```
Submit a Pull Request:

Once your changes are complete, push your branch and open a pull request:

```bash
git push origin your-branch-name
```

### Notes
This project is in its early stages; additional documentation will be added as development progresses.
