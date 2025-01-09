# OMOP ETL 

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

#### Run the Setup Script:

To install all dependencies and set up the environment, run:

```bash
python3 setup.py
```

This script will:
- Install all dependencies via Poetry.
- Set up pre-commit hooks for flake8 and black.
- Create a symlink for easy virtual environment activation.

#### Activate the Virtual Environment:

Use the symlink to activate the environment:

```bash
source activate
```

Or just run through Poetry without having to enter the env (see `Running` section).
Alternatively get the fullpath to the env and activate it manually:


```bash
poetry env info -p
source path/to/env
```

### Running the Project
To run any scripts or commands within the project, you can use:

```bash
poetry run <command>
```

For example:

```bash
poetry run pytest  # Run the tests
poetry run pre-commit run --all-files  # Run pre-commit hooks
poetry run some_file.py # Run a specific file 
```

You can of course also enter the virtual env and run files in 
the terminal or use your IDE and the debugger. 

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
