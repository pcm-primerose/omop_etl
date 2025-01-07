import os
import subprocess
import sys
import platform


def run_command(command, error_message):
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError:
        print(error_message)
        sys.exit(1)


def check_python_version(required_version):
    current_version = platform.python_version_tuple()
    if current_version < tuple(map(str, required_version)):
        print(f"Python {'.'.join(map(str, required_version))} or higher is required.")
        sys.exit(1)


def install_poetry():
    print("Poetry is not installed. Attempting to install...")
    try:
        subprocess.run(
            ["curl", "-sSL", "https://install.python-poetry.org", "|", "python3"],
            check=True,
        )
        print("Poetry installed successfully.")
    except subprocess.CalledProcessError:
        print("Failed to install Poetry. Please visit https://python-poetry.org/ for manual installation.")
        sys.exit(1)


def create_symlink(venv_path):
    # define symlink path in project root
    symlink_path = os.path.join(os.getcwd(), "activate")
    activate_script = os.path.join(venv_path, "bin", "activate")

    # check if symlink already exists
    if os.path.islink(symlink_path):
        os.remove(symlink_path)

    # create the symlink
    try:
        os.symlink(activate_script, symlink_path)
        print(f"Symlink created: {symlink_path}")
    except Exception as e:
        print(f"Failed to create symlink: {e}")


def main():
    print("Setting up the project...")

    # check Python version
    check_python_version((3, 10))

    # check if Poetry is installed
    try:
        subprocess.run(["poetry", "--version"], check=True)
    except FileNotFoundError:
        install_poetry()

    # install dependencies
    print("Installing dependencies...")
    run_command(["poetry", "install"], "Failed to install dependencies with Poetry.")

    # get virtual env path
    venv_path = subprocess.check_output(["poetry", "env", "info", "--path"]).decode().strip()

    # create symlink to virtual environment activation script
    create_symlink(venv_path)

    # activation instructions
    if os.name == "nt":
        activation_cmd = f"{venv_path}\\Scripts\\activate.bat"
    else:
        activation_cmd = f"source {venv_path}/bin/activate"

    print(f"Virtual environment created at: {venv_path}")
    print("\nTo activate the virtual environment manually, use:")
    print(activation_cmd)

    # set up pre-commit hooks
    print("\nSetting up pre-commit hooks...")
    run_command(["poetry", "run", "pre-commit", "install"], "Failed to set up pre-commit hooks.")

    # next steps
    print("\nNext Steps:")
    print("- Activate the virtual environment:")
    print("- Run `source activate` to enter the virtual environment, `poetry shell` or use your IDE.")
    print("- Run tests with `poetry run pytest` or enter the virtual env and run `pytest tests.py`.")
    print("- Start coding! See 'Contributing' section of README for how to contribute")


if __name__ == "__main__":
    main()
