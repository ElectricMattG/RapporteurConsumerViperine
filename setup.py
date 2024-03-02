from setuptools import find_packages, setup

version = open("version").read().strip()

setup_args = {
    "name": 'template service',
    "version": version,
    "description": 'a template fastapi kuberay service',
    "author": 'Matt Gerhold',
    "author_email": '',
    "python_requires": '>=3.10',  # Specify the minimum Python version required
    "packages": find_packages(["template*"]),  # Automatically find all packages in the project
}

if __name__ == "__main__":
    setup(**setup_args)
