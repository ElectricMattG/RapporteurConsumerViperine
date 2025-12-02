from setuptools import find_packages, setup

version = open("version").read().strip()

setup_args = {
    "name": 'Scrape linked web sources',
    "version": version,
    "description": 'Scrape linked web sources',
    "author": 'Matt Gerhold',
    "author_email": '',
    "python_requires": '>=3.11',  # Specify the minimum Python version required
    "packages": find_packages(),  # Automatically find all packages in the project
    "package_dir": {"": "."},  # Package root is the current directory
}

if __name__ == "__main__":
    setup(**setup_args)
