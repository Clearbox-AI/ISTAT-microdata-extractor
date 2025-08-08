from setuptools import setup, find_packages

# Read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

# Parse the requirements.txt file to get a list of dependencies
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="istat-microdata-extractor",  
    version="0.0.1",  
    author="Dario Brunelli",
    author_email="dario@clearbox.ai",
    description="A class for extracting and processing ISTAT microdata files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Clearbox-AI/ISTAT-microdata-extractor",  
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*", "tests.*"]),
    classifiers=[
    ],
    python_requires='>3.9',
    install_requires=requirements,
    extras_require={
        "dev": [
            "check-manifest",
            "pytest>3.9",
        ],
        "test": [
            "coverage",
        ],
    },
    package_data={
    },
    entry_points={
    },
)
