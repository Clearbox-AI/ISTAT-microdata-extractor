import shutil
from pathlib import Path
from setuptools import setup, find_packages
# from Cython.Distutils import build_ext

# Read the content of the README.md for the long_description metadata
with open("README.md", "r") as readme:
    long_description = readme.read()

# Parse the requirements.txt file to get a list of dependencies
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

# class CustomBuild(build_ext):
#     """
#     Custom build class that inherits from Cython's build_ext.

#     This class is created to override the default build behavior.
#     Specifically, it ensures certain non-Cython files are copied
#     over to the build output directory after the Cythonization process.
#     """

#     def run(self):
#         """Override the run method to copy specific files after build."""
#         # Run the original run method
#         build_ext.run(self)

#         build_dir = Path(self.build_lib)
#         root_dir = Path(__file__).parent
#         target_dir = build_dir if not self.inplace else root_dir

#         # List of files to copy after the build process
#         files_to_copy = [
#             "microdata_extractor/__init__.py",
#             "microdata_extractor/microdata_extractor.py",
#         ]

#         for file in files_to_copy:
#             self.copy_file(Path(file), root_dir, target_dir)

    # def copy_file(self, path, source_dir, destination_dir):
    #     """
    #     Utility method to copy files from source to destination.

    #     Parameters
    #     ----------
    #     path : Path
    #         Path of the file to be copied.
    #     source_dir : Path
    #         Directory where the source file resides.
    #     destination_dir : Path
    #         Directory where the file should be copied to.

    #     """
    #     src_file = source_dir / path
    #     dest_file = destination_dir / path
    #     dest_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
    #     shutil.copyfile(str(src_file), str(dest_file))

# Main setup configuration
setup(
    # Metadata about the package
    name="microdata-extractor",
    version="0.0.1",
    author="Clearbox AI",
    author_email="info@clearbox.ai",
    description="A tool for navigating and processing the Abitudini della Vita Quotiiana ISTAT microdata",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Clearbox-AI/ISTAT-microdata-extractor",
    install_requires=requirements,
    python_requires=">=3.9.0",
    
    # Override the build command with our custom class
    # cmdclass=dict(build_ext=CustomBuild),

    # List of packages included in the distribution
    packages=find_packages(),  # Include all packages in the distribution
    include_package_data=True,
)