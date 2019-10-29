"""Setup TORC_py."""
import os
import sys
from setuptools import setup, find_packages

def package_files(directory):
    """Find all non python files."""
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            if (
                '.py' not in filename and
                'dev' not in path and
                'tests' not in path
            ):
                paths.append(os.path.join(path, filename))
    return paths


SETUP_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SETUP_DIR)

PYTHON_VERSION = '{}.{}'.format(sys.version_info.major, sys.version_info.minor)

requirements_filepath = os.path.join(SETUP_DIR, 'requirements.txt')

DEPENDENCIES = []
if os.path.exists(requirements_filepath):
    for line in open(requirements_filepath):
        DEPENDENCIES.append(line.strip())

NAME = 'torcpy'

# write MANIFEST
with open(os.path.join(SETUP_DIR, 'MANIFEST.in'), 'w') as fp:
    for a_file in package_files('torcpy'):
        print(a_file)
        fp.write('include {}\n'.format(a_file))

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name=NAME,
    version='0.1',
    author='Panagiotis Chatzidoukas',
    author_email='hat@zurich.ibm.com',
    description='TORC Tasking library',
    licence='Eclipse Public License v1.0',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='http://github.com/ibm/torc_py/',
    packages=find_packages('.'),
    include_package_data=True,
    install_requires=DEPENDENCIES,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved",
        "Operating System :: OS Independent",
    ],
    zip_safe=False
)

