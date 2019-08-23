import os

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements-dev.txt", "r") as fh:
    tests_require = [line for line in fh.read().split(os.linesep) if line]

with open("requirements.txt", "r") as fh:
    install_requires = [line for line in fh.read().split(os.linesep) if line]

setuptools.setup(
    name="pymq",
    version="0.3.0.dev1",
    author="Thomas Rausch",
    author_email="thomas@rauschig.org",
    description="A simple message-oriented middleware library built for Python IPC across machine boundaries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thrau/pymq",
    download_url="https://pypi.org/project/pymq/",
    packages=setuptools.find_packages(),
    setup_requires=['wheel'],
    test_suite="tests",
    tests_require=tests_require,
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Networking",
        "Development Status :: 4 - Beta",
    ],
)
