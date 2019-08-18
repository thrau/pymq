import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pymq",
    version="0.1.0.dev1",
    author="Thomas Rausch",
    author_email="thomas@rauschig.org",
    description="A simple message-oriented middleware library built for Python IPC across machine boundaries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.dsg.tuwien.ac.at/mc2/pymq",
    packages=setuptools.find_packages(),
    setup_requires=['wheel'],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Networking",
        "Development Status :: 4 - Beta",
    ],
)
