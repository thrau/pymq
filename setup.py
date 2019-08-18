import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="symmetry-eventbus",
    version="0.0.1",
    author="Thomas Rausch",
    author_email="thomas@rauschig.org",
    description="A simple message-oriented middleware library built for IPC",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.dsg.tuwien.ac.at/mc2/symmetry-eventbus",
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
