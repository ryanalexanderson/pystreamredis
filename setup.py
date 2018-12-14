import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pystreamredis",
    version="0.0.2",
    author="Ryan Anderson",
    author_email="ryan.anderson@maerospace.com",
    description="Iterator to read Redis streams",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ryanalexanderson/pystreamredis",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)