from setuptools import setup, find_packages

setup(
    name="binarykv",
    version="0.1.0",
    description="A Python package implementing a higly efficient, random or sequential access binary key/value store.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="bn999",
    author_email="your.email@example.com",
    url="https://github.com/bn999/binarykv",
    packages=find_packages(),
    python_requires=">=3.6",  # Minimum Python version
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
