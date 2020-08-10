import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="datalite", # Replace with your own username
    version="0.5.1",
    author="Ege Ozkan",
    author_email="egeemirozkan24@gmail.com",
    description="A small package that binds dataclasses to an sqlite database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ambertide/datalite",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)