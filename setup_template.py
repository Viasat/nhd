import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="nhd",
    version="0.1.15",
    author="Cliff Burdick",
    author_email="cliff.burdick@viasat.com",
    description="NHD Custom Kubernetes Scheduler",
    long_description=long_description,
    url="https://git.viasat.com/mach3-phy/nhd",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Linux (Ubuntu 18.04)",
    ],
    scripts=['bin/nhd']
)
