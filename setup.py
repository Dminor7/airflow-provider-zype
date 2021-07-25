"""Setup.py for the Zype Airflow provider package."""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-zype setup."""
setup(
    name="airflow-provider-zype",
    version="0.0.1",
    description="A zype provider package.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=zype_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=["zype_provider", "zype_provider.hooks", "zype_provider.operators"],
    install_requires=["apache-airflow>=2.0","zype @ git+https://github.com/Dminor7/zype-python.git@master"],
    setup_requires=["setuptools", "wheel"],
    author="Darsh Shukla",
    author_email="contact.dshukla@gmail.com",
    python_requires=">=3.8,<4.0",
)
