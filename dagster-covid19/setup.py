from setuptools import find_packages, setup

setup(
    name="dagster_covid19",
    packages=find_packages(exclude=["dagster_covid19_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
