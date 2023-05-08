from setuptools import find_packages, setup

setup(
    name="lab5",
    packages=find_packages(exclude=["lab5_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "faker",
        "pandas"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
