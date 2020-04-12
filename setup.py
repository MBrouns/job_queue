from setuptools import setup, find_packages

base_requirements = ["redis>=3.4.1", "click>=7.1.1"]
dev_requirements = ["pytest>=5.4.1", "flake8>=3.7.9"]

setup(
    name="fonlaboristo",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=base_requirements,
    extras_require={"dev": dev_requirements},
    entry_points={"console_scripts": ["fonlaboristo = fonlaboristo.cli:main"]},
)
