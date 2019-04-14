from setuptools import setup

setup(
    name="futile",
    version="0.1",
    description="",
    url="http://github.com/paulgessinger/futile",
    author="Paul Gessinger",
    author_email="hello@paulgessinger.com",
    license="MIT",
    install_requires=["click", "pyyaml", "halo", "sh"],
    extras_require={"dev": ["black"]},
    entry_points={"console_scripts": ["futile=futile.cli:main"]},
    packages=["futile"],
)