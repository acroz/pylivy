from pathlib import Path
from setuptools import setup


README = Path(__file__).parent / "README.rst"


setup(
    name="livy",
    description="A Python client for Apache Livy",
    long_description=README.read_text(),
    packages=["livy"],
    url="https://github.com/acroz/pylivy",
    author="Andrew Crozier",
    author_email="wacrozier@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    use_scm_version={"version_scheme": "post-release"},
    setup_requires=["wheel", "setuptools_scm"],
    install_requires=[
        "dataclasses; python_version<'3.7'",
        "requests",
        "pandas",
    ],
    extras_require={"docs": ["sphinx", "sphinx-autodoc-typehints"]},
)
