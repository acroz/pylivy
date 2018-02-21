from pathlib import Path
from setuptools import setup


README = Path(__file__).parent / 'README.rst'


setup(
    name='livy',
    version='0.1.0',
    description='A Python client for Apache Livy',
    long_description=README.read_text(),
    py_modules=['livy'],
    url='https://github.com/acroz/pylivy',
    author='Andrew Crozier',
    author_email='wacrozier@gmail.com',
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    setup_requires=[
        'wheel'
    ],
    install_requires=[
        'requests',
        'pandas'
    ]
)
