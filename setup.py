# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='sample etl',
    version='0.1.0',
    description='Sample ETL using Prefect',
    long_description=readme,
    author='Alexandre Mello',
    author_email='alexandre@malme.com.br',
    url='https://github.com/alexmalme/etl_w',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

