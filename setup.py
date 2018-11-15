#!/usr/bin/env python
from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

setup(
    name='asyncoss',
    version='0.0.1',
    description='A async aliyun OSS library.',
    long_description=readme,
    author='jerevia',
    author_email='trilliondawn@gmail.com',
    license='MIT',
    install_requires=['aiohttp', 'oss2'],
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ]
)
