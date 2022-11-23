from setuptools import setup, find_packages
import os

ROOT = os.path.dirname(os.path.realpath(__file__))

with open('README.md') as inp:
    readme_content = inp.read()


setup(
    name = 'procstat',
    version = '0.0.3',
    author = 'Gregory Petukhov',
    author_email = 'lorien@lorien.name',
    maintainer='Gregory Petukhov',
    maintainer_email='lorien@lorien.name',
    url='https://github.com/lorien/procstat',
    description = 'A tool to count runtime metrics',
    long_description = readme_content,
    long_description_content_type='text/markdown',
    packages = find_packages(exclude=['test']),
    download_url='https://github.com/lorien/procstat/releases',
    license = "MIT",
    entry_points = {},
    install_requires = [],
    keywords='statistics counter metric runtime',
    classifiers = [
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
