try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open('VERSION', 'rb') as v:
    version = v.read().decode().strip()

with open('README.md', 'rb') as ld:
    long_description = ld.read().decode().strip()

setup(
    author='Maciej Gol',
    author_email='1kroolik1@gmail.com',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Framework :: Django',
        'Programming Language :: Python',
    ],
    description='Celery integration for django-tenant-schemas and django-tenants',
    install_requires=[
        'celery>=5',
    ],
    packages=find_packages(),
    python_requires=">=3.9",
    name='tenant-schemas-celery',
    license='MIT',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/maciej-gol/tenant-schemas-celery',
    version=version,
)
