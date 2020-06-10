from setuptools import setup, find_packages

setup(
    name='pyrugby',
    version='1.0.0',
    description='Scraping rugby related data from various sources on the web',
    url='https://github.com/awgymer/pyrugby',
    author='Arthur Gymer',
    packages=find_packages(),
    install_requires=[
        'requests',
        'markdown',
        'beautifulsoup4'
    ]
)
