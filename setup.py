from setuptools import setup, find_packages

setup(
    name='bitpy',
    version='1.0',
    description='BitPy',
    author='Philippe Remy',
    requires=[
        'numpy',
        'pandas',
        'websocket-client',
        'requests'
    ],
    packages=find_packages()
)
