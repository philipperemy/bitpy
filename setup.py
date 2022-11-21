from setuptools import setup, find_packages

setup(
    name='bitpy',
    version='1.1',
    description='BitPy',
    author='Philippe Remy',
    install_requires=[
        'numpy',
        'websocket-client',
        'requests'
    ],
    packages=find_packages()
)
