from setuptools import setup, find_packages

setup(
    name='bitpy',
    version='1.12',
    description='BitPy',
    author='Philippe Remy',
    install_requires=[
        'numpy',
        'websocket-client',
        'requests',
        'urllib3'
    ],
    packages=find_packages()
)
