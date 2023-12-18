from setuptools import setup, find_packages

setup(
    name='bitpy',
    version='1.29',
    description='BitPy',
    author='Philippe Remy',
    install_requires=[
        'numpy',
        'pandas',
        'websocket-client',
        'requests',
        'urllib3',
        'orjson'
    ],
    packages=find_packages()
)
