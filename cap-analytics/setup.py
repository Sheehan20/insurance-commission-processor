from setuptools import setup, find_packages

setup(
    name="commission_reconciliation",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'openpyxl',
        'pytest'
    ],
)