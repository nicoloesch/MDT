from setuptools import setup

with open("README", 'r') as f:
    long_description = f.read()

setup(
   name='MDT',
   version='0.01',
   description='Plotter for medical data',
   license="MIT",
   long_description='this is the long description',
   author='Nico Lösch',
   packages=['MDT'],
   install_requires=['pyqt5', 'pyqtgraph'],
)