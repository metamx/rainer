from setuptools import setup

setup(
  name='pyrainer',
  version='0.18',
  author='gian',
  author_email='gianmerlino@gmail.com',
  packages=['pyrainer'],
  license='LICENSE',
  url='https://github.com/metamx/rainer',
  description='Python client for Rainer',
  long_description=open('README.md').read(),
  install_requires=["termcolor >= 1.1.0"],
)
