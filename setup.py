#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
from setuptools import setup, find_packages

setup(name="databass",
      version="0.0.01",
      description="Query Compilation + Lineage",
      license="MIT",
      author="WuLab",
      author_email="ewu@cs.columbia.edu",
      url="http://github.com/cudbg/databass",
      include_package_data = True,      
      packages = find_packages(),
      package_dir = {'databass' : 'databass'},
      scripts = [],
      package_data = { 'databass' : ['databass/data/*'] },
      install_requires = [
        'flask', 'parsimonious', 'pandas', 'numpy',
        'python-dateutil', 'nose', 'sqlalchemy'
      ],
      keywords= "database engine compiler")
