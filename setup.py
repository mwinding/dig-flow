from setuptools import setup, find_packages

setup(name='digflow',
      version='0.0.1',
      description='pipelines to analyse cooperative digging videos of fruit fly larvae on a HPC',
      url='https://github.com/mwinding/dig-flow',
      author='Michael Winding',
      author_email='m.j.winding@gmail.com',
      license='MIT',
      packages=find_packages(include=['digflow', 'digflow.*']),
      install_requires=['pandas']
      )

