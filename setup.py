from setuptools import setup

setup(name='minibatch',
      version='0.1',
      description='Python stream processing for humans',
      url='http://github.com/omegaml/minibatch',
      author='Patrick Senti',
      author_email='patrick.senti@omegaml.io',
      license='MIT',
      packages=['minibatch'],
      zip_safe=False,
      install_requires=[
          'mongoengine>=0.18,<0.19',
      ])