import os
from setuptools import setup

README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()
version = open(os.path.join(os.path.dirname(__file__), 'minibatch', 'VERSION')).read()


setup(name='minibatch',
      version=version,
      description='Python stream processing for humans',
      url='http://github.com/omegaml/minibatch',
      long_description=README,
      long_description_content_type='text/x-rst',
      include_package_data=True,
      author='Patrick Senti',
      author_email='patrick.senti@omegaml.io',
      license='MIT',
      packages=['minibatch'],
      zip_safe=False,
      install_requires=[
          'mongoengine>=0.18', # Mongo 4.2 requires at least mongoengine 0.19 due to https://github.com/MongoEngine/mongoengine/pull/2160/files
      ])
