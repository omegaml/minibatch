import os
from pathlib import Path

from setuptools import setup

basedir = Path(os.path.dirname(__file__))
README = open(basedir / 'README.rst').read()
version = open(basedir / 'minibatch' / 'VERSION').read()

kafka_deps = ['kafka-python==1.4.7']
mqtt_deps = ['paho-mqtt==1.5.0']

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
          # Mongo 4.2 requires at least mongoengine 0.19 due to
          # https://github.com/MongoEngine/mongoengine/pull/2160/files
          'mongoengine>=0.18',
          'dill',
      ],
      extras_require={
          'kafka': kafka_deps,
          'mqtt': mqtt_deps,
          'all': kafka_deps + mqtt_deps
      },
      )
