from setuptools import setup


# requirements
install_requires = [
    "authlib",  # FIXME: drop after next release of confluent-kafka with OIDC support
    "confluent-kafka >= 1.6.1, != 2.1.0, != 2.1.1",
    "dataclasses ; python_version < '3.7'",
    "importlib-metadata ; python_version < '3.8'",
    "requests",  # FIXME: drop after next release of confluent-kafka with OIDC support
    "tqdm",
    "certifi>=2020.04.05.1",
    "typing-extensions ; python_version < '3.8'",
]

dev_requires = [
    "autopep8",
    "docker",
    "flake8",
    "isort",
    "pytest",
    "pytest-timeout",
    "pytest-integration",
    "sphinx",
    "sphinx_rtd_theme",
    "twine",
]


setup(name='adc-streaming',
      description='Astronomy Data Commons streaming client libraries',
      long_description=open("README.md").read(),
      long_description_content_type="text/markdown",
      url='https://github.com/astronomy-commons/adc-streaming',
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: BSD License",
          "Development Status :: 3 - Alpha",
          "Operating System :: POSIX :: Linux",
          "Operating System :: MacOS :: MacOS X"
      ],
      author='Astronomy Data Commons Team',
      author_email='swnelson@uw.edu',
      license='BSD',
      packages=['adc'],
      install_requires=install_requires,
      extras_require={
          "dev": dev_requires,
      },
      zip_safe=False)
