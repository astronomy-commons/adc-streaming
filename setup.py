from setuptools import setup

##
# Infer version from git tag information
##


def git_version():
    # Uses the following logic:
    #
    # * If the current commit has an annotated tags, the version is simply the tag with
    #   the leading 'v' removed.
    #
    # * If the current commit is past an annotated tag, the version is constructed at:
    #
    #    '{tag+1}.dev{commitcount}+{gitsha}'
    #
    #  where {commitcount} is the number of commits after the tag (obtained with `git describe`)
    #  and {tag+1} is the tag version incremented by one (i.e., if tag=0.0.1, it's 0.0.2). This
    #  is needed because .dev tags compare as less than non-.dev tags, per PEP-440.
    #
    # * If there are no annotated tags in the past, the version is:
    #
    #    '0.0.0.dev{commitcount}+{gitsha}'
    #
    # Inspired by https://github.com/pyfidelity/setuptools-git-version
    # Creates PEP-440 compliant versions

    from subprocess import check_output

    command = 'git describe --tags --long --dirty --always'
    version = check_output(command.split()).decode('utf-8').strip()

    parts = version.split('-')
    if len(parts) in (3, 4):
        dirty = len(parts) == 4
        tag, count, sha = parts[:3]
        if not tag.startswith('v'):
            raise Exception(
                "Annotated tags on the repository must begin with the letter 'v'. Please fix this then try building agains.")
        tag = tag[1:]
        if count == '0' and not dirty:
            return tag
    elif len(parts) in (1, 2):
        tag = "0.0.0"
        dirty = len(parts) == 2
        sha = parts[0]
        # Number of commits since the beginning of the current branch
        count = check_output(
            "git rev-list --count HEAD".split()).decode('utf-8').strip()

    # this will be a .dev version; assume the version is of the form
    # major[.minor[.patchlevel]], and increment the patchlevel by 1.
    # because minor and/or patchlevel are optional
    (major, minor, patchlevel) = (tag + ".0.0").split('.')[:3]
    patchlevel = str(int(patchlevel) + 1)
    tag = '.'.join((major, minor, patchlevel))

    # if the working directory is dirty, append a '.dYYYYMMDD' tag
    if dirty:
        import time
        dirtytag = time.strftime('.d%Y%m%d')
    else:
        dirtytag = ''

    fmt = '{tag}.dev{commitcount}+g{gitsha}{dirtytag}'
    return fmt.format(tag=tag, commitcount=count, gitsha=sha.lstrip('g'), dirtytag=dirtytag)


# requirements
install_requires = [
    "confluent-kafka",
    "dataclasses ; python_version < '3.7'",
    "tqdm",
    "certifi>=2020.04.05.1"
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
      version=git_version(),
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
