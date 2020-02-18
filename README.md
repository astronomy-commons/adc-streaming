# Astronomy Data Commons Genesis Client Libraries

Libraries making it easy to access astronomy data commons resources.

## Developer notes

### Tag, build, and upload to PyPI and Conda

```
git tag -s -a v0.x.x
```

```
python setup.py sdist bdist_wheel
twine upload dist/*
```

```
conda build -c defaults -c conda-forge recipe
anaconda upload --user scimma /Users/mjuric/anaconda3/conda-bld/noarch/adc-0.0.2-py_0.tar.bz2
```
