# createch

## Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter), in brief:
  - Install: `git-crypt`
  - Have a Nesta AWS account configured with `awscli`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure pre-commit
  - Configure metaflow to use AWS

## Features

Run `python createch/getters/gtr.py` to fetch gtr project descriptions, funders, topics and a link table for merging them.

We still need to create getters for gtr organisation data and locations

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
