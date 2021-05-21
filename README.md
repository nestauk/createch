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

Run `python createch/getters/crunchbase.py` to fetch and save relevant CrunchBase tables including:

- `crunchbase_organizations`: CrunchBase organisations in the UK
- `crunchbase_funding_rounds`: CrunchBase funding rounds in the UK
- `crunchbase_organizations_categories`: lookup between CrunchBase organisations in the UK and their categories
- `crunchbase_category_groups`: Lookup between crunchbase categories and higher level categories.

Run `python createch/getters/gtr.py` to fetch:

- `gtr_projects`
- `gtr_funders` (which we use to get project start dates)
- `gtr_topics`
- `gtr_link_table` for merging various gtr tables

We still need to create fetchers & queries for gtr organisation data and locations

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
