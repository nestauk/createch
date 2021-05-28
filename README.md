# createch

## Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter), in brief:
  - Install: `git-crypt`
  - Have a Nesta AWS account configured with `awscli`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure pre-commit
  - Configure metaflow to use AWS
- Run `git clone https://github.com/martingerlach/hSBM_Topicmodel.git` inside `createch` to clone the `top-SBM` repo.

## Features

### Extra dependencies

Run `python -m spacy download en_core_web_sm` to install the Spacy language model

### Fetch data

Run `make fetch-daps1` to Fetch GtR and CB data from `nesta/nestauk` (DAPS1), including:

- Crunchbase:
  - `crunchbase_organizations`: CrunchBase organisations in the UK
  - `crunchbase_funding_rounds`: CrunchBase funding rounds in the UK
  - `crunchbase_organizations_categories`: lookup between CrunchBase organisations in the UK and their categories
  - `crunchbase_category_groups`: Lookup between crunchbase categories and higher level categories.
- GtR:
  - `gtr_projects`
  - `gtr_funders` (which we use to get project start dates)
  - `gtr_topics`
  - `gtr_link_table` for merging various gtr tables

We still need to create fetchers & queries for gtr organisation data and locations

### Tokenise and Word2Vec source descriptions

Run `python createch/pipeline/model_tokenise.py` to tokenise {source} descriptions and train a word2vec model. The respective json files and models are saved in `outputs/{output_type}/{source}`

### Semantic identification

Run `python createch/pipeline/semantic_identification.py` to expand technology vocabularies and tag relevant descriptions. The expanded vocabularies and id - area lookups are saved in `outputs/data/{source}`.

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
