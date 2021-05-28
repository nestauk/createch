SHELL := /bin/bash

DEFAULT_GOAL: all

# Default `test_mode` for metaflows
test_mode = true

# If `batch=true` define `batch_args`
batch = false
ifeq ($(batch),true)
batch_args = --with batch:queue=job-queue-nesta-metaflow-test,memory=32000,cpu=1
endif

all: match-gtr match-crunchbase

## Dump CH + CB names to disk ready for passing to flow
$(BASE)/outputs/.cache/cb_names.json: crunchbase/prepare.py
	python crunchbase/prepare.py

## Dump CH + GtR names to disk
$(BASE)/outputs/.cache/gtr_names.json: gtr/prepare.py
	python gtr/prepare.py

.PHONY: match-crunchbase
## Fuzzymatch Crunchbase to Companies House
match-crunchbase: $(BASE)/outputs/.cache/cb_names.json
	python jacchammer.py\
	 --environment=conda --no-pylint\
	 run\
	 --names-x $(BASE)/outputs/.cache/company_names.json\
	 --names-y $(BASE)/outputs/.cache/cb_names.json\
	 --clean_names true\
	 --test_mode $(test_mode)\
	 $(batch_args)
	#  --run-id-file $(BASE)/createch/config/metaflow_run_ids/jacchammer/crunchbase\


.PHONY: match-gtr
## Fuzzymatch GtR to Companies House
match-gtr: $(BASE)/outputs/.cache/gtr_names.json
	python jacchammer.py\
	 --environment=conda --no-pylint\
	 run\
	 --names-x $(BASE)/outputs/.cache/company_names.json\
	 --names-y $(BASE)/outputs/.cache/gtr_names.json\
	 --clean_names true\
	 --test_mode $(test_mode)\
	 $(batch_args)
	#  --run-id-file $(BASE)/createch/config/metaflow_run_ids/gtr\
