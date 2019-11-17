#!/usr/bin/env bash

set -e
set -x

echo "Installing bisect_ppx"
opam install bisect_ppx

echo "Running test and making coverage report"
make clean
dune runtest
bisect-ppx-report -I _build/default/ --coveralls coverage.json --service-name travis-ci --service-job-id $TRAVIS_JOB_ID `find . -name 'bisect*.out'`

echo "Uploading coverage report"
curl -L -F json_file=@./coverage.json https://coveralls.io/api/v1/jobs"