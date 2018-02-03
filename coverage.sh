#!/usr/bin/env bash

set -e
set -x

# assume that tests have been run and lib was built with instrumentation on
echo "Making coverage report"
make coverage

echo "Uploading coverage report"
ocveralls `find . -name 'bisect*.out'` --send