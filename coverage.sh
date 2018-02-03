#!/usr/bin/env bash

set -e
set -x

echo "Making coverage report"
make test
make coverage

echo "Uploading coverage report"
ocveralls `find . -name 'bisect*.out'` --send