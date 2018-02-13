#!/usr/bin/env bash

set -e
set -x

echo "Installing bisect_ppx"
opam install bisect_ppx

echo "Installing ocveralls"
opam install ocveralls

echo "Running test and making coverage report"
make test

echo "Uploading coverage report"
ocveralls `find . -name 'bisect*.out'` --send