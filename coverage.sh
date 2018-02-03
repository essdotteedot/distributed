#!/usr/bin/env bash

set -e
set -x

echo "Installing ounit"
opam install ounit

echo "Installing bisect_ppx"
opam install bisect_ppx

echo "Installing ocveralls"
opam install ocveralls

echo "Making coverage report"
make test
make coverage

echo "Uploading coverage report"
ocveralls `find . -name 'bisect*.out'` --send