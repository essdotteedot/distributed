#!/usr/bin/env bash

set -e
set -x

echo "Installing alcotest"
opam install alcotest

echo "Installing bisect_ppx"
opam install bisect_ppx

echo "Installing ocveralls"
opam install ocveralls

echo "Pinning lwt safer semantics"
opam pin add lwt https://github.com/ocsigen/lwt.git\#safer-semantics -y

echo "Running test and making coverage report"
make test

echo "Uploading coverage report"
ocveralls `find . -name 'bisect*.out'` --send