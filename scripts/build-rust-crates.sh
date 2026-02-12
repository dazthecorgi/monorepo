#!/bin/bash

set -euo pipefail

pushd vdf
./generate.sh
popd

pushd ferret
./generate.sh
popd

pushd bls48581
./generate.sh
popd

pushd bulletproofs
./generate.sh
popd

pushd verenc
./generate.sh
popd

pushd channel
./generate.sh
popd

pushd channel
./generate.sh
popd

pushd rpm
./generate.sh
popd
