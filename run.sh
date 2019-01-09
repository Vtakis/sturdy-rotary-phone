#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
valgrind --leak-check=yes ${DIR}/myexe
