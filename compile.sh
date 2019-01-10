#!/bin/bash
make
valgrind --leak-check=full --show-leak-kinds=all ./myexe input-files/small/