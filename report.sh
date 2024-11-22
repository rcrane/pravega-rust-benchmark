#!/bin/bash

FILE=$(ls -1 result_*.json | tail -n 1)
jq . $FILE
