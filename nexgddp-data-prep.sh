#!/bin/bash

case "$1" in
    first)
        type docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose is required but it's not installed.  Aborting."; exit 1; }
        docker-compose -f docker-compose-step1.yml build && docker-compose -f docker-compose-step1.yml up
        ;;
    second)
        type docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose is required but it's not installed.  Aborting."; exit 1; }
        docker-compose -f docker-compose-step2.yml build && docker-compose -f docker-compose-step2.yml up
        ;;
esac

exit 0
