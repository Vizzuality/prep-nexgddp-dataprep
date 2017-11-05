#!/bin/bash
set -e

case "$1" in
    first)
        echo "Running first step"
        exec python process_step_one.py
        ;;
    second)
        echo "Running second step"
        exec python process_step_two.py
        ;;
    *)
        exec "$@"
esac
