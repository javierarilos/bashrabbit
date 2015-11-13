#!/usr/bin/env bash
# Run tests: unit tests are always run. integration tests when there is a rabbitmq available.
#   RabbitMQ is expected to be on guest:guest@127.0.0.1
#   RabbitMQ location can be configured as in the following example:
#   Usage sample: RABBIT_HOST=10.95.101.248 RABBIT_USER=speech RABBIT_PASS=speech ./run_tests.sh

cd src/
python -m unittest discover --verbose
