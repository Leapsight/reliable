#!/bin/bash

# set -e

docker exec -it fleet-riakkv riak-admin bucket-type create sets '{"props":{"datatype":"set", "n_val":3}}'
docker exec -it fleet-riakkv riak-admin bucket-type activate sets
