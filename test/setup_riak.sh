#!/bin/bash

set -e

docker exec -it riakkv riak-admin bucket-type create sets '{"props":{"datatype":"set", "n_val":3}}'
docker exec -it riakkv riak-admin bucket-type activate sets
