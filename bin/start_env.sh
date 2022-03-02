#!/bin/bash

docker compose rm -fsv
docker compose down

# If arg "new" is supplied, tear down the volumes
# and start from scratch.

if [ "$1" =  "new" ]
    then
        echo "Using new volumes."
        say building this new shidd
        docker volume prune -f
    else
        echo "Using existing volumes."
        say using existing shidd
fi

docker compose up -d --build
