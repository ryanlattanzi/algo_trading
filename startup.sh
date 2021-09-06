#!/usr/bin/env bash

docker compose rm -fsv
docker compose down

echo "New database build? (y)"
read new_build

if [ "$new_build" == "y" ]; then
    echo "Building this new shidd..."
    say building this new shidd
    docker volume prune -f
else
    echo "Ay bruh no new shit"
    say ay bruh no new shit
fi

docker compose up