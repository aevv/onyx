#!/bin/bash

git pull
sudo docker compose -f docker-compose.dev.yml -p onyx-stack up -d --build --force-recreate