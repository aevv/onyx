#!/bin/bash

container = $(sudo docker container ls | grep background | awk '{print $1}')

sudo docker container exec -it $container car /var/log/onyx-info.log | grep 'codat'