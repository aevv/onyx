#!/bin/bash

container=$(sudo docker container ls | grep background | awk '{print $1}')

sudo docker container exec -it $container cat /var/log/onyx_info.log | grep 'codat:'