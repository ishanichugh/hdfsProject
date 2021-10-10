#!/bin/bash
sudo docker build -f Dockerfile.namenode . -t namenode
sudo docker build -f Dockerfile.datanode . -t datanode
sudo docker run -d --name namenode1 -p 1099:1099 namenode
sudo docker run -d --name datanode1 -p 2099:2099 datanode
sudo docker run -d --name datanode2 -p 2098:2099 datanode