#!/bin/bash
sudo docker stop namenode1
sudo docker stop datanode1
sudo docker stop datanode2
sudo docker rm namenode1 datanode1 datanode2