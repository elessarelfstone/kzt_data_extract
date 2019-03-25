#!/bin/bash

echo vagrant | su -

yum install epel-release -y
yum install https://centos7.iuscommunity.org/ius-release.rpm -y
yum install python36u python36u-devel python36u-pip -y
pip3.6 install --upgrade pip
pip3.6 install -r /vagrant/requirements.txt

 groupadd luigi
useradd -g luigi luigi

mkdir /etc/luigi
chown luigi:luigi /etc/luigi

mkdir /var/run/luigi
mkdir /var/log/luigi
mkdir /var/lib/luigi
chown luigi:luigi /var/run/luigi
chown luigi:luigi /var/log/luigi
chown luigi:luigi /var/lib/luigi