# -*- mode: ruby -*-
# vi: set ft=ruby :


Vagrant.configure("2") do |config|
  config.vm.define "server" do |server|
    server.vm.box = "centos/7"
    server.vm.hostname = "big-data-collector"
    server.vm.network "private_network", ip: "192.168.56.9"
    server.vm.provision "shell", path: "install.sh"
    server.vm.synced_folder ".", "/vagrant", type: "rsync", rsync__exclude: [".git/", "./idea", "__pycache__"]
  end
end