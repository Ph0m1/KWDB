cd .CI
sh 7_build_kwbase_withUI.sh
The cluster startup command is as follows:
start_cluster.sh ${ROOT_PATH} //ROOT_PATH is the project root directory, for example: /home/blackhole/go2/src/github.com/ZDP

Note: need to configure sudo password-free, otherwise it may not work properly
