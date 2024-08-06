#! /bin/bash

# uninstall pkg
function uninstall() {
	if [ "$1" == "bare" ];then
		dpkg --help >/dev/null 2>&1
		if [ $? -ne 0 ];then
			local manager="rpm"
		else
			local manager="dpkg"
		fi
		if [ "$manager" == "dpkg" ];then
			eval $kw_cmd_prefix $manager -r kaiwudb-server >/dev/null 2>&1
			eval $kw_cmd_prefix $manager -r kaiwudb-libcommon >/dev/null 2>&1
		elif [ "$manager" == "rpm" ];then
			eval $kw_cmd_prefix $manager -e kaiwudb-server >/dev/null 2>&1
			eval $kw_cmd_prefix $manager -e kaiwudb-libcommon >/dev/null 2>&1
		fi
		sudo rm -rf /usr/local/kaiwudb >/dev/null 2>&1
	else
		local image=`docker ps -a --filter name=kaiwudb-container --format {{.Image}}`
		if [ -n "$image" ];then
			docker rm kaiwudb-container > /dev/null 2>&1
		else
			local image=$(container_image)
		fi
		docker rmi $image > /dev/null 2>&1
	fi
}

function uninstall_dir() {
	local ret=""
	local data_root=$1
	if [ -n "$data_root" ];then
		eval $kw_cmd_prefix rm -rf /etc/kaiwudb /etc/kwdb /etc/profile.d/kw_env.sh /etc/systemd/system/kaiwudb.service
		if [ "$clear_user" == "yes" ] || [ "$clear_user" == "y" ]; then
			eval $kw_cmd_prefix rm -rf $data_root
		fi
	fi
}

function delete_user() {
	if [ "$1" == "bare" ];then
		# if bare mode， delete user and group
		eval $kw_cmd_prefix userdel -r $2 >/dev/null 2>&1
		sudo sed -i "/^$2 ALL=(ALL)  NOPASSWD: ALL$/d" /etc/sudoers
	fi
}