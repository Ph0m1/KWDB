#!/bin/python3
import os
import sys
import re

node_id_map = {}
http_ip_map = {}
join_node_id_map = {}
join_http_ip_map = {}

def get_nodes(node_str: str):
    strs = node_str.split(':')
    if len(strs) < 2:
        return None
    nodes = strs[1]
    nodes = re.sub(' ', '', nodes)
    nodes = nodes.split(',')
    ids = []
    for node in nodes:
        if re.match('c\d+', node):
            ids.append(int(re.sub('c', '', node)))
    return ids


def get_url_from_node_id(node_id: int):
    return node_id_map[str(node_id)]


def get_http_url_from_node_id(node_id: int):
    return http_ip_map[str(node_id)]


def get_url_for_join(node_id: int):
    return join_node_id_map[str(node_id)]


def get_http_url_fot_join(node_id: int):
    return join_http_ip_map[str(node_id)]


def get_sleep_cmd(sql: str):
    strs = sql.split(':')
    if len(strs) < 2:
        return None
    time = strs[1]
    return "sleep " + time


def is_blank(sql):
    sql = re.sub('\n', '', sql)
    sql = re.sub(' ', '', sql)
    if sql == "":
        return True
    return False


def get_node_id_map(kwbin: str):
    command = '{} node status --insecure --host=127.0.0.1:26257'.format(kwbin)
    res = os.popen(command)
    res = res.read()
    res = res.split('\n')
    res = res[1:]
    res = res[:-1]
    global node_id_map
    for i in res:
        node_id = i.split('\t')[0]
        ip = i.split('\t')[1]
        node_id_map[node_id] = ip
        port = ip.split(':')[1]
        http_port = int(port)-26257+8080
        http_addr = ip.split(':')[0]+':'+str(http_port)
        http_ip_map[node_id] = http_addr


def get_join_node_id_map(kwbin: str):
    command = '{} node status --insecure --host=127.0.0.1:26257'.format(kwbin)
    res = os.popen(command)
    res = res.read()
    res = res.split('\n')
    res = res[1:]
    res = res[:-1]
    global node_id_map
    global join_node_id_map
    global join_http_ip_map
    for i in range(1,6):
        node_id = str(5+i)
        listen_port = 26261+i
        join_node_id_map[node_id] = '127.0.0.1'+':'+str(listen_port)
        http_port = 8084+i
        http_addr = '127.0.0.1'+':'+str(http_port)
        join_http_ip_map[node_id] = http_addr

    '''for i in res:
        node_id = str(int(i.split('\t')[0])+5)
        ip = i.split('\t')[1]
        listen_port = int(ip.split(':')[1])+5
        join_node_id_map[node_id] = ip.split(':')[0]+':'+str(listen_port)
        port = ip.split(':')[1]
        http_port = int(port)-26257+8080+5
        http_addr = ip.split(':')[0]+':'+str(http_port)
        join_http_ip_map[node_id] = http_addr'''


if __name__ == "__main__":
    args = sys.argv[1:]
    sql_path = args[0]
    kwbin_path = args[2] + ' ' + args[3]
    output_file_path = args[4]

    store_dir = args[5]

    output_cmd_file = args[6]
    # print("=============================================")
    # print("sql_path ", sql_path)
    # print("kwbin_path ", kwbin_path)
    # print("output_file_path ", output_file_path)
    # print("license_dir ", license_dir)
    # print("store_dir ", store_dir)
    # print("output_cmd_file ", output_cmd_file)
    # print("=============================================")

    with open(sql_path, 'r') as f:
        sqls = f.readlines()
        sqls = [re.sub('\n', ' ', sql) for sql in sqls]
    cmds = []
    stmt = ''
    last_exec_node = [1]

    get_node_id_map(kwbin_path)
    get_join_node_id_map(kwbin_path)
    # for i in node_id_map:
    #     print(i,"    ",node_id_map[i])
    #     print(i,"    ",http_ip_map[i])


    for sql in sqls:
        if re.match('-- node:', sql):
            # reset last_exec_node
            node_ids = get_nodes(sql)
            last_exec_node = node_ids
            pass
        elif re.match('-- kill:', sql):
            # kill node
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = 'listen-addr=' + get_url_for_join(node_id)
                else:
                    url = 'listen-addr=' + get_url_from_node_id(node_id)
                cmd = 'kill -9 `ps -ef | grep kwbase | grep \'{}\''.format(url) + ' | awk \'{print $2}\'`'
                cmds.append(cmd)

            pass
        elif re.match('-- sleep', sql):
            # sleep
            cmd = get_sleep_cmd(sql)
            cmds.append(cmd)
            pass
        elif re.match('-- restart', sql):
            #print(sql)
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = get_url_for_join(node_id)
                    store_id =int(url.split(':')[-1]) - 26256
                    cmd = ' {} {}' \
                          ' --insecure --listen-addr={}' \
                          ' --http-addr={}' \
                          ' --store={}/{}' \
                          ' --locality=region=CN-100000-0{}' \
                          ' --pid-file={}/kwbase.pid ' \
                          ' --join={} --background'.format(
                        kwbin_path, 'start', url, get_http_url_fot_join(node_id), store_dir, 'c' + str(store_id),
                        "%02d" % node_id, store_dir+'/c' + str(store_id), get_url_from_node_id(1))
                    cmds.append(cmd)
                else:
                    url = get_url_from_node_id(node_id)
                    store_id =int(url.split(':')[-1]) - 26256
                    cmd = ' {} {}' \
                          ' --insecure --listen-addr={}' \
                          ' --http-addr={}' \
                          ' --store={}/{}' \
                          ' --locality=region=CN-100000-0{}' \
                          ' --pid-file={}/kwbase.pid ' \
                          ' --join={} --background'.format(
                        kwbin_path, 'start', url, get_http_url_from_node_id(node_id), store_dir, 'c' + str(store_id),
                        "%02d" % node_id, store_dir+'/c' + str(store_id), get_url_from_node_id(1))
                    cmds.append(cmd)
        elif re.match('-- join', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_for_join(node_id)
                cmd = ' {} {}' \
                      ' --insecure --listen-addr={}' \
                      ' --http-addr={}' \
                      ' --store={}/{}' \
                      ' --locality=region=CN-100000-0{}' \
                      ' --join={} --background'.format(
                    kwbin_path, 'start', url, get_http_url_fot_join(node_id), store_dir,
                    'c' + str(node_id),
                    "%02d" % node_id,
                    get_url_from_node_id(1)
                )
                cmds.append(cmd)
        elif re.match('-- decommission', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = get_url_for_join(node_id)
                else:
                    url = get_url_from_node_id(node_id)
                cmd = ' {} {}' \
                      ' --decommission --insecure ' \
                      '--host={} --drain-wait=8s'.format(kwbin_path, 'quit', url)
                cmds.append(cmd)
        elif re.match('-- exec-decommission', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = get_url_for_join(node_id)
                else:
                    url = get_url_from_node_id(node_id)
                cmd = ' {} {}' \
                      ' decommission {} --insecure ' \
                      '--host={} --wait=none'.format(kwbin_path, 'node', node_id, url)
                cmds.append(cmd)
        elif re.match('-- wait-join', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;"\
                    "while [ $({} node status {}" \
                      " --insecure --host={}" \
                      " --format=csv | tail -n1| grep -cP \"true,healthy\") -ne 1 ] && [ $count -lt 180 ];" \
                      "do " \
                      " sleep 1s;  count=$((count+1)) ;" \
                      "done\nif [ $count -ge 180 ]; then echo \"wait-join timeout after 180s\" ;fi".format(kwbin_path, str(node_id), url)
                cmds.append(cmd)
        elif re.match('-- wait-dead', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;"\
                      "while [ $({} node status {}" \
                      " --insecure --host={}" \
                      " --format=csv | tail -n1| grep -cP \"false,dead\") -ne 1 ] && [ $count -lt 180 ];" \
                      "do " \
                      " sleep 1s;  count=$((count+1)) ;" \
                      "done\nif [ $count -ge 180 ]; then echo \"wait-dead timeout after 180s\" ;fi".format(kwbin_path, str(node_id), url)
                cmds.append(cmd)
        elif re.match('-- is-unhealthy', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;"\
                      "while [ $({} node status {}" \
                      " --insecure --host={}" \
                      " --format=csv | tail -n1| grep -cP \"false,unhealthy\") -ne 1 ] && [ $count -lt 180 ];" \
                      "do " \
                      " sleep 1s;  count=$((count+1)) ;" \
                      "done\nif [ $count -ge 180 ]; then echo \"is-unhealthy timeout after 180s\" ;fi".format(kwbin_path, str(node_id), url)
                cmds.append(cmd)
        elif re.match('-- is-prejoin', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;"\
                      "while [ $({} node status {}" \
                      " --insecure --host={}" \
                      " --format=csv | tail -n1| grep -cP \"true,pre-join\") -ne 1 ] && [ $count -lt 180 ];" \
                      "do " \
                      " sleep 1s;  count=$((count+1)) ;" \
                      "done\nif [ $count -ge 180 ]; then echo \"is-prejoin timeout after 180s\" ;fi".format(kwbin_path, str(node_id), url)
                cmds.append(cmd)
        elif re.match('-- is-joining', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;"\
                      "while [ $({} node status {}" \
                      " --insecure --host={}" \
                      " --format=csv | tail -n1| grep -cP \"true,joining\") -ne 1 ] && [ $count -lt 180 ];" \
                      "do " \
                      " sleep 1s;  count=$((count+1)) ;" \
                      "done\nif [ $count -ge 180 ]; then echo \"is-joining timeout after 180s\" ;fi".format(kwbin_path, str(node_id), url)
                cmds.append(cmd)
        elif re.match('-- is-decommissioning', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;"\
                      "while [ $({} node status {}" \
                      " --insecure --host={}" \
                      " --format=csv | tail -n1| grep -cP \"true,decommissioning\") -ne 1 ] && [ $count -lt 180 ];" \
                      "do " \
                      " sleep 1s;  count=$((count+1)) ;" \
                      "done\nif [ $count -ge 180 ]; then echo \"is-decommissioning timeout after 180s\" ;fi".format(kwbin_path, str(node_id), url)
                cmds.append(cmd)
        elif re.match('-- wait-running', sql):
            node_ids = get_nodes(sql)
            url = get_url_from_node_id(node_ids[0])
            cmd = "count=0;"\
                  "while [ $({} sql --insecure --host={} " \
                  "-e \"select * from [show ts partitions] where status != 'running'\" --format=csv | grep -v status | wc -l ) -ne 0 ] && [ $count -lt 180 ];" \
                  "do " \
                  " sleep 1s;  count=$((count+1)) ;" \
                  "done\nif [ $count -ge 180 ]; then echo \"wait-running timeout after 180s\" ;fi".format(kwbin_path, url)
            cmds.append(cmd)
        elif re.match('-- wait-replicas', sql):
            node_ids = get_nodes(sql)
            url = get_url_from_node_id(node_ids[0])
            cmd = "count=0;" \
                  "while [ $({} sql --insecure --host={} " \
                  "-e \"select * from [show ts partitions] where array_length(replicas,1) != 3\" --format=csv | grep -v status | wc -l ) -ne 0 ] && [ $count -lt 180 ];" \
                  "do " \
                  " sleep 1s;  count=$((count+1)) ;" \
                  "done\nif [ $count -ge 180 ]; then echo \"wait-replicas timeout after 180s\" ;fi".format(kwbin_path, url)
            cmds.append(cmd)
        elif re.match('-- upgrade-complete', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(node_id)
                store_id =int(url.split(':')[-1]) - 26256
                cmd = ' {} {}' \
                      ' --insecure --listen-addr={}' \
                      ' --http-addr={}' \
                      ' --store={}/{}' \
                      ' --locality=region=CN-100000-0{}' \
                      ' --pid-file={}/kwbase.pid ' \
                      ' --join={} --background'.format(
                    kwbin_path, 'start --upgrade-complete', url, get_http_url_from_node_id(node_id), store_dir,
                    'c' + str(store_id), "%02d" % node_id, store_dir+'/c' + str(store_id), get_url_from_node_id(1))
            cmds.append(cmd)
        elif re.match('-- upgrade', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                cmd = '{}  node upgrade {} --insecure --host={}'.format(
                    kwbin_path,node_id,get_url_from_node_id(node_id)
                )
            cmds.append(cmd)
        elif re.match('-- set-dead', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                cmd = '{}  node set-status dead --node-id={} --insecure --host={}'.format(
                    kwbin_path,node_id,get_url_from_node_id(1)
                )
            cmds.append(cmd)

        #elif re.match('-- return', sql):
        #    cmds.append('return ')



        elif re.match('-- ', sql):
            # other comments
            pass
        else:
            # normal sql stmt
            if is_blank(sql):
                pass
            if len(sql) == 0:
                pass
            temp_sql =  re.sub(' ', '', sql)
            if len(temp_sql)>0 and temp_sql[-1] == ';':
                stmt += ' ' + sql
                stmt = re.sub('^ *','',stmt)

                for node in last_exec_node:
                    cmd = "{} sql --host={} --insecure --format=table --set \"errexit=false\" --echo-sql -e \"{}\" 2>&1 | tee -a {} > /dev/null".format(
                        kwbin_path, get_url_from_node_id(node), stmt, output_file_path)
                    cmds.append(cmd)
                stmt = ''
            else:
                stmt += ' ' + sql
            pass
    with open(output_cmd_file, 'w') as f:
        for cmd in cmds:
            f.write(cmd)
            f.write('\n')
