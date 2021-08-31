import requests
import random
import time
import psutil
import subprocess
import os
import shutil

# stem for Tor control
from stem.control import Controller
from stem import Signal
# import stem.connection
# import stem.socket
import stem.process

# start tor process
def start_tor(socks_port, control_port,max_try=5):
    if max_try==0:
        raise Exception("failed to start tor process socks:{socks} control:{control}".format(socks=socks_port,
                                                                                   control=control_port))
    else:
        try:
            tor_process = stem.process.launch_tor_with_config(
                config={
                'SocksPort': str(socks_port),
                'ControlPort': str(control_port),
                'DataDirectory': '~/.tor/tor_custom_{socks}_{control}'.format(socks=socks_port, control=control_port)
                },
                timeout=15
            )
            print("started tor process socks:{socks} control:{control}".format(socks=socks_port, control=control_port))
            print("tor process pid ",tor_process.pid)
            if not tor_process:
                print("start_tor: tor process is NoneType")
                print(tor_process)
                print(type(tor_process))
                raise Exception("tor process is NoneType")
            return tor_process
        except Exception as e:
            if 'timeout' in str(e):
                print(e)
                print("tor launch reached timeout for tor process socks:{socks} control:{control}".format(socks=socks_port,
                                                                                   control=control_port))
                print('trying launching tor again')
                return start_tor(socks_port, control_port, max_try-1)
            else:
                raise e


# renew tor connection
def renew_connection(control_port,sleep_time=None):
    with Controller.from_port(port=control_port) as controller:
        controller.authenticate()
        # print("Waiting time for Tor to change IP: " + str(controller.get_newnym_wait()) + " seconds")
        if sleep_time == None:
            time.sleep(random.uniform(0.5, 3))
        else:
            time.sleep(sleep_time)
        controller.signal(Signal.NEWNYM)
        controller.close()


def get_public_ip(proxy):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
    }
    res = requests.get("http://icanhazip.com", headers=headers, proxies=proxy)
    return res.content


def start_multiple_tor_processes(port_pair_list):
    process_list=[]
    for port_pair in port_pair_list:
        tor_process = start_tor(port_pair[0],port_pair[1])
        process_list.append(tor_process)
    return process_list

def kill_tor_processes(process_list):
    for p in process_list:
        try:
            print('trying to kill tor process with pid',p.pid)
            p.kill()
            print('killed tor process with pid',p.pid)
        except Exception as e:
            print('failed to kill process with pid ',p.pid ,'because of ',e)

def force_kill_tor_processes():
    cmd = "ps aux | grep 'tor -f -' | grep -v grep"
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    if len(err)>0:
        raise err
    if len(out) > 0:
        out = out.split('\n')
        out = out[:-1]
        tor_pids = [int(l.split()[1]) for l in out]
        for p in tor_pids:
            try:
                print('trying to force kill process with pid ', str(p))
                psutil.Process(int(p)).terminate()
                print('force killed process with pid ', str(p))
            except Exception as e:
                print('failed to force kill process with pid ', str(p), 'because of ', e)
    # remove custom tor config files
    folder = os.path.expanduser('~/.tor/')
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

if __name__ == "__main__":
    # force_kill_tor_processes()
    socks_ports = list(range(19000,19003))
    control_ports = list(range(19100,19103))
    port_pair_list = [[x,y] for x,y in zip(socks_ports,control_ports)]
    tor_process_list = start_multiple_tor_processes(port_pair_list)
    for socks_port, control_port in port_pair_list:
        proxy = {'http': "socks5://127.0.0.1:{port}".format(port=(socks_port))}
        print("public ip is: {} for socks port {}".format(get_public_ip(proxy),socks_port))
    print("renew connection")
    for socks_port, control_port in port_pair_list:
        renew_connection(control_port,sleep_time=0)
        proxy = {'http': "socks5://127.0.0.1:{port}".format(port=(socks_port))}
        print("public ip is: {} for socks port {}".format(get_public_ip(proxy),socks_port))

    kill_tor_processes(tor_process_list)

