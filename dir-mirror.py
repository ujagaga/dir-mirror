#!/usr/bin/python3

import os
import hashlib
import argparse
import sys
import inotify.adapters
import inotify.constants
import time
import sqlite3
from tendo import singleton
import threading
import asyncio
import json
import websockets

me = singleton.SingleInstance()
db = None
DATABASE = os.path.join(os.path.expanduser('~'), ".dir-mirror.db")
ROOT = "/"
MODE = None
PORT = None
HOST = None
event_queue = []
file_db = {}


def print_files():
    print("\n\n")
    for key in file_db.keys():
        print(key, file_db[key])

    print("\n\n")


def calc_hash(file_path):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)

    return file_hash.hexdigest()


def db_open():
    global db

    db = sqlite3.connect(DATABASE)


def db_close():
    global db

    db.close()


def query_db(query, args=(), one=False):
    cur = db.execute(query, args)
    rv = [dict((cur.description[idx][0], value)
               for idx, value in enumerate(row)) for row in cur.fetchall()]
    return (rv[0] if rv else None) if one else rv


def exec_db(query):
    db.execute(query)
    if not query.startswith('SELECT'):
        db.commit()


def get_stat(file_path):
    info = None
    try:
        fhash = None
        fsize = None
        mtime = None

        if os.path.isdir(file_path):
            ftype = 'dir'
        elif os.path.isfile(file_path):
            if os.path.islink(file_path):
                ftype = 'unsupported'
            else:
                ftype = 'file'
                fhash = calc_hash(file_path)
                fsize = os.path.getsize(file_path)
                mtime = os.path.getmtime(file_path)
        else:
            ftype = 'unsupported'

        if ftype != 'unsupported':
            info = {'type': ftype, 'mtime': mtime, 'size': fsize, 'hash': fhash}
        else:
            print("Warning, Unsupported file:{}".format(file_path))

    except Exception as e:
        print("ERROR in stat for:{}\n\t{}".format(file_path, e))

    return info


def add_dir_to_db(relative_dir_path):
    global file_db

    relative_dir_path = "/" + relative_dir_path.strip("/")
    full_dir_path = os.path.join(ROOT, relative_dir_path.strip("/"))
    parent_dir = os.path.dirname(relative_dir_path)
    dir_name = '/' + os.path.basename(relative_dir_path)

    # Add self to parent
    if dir_name not in file_db[parent_dir]['content']:
        file_db[parent_dir]['content'].append(dir_name)

    # Add children to self content
    for root, d_names, f_names in os.walk(full_dir_path):
        # Add folder to self content
        relative_root = '/' + root[len(ROOT):].strip('/')
        root_stat = get_stat(root)
        parent_dir = os.path.dirname(relative_root)
        dir_name = '/' + os.path.basename(relative_root)

        if root_stat is not None:
            if dir_name not in file_db[parent_dir]['content']:
                file_db[parent_dir]['content'].append(dir_name)

            root_content = []

            # Add files
            for f in f_names:
                full_path = os.path.join(root, f)
                relative_path = os.path.join(relative_root, f)
                file_stat = get_stat(full_path)

                if file_stat is not None:
                    root_content.append('/' + f)
                    file_db[relative_path] = file_stat

            root_stat["content"] = root_content

        file_db[relative_root] = root_stat


def delete_dir_from_db(relative_dir_path):
    global file_db

    relative_dir_path = "/" + relative_dir_path.strip("/")
    parent_dir = os.path.dirname(relative_dir_path)
    dir_name = '/' + os.path.basename(relative_dir_path)

    if file_db[relative_dir_path]["type"] != 'dir':
        print("ERROR: {} is not a directory.".format(relative_dir_path))
        return

    # Delete children
    try:
        for child_name in file_db[relative_dir_path]["content"]:
            child_relative_path = os.path.join(relative_dir_path, child_name.strip('/'))
            if file_db[child_relative_path]["type"] == 'dir':
                delete_dir_from_db(child_relative_path)
            else:
                try:
                    del file_db[child_relative_path]
                except Exception as ef:
                    print("ERROR deleting file from parent:\n\t{}".format(ef))
    except Exception as ed:
        print("ERROR listing children:\n\t{}".format(ed))

    # Delete self from parent
    try:
        file_db[parent_dir]['content'].remove(dir_name)
    except Exception as e:
        print("ERROR removing {} from database:\n\t{}".format(dir_name, e))

    # Finally delete self from database
    del file_db[relative_dir_path]


def build_file_list():
    global file_db

    # Add root dir
    root_stat = get_stat(ROOT)
    file_db['/'] = root_stat

    for obj in os.listdir(ROOT):
        full_path = os.path.join(ROOT, obj)
        if os.path.isdir(full_path):
            add_dir_to_db(obj)
        else:
            file_stat = get_stat(full_path)

            if file_stat is not None:
                file_db['/' + obj] = file_stat
                root_content = file_db['/'].get('content', [])
                root_content.append('/' + obj)
                file_db['/']['content'] = root_content


def check_args():
    global args
    global ROOT
    global MODE
    global PORT
    global HOST

    ROOT = args.root.replace("'", "")
    if not os.path.isdir(ROOT):
        sys.exit("ERROR: Root dir not found: {}".format(ROOT))

    HOST = args.host.replace("'", "")
    MODE = args.mode.replace("'", "")
    if 'client' in args.mode.replace("'", ""):
        MODE = 'client'
        print("Running in client mode.")
        if len(HOST) < 5:
            sys.exit("ERROR: please specify a valid host to connect to.")
    else:
        MODE = 'server'
        print("Running in server mode.")

    PORT = args.port
    try:
        PORT = int(args.port)
        if PORT < 80:
            sys.exit("ERROR, Please specify a port higher than 80.")
    except Exception as e:
        sys.exit("ERROR: {}, \n\tPlease specify a port higher than 80.".format(e))


def handle_event(event_data):
    global event_queue
    global file_db

    event_queue.append(event_data)

    if event_data["obj_type"] == 'dir':
        # Working with a dir
        if event_data["ev_type"] == "DELETE":
            delete_dir_from_db(event_data["path"])
        else:
            add_dir_to_db(event_data["path"])
    else:
        # Working with a file
        parent_dir = '/' + os.path.dirname(event_data["path"]).strip("/")
        file_name = '/' + os.path.basename(event_data["path"])

        if event_data["ev_type"] == "DELETE":
            del file_db[event_data["path"]]

            # delete self from parent dir
            try:
                file_db[parent_dir]['content'].remove(file_name)
            except Exception as e:
                print("ERROR removing {} form database:\n\t{}".format(file_name, e))
        else:
            # Add self to parent
            full_path = os.path.join(ROOT, event_data["path"])
            file_stat = get_stat(full_path)
            file_relative_path = '/' + event_data["path"].strip('/')

            if file_stat is not None:
                file_db[file_relative_path] = file_stat
                if file_name not in file_db[parent_dir]['content']:
                    file_db[parent_dir]['content'].append(file_name)


def parse_event(ev_type, ev_path, ev_file_name):
    """ Processes file system change events received from kernel

        Calls handle_event function using an event object formatted as a dictionary containing:
        ev_type can be either "CREATED" or "DELETED"
        obj_type can be either "file" or "folder"
        path is a path relative to the ROOT directory stripped of leading and trailing "/"
    """

    global event_queue

    obj_event_type = None
    obj_type = 'file'

    if len(ev_file_name) > 0:
        for event_type in ev_type:
            if 'ISDIR' in event_type:
                obj_type = 'dir'

            if "CREATE" in event_type or "MOVED_TO" in event_type:
                obj_event_type = "CREATE"
            elif "DELETE" in event_type or "MOVED_FROM" in event_type:
                obj_event_type = "DELETE"

    full_path = os.path.join(ev_path, ev_file_name)
    relative_path = full_path[len(ROOT):].strip('/')

    if obj_event_type is not None:
        event_data = {"ev_type": obj_event_type, "obj_type": obj_type, "path": relative_path, "time": time.time()}
        handle_event(event_data)


def file_system_event_watcher():
    while True:
        for event in watcher.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event
            parse_event(type_names, path, filename)


# Websocket communication
async def get_events(websocket, path):
    request = await websocket.recv()
    print("Rx:", request)

    response = "New " + request
    print("Tx:", response)

    await websocket.send(response)


def ws_server():
    start_server = websockets.serve(get_events, "0.0.0.0", PORT)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--root", help="Folder to monitor.", required=True, type=ascii)
parser.add_argument("--mode", help="Operational mode (server or client).", required=True, type=ascii)
parser.add_argument("--port", help="Communication port.", required=False, default="1313", type=int)
parser.add_argument("--host", help="If running in client mode, IP or URL to host.", required=False, type=ascii, default="")
args = parser.parse_args()
check_args()

print("Watching {}".format(ROOT))
event_mask = (inotify.constants.IN_CREATE
              | inotify.constants.IN_MOVED_TO
              | inotify.constants.IN_MOVED_FROM
              | inotify.constants.IN_DELETE)
watcher = inotify.adapters.InotifyTree(ROOT, mask=event_mask)

print("Building file list. Please wait.")
build_file_list()

t_fs_watcher = threading.Thread(target=file_system_event_watcher)
t_fs_watcher.daemon = True
t_fs_watcher.start()


ws_server()
