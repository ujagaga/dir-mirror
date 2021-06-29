#!/usr/bin/python3

import os
import hashlib
import argparse
import sys
import inotify.adapters
import inotify.constants
import time
import sqlite3
import threading
import json
import socket
import shutil

DATABASE = None
db = None
ROOT = "/"
MODE = None
PORT = None
HOST = None
EVENT_LIFETIME = 6 * 60 * 60        # 6 hours event lifetime. After this it is deleted from the database
last_event_cleanup_time = 0
new_file_temp_path = ''
new_file_path = ''
new_file_mtime = ''
new_file_size = 0
new_file_hash = ''


def calc_hash(file_path):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)

    return file_hash.hexdigest()


# Database basic functions
def db_open():
    global db

    db = sqlite3.connect(DATABASE)


def query_db(query, args=(), one=False):
    cur = db.execute(query, args)
    rv = [dict((cur.description[idx][0], value)
               for idx, value in enumerate(row)) for row in cur.fetchall()]
    return (rv[0] if rv else None) if one else rv


def exec_db(query):
    db.execute(query)
    if not query.startswith('SELECT'):
        db.commit()


def init_database():
    global DATABASE
    global db

    db_dir = os.path.join(ROOT, '.dir-mirror')
    if not os.path.isdir(db_dir):
        os.mkdir(db_dir)

    DATABASE = os.path.join(db_dir, "database.db")
    if os.path.exists(DATABASE):
        os.remove(DATABASE)

    # Create an empty database
    db = sqlite3.connect(DATABASE)

    # Create an event queue table
    sql = "create table events (id INTEGER PRIMARY KEY AUTOINCREMENT, ev_type TEXT, obj_type TEXT, path TEXT, " \
          ", hash TEXT, time INTEGER)"
    db.execute(sql)

    # Create a file and folder list table
    sql = "create table files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT, type TEXT, mtime INTEGER, " \
          "size INTEGER, hash TEXT, content TEXT)"
    db.execute(sql)

    # Save changes and close
    db.commit()
    db.close()


def add_event_to_db(event):
    """ Adds events to database.
        event to add in format:
            {'ev_type': <CREATE or DELETE>, 'obj_type': <file or dir>, 'path': <object path relative to ROOT>}
    """
    global db

    db_open()

    try:
        sql = "INSERT INTO events (ev_type, obj_type, path, hash, time) VALUES ('{}', '{}', '{}', '{}', '{}')" \
              "".format(event['ev_type'], event['obj_type'], event['path'], event['hash'], int(time.time()))
        db.execute(sql)
        db.commit()
    except Exception as e:
        print("ERROR writing events to db: {}".format(e))

    db.close()


def get_events_from_db(from_time):
    """ Retrieves all events from database that happened after specified timestamp. """
    global db

    sql = "SELECT * FROM events WHERE time > '{}' ORDER BY time".format(int(from_time))
    db_open()
    result = query_db(sql, one=True)
    db.close()

    return result


def cleanup_events_from_db():
    global db
    global EVENT_LIFETIME
    global last_event_cleanup_time

    if (time.time() - last_event_cleanup_time) > (60 * 60):
        # Only do the cleanup every hour to avoid calling database too often
        oldest_acceptable_time = time.time() - EVENT_LIFETIME

        sql = "DELETE FROM events WHERE time < {}".format(oldest_acceptable_time)
        db_open()
        db.execute(sql)
        db.commit()
        db.close()

        last_event_cleanup_time = time.time()


def get_file_from_db(path):
    global db

    sql = "SELECT * FROM files WHERE path='{}'".format(path)
    db_open()
    result = query_db(sql, one=True)
    db.close()

    return result


def get_all_files_from_db():
    global db

    sql = "SELECT * FROM files"
    db_open()
    result = query_db(sql)
    db.close()

    return result


def update_file_in_db(path, type, mtime, size, hash):
    """ Adds a new or updates existing file of folder in database and updates its parent content

        path: <path relative to ROOT>,
        type: <file or dir>,
        mtime: <modify time>,
        size: <size in bytes if it is a file>,
        hash: <file hash if it is a file>,
    """
    global db

    db_open()

    try:
        # Add requested file
        sql = "SELECT id FROM files WHERE path='{}'".format(path)
        result = query_db(sql, one=True)

        if result is None:
            sql = "INSERT INTO files (path, type, mtime, size, hash, content) VALUES " \
                  "('{}', '{}', '{}', '{}', '{}', '{}')".format(path, type, mtime, size, hash, '[]')
        else:
            sql = "UPDATE files SET path='{}', type='{}', mtime='{}', size='{}', hash='{}'" \
                  "WHERE id='{}'".format(path, type, mtime, size, hash, result['id'])

        db.execute(sql)

        if path != '/':
            # Find parent dir
            parent_path = os.path.dirname(path)
            obj_name = '/' + os.path.basename(path)
            sql = "SELECT * FROM files WHERE path='{}'".format(parent_path)
            parent = query_db(sql, one=True)
            if parent is not None:
                # Update parent content
                content = json.loads(parent['content'])
                if obj_name not in content:
                    content.append(obj_name)
                    sql = "UPDATE files SET content='{}' WHERE path='{}'".format(json.dumps(content), parent_path)
                    db.execute(sql)

        db.commit()

    except Exception as e:
        print("ERROR writing files to db: {}".format(e))

    db.close()


def remove_file_from_db(path):
    """ Removes a file or folder from database and from it's parent content

        path is a path to the file or folder to remove
    """
    global db

    db_open()
    try:
        if path != '/':
            # Find parent dir
            parent_path = os.path.dirname(path)
            obj_name = '/' + os.path.basename(path)
            sql = "SELECT * FROM files WHERE path='{}'".format(parent_path)
            parent = query_db(sql, one=True)
            if parent is not None:
                # Delete file form parent content
                content = json.loads(parent['content'])
                content.remove(obj_name)
                sql = "UPDATE files SET content='{}' WHERE path='{}'".format(json.dumps(content), parent_path)
                db.execute(sql)

            # Delete the requested entry
            sql = "DELETE FROM files WHERE path = '{}'".format(path)
            db.execute(sql)
            db.commit()

    except Exception as e:
        print("ERROR removing files from db: {}".format(e))

    db.close()


def get_stat(file_path):
    info = None
    try:
        fhash = ""
        fsize = ""
        mtime = ""

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


def handle_obj_add(relative_path):
    full_obj_path = os.path.join(ROOT, relative_path.strip("/"))
    obj_stat = get_stat(full_obj_path)

    update_file_in_db(relative_path, obj_stat['type'], obj_stat['mtime'], obj_stat['size'], obj_stat['hash'])

    if obj_stat['type'] == 'dir':
        # Add children to self content
        for root, d_names, f_names in os.walk(full_obj_path):
            # Add folder to self content
            relative_root = '/' + root[len(ROOT):].strip('/')
            root_stat = get_stat(root)

            if root_stat is not None and '.dir-mirror' not in relative_root:
                update_file_in_db(relative_root, root_stat['type'], root_stat['mtime'], root_stat['size'], root_stat['hash'])

                # Add files
                for f in f_names:
                    full_path = os.path.join(root, f)
                    relative_path = os.path.join(relative_root, f)
                    file_stat = get_stat(full_path)

                    if file_stat is not None:
                        update_file_in_db(relative_path, file_stat['type'], file_stat['mtime'], file_stat['size'], file_stat['hash'])

    return obj_stat['hash']


def handle_obj_delete(relative_path):
    # Get the dir object from db
    obj = get_file_from_db(relative_path)

    if obj['type'] == 'dir':
        obj_content = json.loads(obj['content'])

        # Delete children
        try:
            for child_name in obj_content:
                child_relative_path = os.path.join(relative_path, child_name.strip('/'))

                # get child
                child = get_file_from_db(child_relative_path)

                if child["type"] == 'dir':
                    handle_obj_delete(child_relative_path)
                else:
                    remove_file_from_db(child_relative_path)

        except Exception as ed:
            print("ERROR listing children:\n\t{}".format(ed))

    # Finally delete self from database
    remove_file_from_db(relative_path)

    return obj['hash']


def check_args():
    """ Verifies input arguments """
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


def parse_event(ev_type, ev_path, ev_file_name):
    """ Processes file system change events received from kernel

        Calls handle_event function using an event object formatted as a dictionary containing:
        ev_type can be either "CREATED" or "DELETED"
        obj_type can be either "file" or "folder"
        path is a path relative to the ROOT directory.
    """
    # print("EVENT:", ev_type, ev_path, ev_file_name)

    cleanup_events_from_db()

    obj_event_type = None
    obj_type = 'file'

    if len(ev_file_name) > 0 and '.dir-mirror' not in ev_path and not ev_file_name.startswith('.'):
        for event_type in ev_type:
            if 'ISDIR' in event_type:
                obj_type = 'dir'

            if "CREATE" in event_type or "MOVED_TO" in event_type:
                obj_event_type = "CREATE"
            elif "DELETE" in event_type or "MOVED_FROM" in event_type:
                obj_event_type = "DELETE"

        full_path = os.path.join(ev_path, ev_file_name)
        relative_path = '/' + full_path[len(ROOT):].strip('/')

        if obj_event_type is not None:
            if obj_event_type == "DELETE":
                hash = handle_obj_delete(relative_path)
            else:
                hash = handle_obj_add(relative_path)

            event_data = {"ev_type": obj_event_type, "obj_type": obj_type, "path": relative_path, "hash": hash}
            add_event_to_db(event_data)


def file_system_event_watcher():
    """ Receives file system change events from kernel

        Passes the events to parser for processing.
    """
    while True:
        for event in watcher.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event
            parse_event(type_names, path, filename)


def parse_client_request(request):
    global new_file_temp_path
    global new_file_path
    global new_file_size
    global new_file_mtime
    global new_file_hash

    print("Parsing request: {}".format(request), flush=True)

    response_data = {"code": "OK"}

    if 'request' in request.keys():
        if request['request'] == 'get_files':
            response_data['response'] = get_all_files_from_db()
            response = json.dumps(response_data)
            yield response

        elif request['request'] == 'get_file_content':
            if 'path' in request.keys():
                full_path = os.path.join(ROOT, request['path'].strip('/'))
                if os.path.isfile(full_path):
                    file_data = get_file_from_db(request['path'])

                    response_msg = {'path': file_data['path'], 'size': file_data['size'], 'hash': file_data['hash'],
                                    'mtime': file_data['mtime']}
                    response = json.dumps(response_msg) + '\n'
                    yield response

                    f = open(full_path, 'rb')
                    while True:
                        data = f.read(1024)
                        if not data:
                            break
                        yield data
                    f.close()

            else:
                response_data['code'] = "ERROR"
                response_data['response'] = "Unsupported request"
                response = json.dumps(response_data)
                yield response

        elif request['request'] == 'update_file':
            if 'path' in request.keys():
                file_name = os.path.basename(request['path'])
                old_file_full_path = os.path.join(ROOT, request['path'].strip('/'))
                new_file_temp_path = os.path.join(ROOT, '.dir-mirror', file_name)

                if os.path.isfile(old_file_full_path):
                    file_data = get_file_from_db(request['path'])
                    current_mtime = file_data['mtime']
                else:
                    current_mtime = 0

                if 'mtime' in request.keys():
                    new_file_mtime = request['mtime']
                else:
                    new_file_mtime = -1

                if current_mtime < new_file_mtime:
                    # New file is newer or old file does not exist. Receive it.
                    new_file_path = request['path']
                    new_file_size = request['size']
                    new_file_hash = request['hash']

                    response_data['code'] = "OK"
                    response_data['response'] = ""
                    response = json.dumps(response_data)
                    yield response
                else:
                    new_file_size = 0
                    response_data['code'] = "ERROR"
                    response_data['response'] = "Existing file newer."
                    response = json.dumps(response_data)
                    yield response

            else:
                response_data['code'] = "ERROR"
                response_data['response'] = "Unsupported request"
                response = json.dumps(response_data)
                yield response

        elif request['request'] == 'get_events':
            if 'time' in request.keys():
                timestamp = request['time']
            else:
                timestamp = 0
            response_data['response'] = json.dumps(get_events_from_db(timestamp))
            response = json.dumps(response_data)
            yield response

        elif request['request'] == 'set_events':
            if 'events' in request.keys():
                try:
                    # INSERT INTO events (ev_type, obj_type, path, hash, time
                    for event in request['events']:
                        if event['ev_type'] == 'DELETE':
                            if 'path' in event.keys():
                                full_path = os.path.join(ROOT, event['path'].strip('/'))
                                if os.path.isfile(full_path):
                                    file_data = get_file_from_db(event['path'])

                            else:
                                response_data = {'code': "ERROR", 'response': "Error parsing event. No path specified"}
                                response = json.dumps(response_data)
                                yield response
                        else:
                            response_data = {'code': "ERROR", 'response': "Unsupported event: {}".format(event['ev_type'])}
                            response = json.dumps(response_data)
                            yield response

                except Exception as e:
                    print("ERROR: could not parse events\n\t{}".format(e))
                    response_data = {'code': "ERROR", 'response': "could not parse events."}
                    response = json.dumps(response_data)
                    yield response

        else:
            response_data['code'] = "ERROR"
            response_data['response'] = "Unknown request"
            response = json.dumps(response_data)
            yield response

    else:
        response_data['code'] = "ERROR"
        response_data['response'] = "No request specified"
        response = json.dumps(response_data)
        yield response


def new_file_receive(raw_data):
    global new_file_temp_path
    global new_file_path
    global new_file_size
    global new_file_mtime
    global new_file_hash

    f = open(new_file_temp_path, 'rb')
    f.write(raw_data)
    f.close()

    new_file_size -= raw_data

    response_data = {'code': "OK", 'response': ""}
    if new_file_size <= 0:
        # Writing done. Check hash.
        hash = calc_hash(new_file_temp_path)
        if hash != new_file_hash:
            response_data = {'code': "ERROR", 'response': "bad hash"}
        else:
            old_file_full_path = os.path.join(ROOT, new_file_path.strip('/'))

            if os.path.isfile(old_file_full_path):
                file_data = get_file_from_db(new_file_path)
                current_mtime = file_data['mtime']
            else:
                current_mtime = 0

            if current_mtime < new_file_mtime:
                # New file is newer or old file does not exist. Move it to specified location
                os.makedirs(os.path.dirname(old_file_full_path))
                shutil.move(new_file_temp_path, old_file_full_path)
                response_data = {'code': "OK", 'response': "Done"}
            else:
                response_data = {'code': "ERROR", 'response': "Old file is newer"}

    return json.dumps(response_data)


def socket_server():
    print("Listening for requests on port {}\n".format(PORT), flush=True)

    # Open the server socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", PORT))
        s.listen(1)     # Limit to one client at a time to avoid race conditions
        while True:
            # Wait for a connection from a client
            conn, addr = s.accept()
            with conn:
                # Client is connected. Receive a message.
                msg = ''
                while True:
                    data = conn.recv(1024)

                    if not data:
                        break
                    else:
                        if new_file_size > 0:
                            answer = new_file_receive(data)
                            conn.sendall(answer.encode())
                        else:
                            msg += data.decode()
                            if msg.endswith('\n'):
                                break

                # Parse the received message and send response
                try:
                    if len(msg) > 5:
                        request = json.loads(msg)
                        for answer in parse_client_request(request):
                            conn.sendall(answer.encode())
                            
                except Exception as e:
                    print("ERROR parsing message: {}:\n\t{}".format(msg, e))
                    conn.sendall("ERROR parsing message".encode())


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--root", help="Folder to monitor.", required=True, type=ascii)
parser.add_argument("--mode", help="Operational mode (server or client).", required=True, type=ascii)
parser.add_argument("--port", help="Communication port.", required=False, default="1313", type=int)
parser.add_argument("--host", help="If running in client mode, IP or URL to host.", required=False, type=ascii, default="")
args = parser.parse_args()
check_args()
init_database()

print("Watching {}".format(ROOT))
event_mask = (inotify.constants.IN_CREATE
              | inotify.constants.IN_MOVED_TO
              | inotify.constants.IN_MOVED_FROM
              | inotify.constants.IN_DELETE)
watcher = inotify.adapters.InotifyTree(ROOT, mask=event_mask)

print("Building file list. Please wait.")
handle_obj_add("/")

t_fs_watcher = threading.Thread(target=file_system_event_watcher)
t_fs_watcher.daemon = True
t_fs_watcher.start()

try:
    if MODE == 'server':
        socket_server()
    else:
        pass
        # ws_client()
finally:
    if os.path.exists(DATABASE):
        print("Deleting database:{}".format(DATABASE))
        os.remove(DATABASE)
