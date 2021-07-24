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
MAX_CLIENT_SYNC_TIMEOUT = 60
client_sync_timeout = 10
EXIT_FLAG = False
watcher = None
t_fs_watcher = None
EVENT_IDLE_TIMEOUT = 20  # Minimum time to consider events to be idle.


def calc_hash(file_path):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)

    return file_hash.hexdigest()


def query_db(db, query, args=(), one=False):
    cur = db.execute(query, args)
    rv = [dict((cur.description[idx][0], value)
               for idx, value in enumerate(row)) for row in cur.fetchall()]
    return (rv[0] if rv else None) if one else rv


def init_database():
    global DATABASE

    db_dir = os.path.join(ROOT, '.dir-mirror')
    if not os.path.isdir(db_dir):
        os.mkdir(db_dir)

    DATABASE = os.path.join(db_dir, "database.db")
    # Create an empty database or connect to an existing one
    db = sqlite3.connect(DATABASE)

    try:
        if get_event_count_from_db() > 0:
            print("Clearing old events from the database.")
            db = sqlite3.connect(DATABASE)
            sql = "DELETE FROM events"
            db.execute(sql)
            db.commit()
    except:
        # Create an event queue table
        sql = "create table events (id INTEGER PRIMARY KEY AUTOINCREMENT, ev_type TEXT, obj_type TEXT, path TEXT, " \
              "hash TEXT, time INTEGER)"
        db.execute(sql)
        db.commit()

        # Create a file and folder list table
        sql = "create table files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT, obj_type TEXT, mtime INTEGER, " \
              "size INTEGER, hash TEXT, content TEXT)"
        db.execute(sql)
        db.commit()

        # Create a variable list table so we can store arbitrary information
        sql = "create table vars (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, value TEXT, time INTEGER)"
        db.execute(sql)
        db.commit()

    db.close()


def add_var_to_db(name, value=""):
    db = sqlite3.connect(DATABASE)

    try:
        sql = "INSERT INTO vars (name, value, time) VALUES ('{}', '{}', '{}')".format(name, value, int(time.time()))
        db.execute(sql)
        db.commit()
    except Exception as e:
        print("ERROR writing variable to db: {}".format(e))

    db.close()


def get_var_from_db(name):
    sql = "SELECT * FROM vars WHERE name = '{}'".format(name)
    db = sqlite3.connect(DATABASE)
    result = query_db(db, sql, one=True)
    db.close()

    return result


def add_event_to_db(event):
    """ Adds events to database.
        event to add in format:
            {'ev_type': <CREATE or DELETE>, 'obj_type': <file or dir>, 'path': <object path relative to ROOT>}
    """
    db = sqlite3.connect(DATABASE)

    try:
        sql = "INSERT INTO events (ev_type, obj_type, path, hash, time) VALUES ('{}', '{}', '{}', '{}', '{}')" \
              "".format(event['ev_type'], event['obj_type'], event['path'], event['hash'], int(time.time()))
        db.execute(sql)

        # Set last event time
        sql = "INSERT INTO vars (name, value, time) VALUES ('{}', '{}', '{}')".format("ev_time", "", int(time.time()))
        db.execute(sql)

        db.commit()
    except Exception as e:
        print("ERROR writing events to db: {}".format(e))

    db.close()


def get_events_from_db(from_time):
    """ Retrieves all events from database that happened after specified timestamp. """

    sql = "SELECT * FROM events WHERE time > '{}' ORDER BY time".format(int(from_time))
    db = sqlite3.connect(DATABASE)
    result = query_db(db, sql)
    db.close()

    return result


def cleanup_events_from_db():
    global EVENT_LIFETIME
    global last_event_cleanup_time

    if (time.time() - last_event_cleanup_time) > (60 * 60):
        # Only do the cleanup every hour to avoid calling database too often
        oldest_acceptable_time = time.time() - EVENT_LIFETIME

        sql = "DELETE FROM events WHERE time < {}".format(oldest_acceptable_time)
        db = sqlite3.connect(DATABASE)
        db.execute(sql)
        db.commit()
        db.close()

        last_event_cleanup_time = time.time()


def get_file_from_db(path):
    sql = "SELECT * FROM files WHERE path='{}'".format(path)
    db = sqlite3.connect(DATABASE)
    result = query_db(db, sql, one=True)
    db.close()

    return result


def get_file_count_from_db():
    sql = "SELECT * FROM files"
    db = sqlite3.connect(DATABASE)
    result = query_db(db, sql)
    db.close()

    return len(result)


def get_event_count_from_db():
    sql = "SELECT * FROM events"
    db = sqlite3.connect(DATABASE)
    result = query_db(db, sql)
    db.close()

    return len(result)


def get_all_files_from_db(dir_path=None):
    sql = "SELECT * FROM files"
    if dir_path is not None:
        sql += " WHERE path LIKE '{}'".format(dir_path)

    db = sqlite3.connect(DATABASE)
    result = query_db(db, sql)
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
    db = sqlite3.connect(DATABASE)

    try:
        # Add requested file
        sql = "SELECT id FROM files WHERE path='{}'".format(path)
        result = query_db(db, sql, one=True)

        if result is None:
            sql = "INSERT INTO files (path, obj_type, mtime, size, hash, content) VALUES " \
                  "('{}', '{}', '{}', '{}', '{}', '{}')".format(path, type, mtime, size, hash, '[]')
        else:
            sql = "UPDATE files SET path='{}', obj_type='{}', mtime='{}', size='{}', hash='{}'" \
                  "WHERE id='{}'".format(path, type, mtime, size, hash, result['id'])

        db.execute(sql)

        if path != '/':
            # Find parent dir
            parent_path = os.path.dirname(path)
            obj_name = '/' + os.path.basename(path)
            sql = "SELECT * FROM files WHERE path='{}'".format(parent_path)
            parent = query_db(db, sql, one=True)
            if parent is not None:
                # Update parent content
                content = json.loads(parent['content'])
                if obj_name not in content:
                    content.append(obj_name)
                    sql = "UPDATE files SET content='{}' WHERE path='{}'".format(json.dumps(content), parent_path)
                    db.execute(sql)

        db.commit()

    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR writing files to db on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))

    db.close()


def add_missing_path_to_db(relative_path):
    full_path = os.path.join(ROOT, relative_path.strip('/'))
    obj_stat = get_stat(full_path)
    if obj_stat is not None:
        update_file_in_db(relative_path, obj_stat['obj_type'], obj_stat['mtime'], obj_stat['size'], obj_stat['hash'])


def remove_file_from_db(path):
    """ Removes a file or folder from database and from it's parent content

        path is a path to the file or folder to remove
    """

    db = sqlite3.connect(DATABASE)
    try:
        if path != '/':
            # Find parent dir
            parent_path = os.path.dirname(path)
            obj_name = '/' + os.path.basename(path)
            sql = "SELECT * FROM files WHERE path='{}'".format(parent_path)
            parent = query_db(db, sql, one=True)
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

    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR removing files from db on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))

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
            info = {'obj_type': ftype, 'mtime': mtime, 'size': fsize, 'hash': fhash}
        else:
            print("Warning, Unsupported file:{}".format(file_path))

    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR getting stats on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))

    return info


def handle_obj_add(relative_path):
    """ When creating objects, entire content and paths need to be noted in the file database,
        but in event database, we only need events for creating files. Directories will be created consequently.
    """
    full_obj_path = os.path.join(ROOT, relative_path.strip("/"))
    obj_stat = get_stat(full_obj_path)

    if obj_stat is not None:
        update_file_in_db(relative_path, obj_stat['obj_type'], obj_stat['mtime'], obj_stat['size'], obj_stat['hash'])

        if obj_stat['obj_type'] == 'dir':
            # Add children to self content
            for root, d_names, f_names in os.walk(full_obj_path):
                # Add folder to self content
                relative_root = '/' + root[len(ROOT):].strip('/')
                root_stat = get_stat(root)

                if root_stat is not None and '.dir-mirror' not in relative_root:
                    update_file_in_db(relative_root, root_stat['obj_type'], root_stat['mtime'], root_stat['size'], root_stat['hash'])

                    # Add files
                    for f in f_names:
                        full_path = os.path.join(root, f)
                        relative_path = os.path.join(relative_root, f)
                        file_stat = get_stat(full_path)

                        if file_stat is not None:
                            update_file_in_db(relative_path, file_stat['obj_type'], file_stat['mtime'], file_stat['size'], file_stat['hash'])
                            # Add file create event
                            event_data = {"ev_type": "CREATE", "obj_type": file_stat['obj_type'], "path": relative_path, "hash": file_stat['hash']}
                            add_event_to_db(event_data)

        else:
            # Add file create event
            event_data = {"ev_type": "CREATE", "obj_type": obj_stat['obj_type'], "path": relative_path, "hash": obj_stat['hash']}
            add_event_to_db(event_data)


def handle_obj_delete(relative_path):
    # Get the dir object from db
    obj = get_file_from_db(relative_path)

    if obj is not None:
        # When deleting a directory, we only need event for whole directory, not the children, as they will get deleted consequently.
        event_data = {"ev_type": "DELETE", "obj_type": obj['obj_type'], "path": relative_path, "hash": obj['hash']}
        add_event_to_db(event_data)

        if obj['obj_type'] == 'dir':
            obj_content = json.loads(obj['content'])

            # Delete children
            try:
                for child_name in obj_content:
                    child_relative_path = os.path.join(relative_path, child_name.strip('/'))

                    # get child
                    child = get_file_from_db(child_relative_path)

                    if child['obj_type'] == 'dir':
                        handle_obj_delete(child_relative_path)
                    else:
                        remove_file_from_db(child_relative_path)

            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print("ERROR listing children on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))

        # Finally delete self from database
        remove_file_from_db(relative_path)


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
    except Exception as exc:
        sys.exit("ERROR: {}, \n\tPlease specify a port higher than 80.".format(exc))


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

    if len(ev_file_name) > 0 and '.dir-mirror' not in ev_path and not ev_file_name.startswith('.'):
        for event_type in ev_type:
            if "CREATE" in event_type or "MOVED_TO" in event_type or "MODIFY" in event_type:
                obj_event_type = "CREATE"
            elif "DELETE" in event_type or "MOVED_FROM" in event_type:
                obj_event_type = "DELETE"

        full_path = os.path.join(ev_path, ev_file_name)
        relative_path = '/' + full_path[len(ROOT):].strip('/')

        if obj_event_type is not None:
            if obj_event_type == "DELETE":
                handle_obj_delete(relative_path)
            else:
                handle_obj_add(relative_path)


def file_system_event_watcher():
    """ Receives file system change events from kernel

        Passes the events to parser for processing.
    """
    global EXIT_FLAG

    # while not EXIT_FLAG:
    try:
        for event in watcher.event_gen(yield_nones=False):
            (_, type_names, path, filename) = event
            parse_event(type_names, path, filename)

    except Exception as e:
        # The watcher caused an error, lost track of events. Best re-start the whole app.
        # We do this by exiting and leave it to the startup script to restart
        print("Watcher error.Exiting.")
        EXIT_FLAG = True


def parse_client_request(request):
    global new_file_temp_path
    global new_file_path
    global new_file_size
    global new_file_mtime
    global new_file_hash

    print("Parsing request: {}".format(request), flush=True)

    response_data = {"code": "OK"}

    if 'request' in request.keys():
        if request['request'] == 'get_file':
            if 'path' in request.keys():
                full_path = os.path.join(ROOT, request['path'].strip('/'))
                if os.path.isfile(full_path):
                    file_data = get_file_from_db(request['path'])
                    if file_data is None:
                        # File exists but is not added to DB. Add it now.
                        add_missing_path_to_db(request['path'])
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

                    response_data = None
                elif os.path.isdir(full_path):
                    response_data['response'] = get_all_files_from_db(request['path'])
                else:
                    response_data = {'code': "ERROR", 'response': "No such file: {}".format(request['path'])}
            else:
                response_data['response'] = get_all_files_from_db()

        elif request['request'] == 'update_file':
            if 'path' in request.keys() and 'obj_type' in request.keys():
                if request['obj_type'] == 'file':
                    file_name = os.path.basename(request['path'])
                    old_file_full_path = os.path.join(ROOT, request['path'].strip('/'))
                    new_file_temp_path = os.path.join(ROOT, '.dir-mirror', file_name)

                    done_processing = False
                    current_mtime = 0
                    if os.path.isfile(old_file_full_path):
                        file_data = get_file_from_db(request['path'])
                        if file_data is None:
                            add_missing_path_to_db(request['path'])
                            file_data = get_file_from_db(request['path'])

                        current_mtime = file_data['mtime']
                        if request.get('hash', '') == file_data['hash']:
                            response_data = {'code': "ERROR", 'response': "Same file already exists"}
                            done_processing = True

                    if not done_processing:
                        if 'mtime' in request.keys():
                            try:
                                new_file_mtime = int(request['mtime'])
                            except:
                                new_file_mtime = -1
                        else:
                            new_file_mtime = -1

                        if current_mtime < new_file_mtime:
                            # New file is newer or old file does not exist. Receive it.
                            new_file_path = request['path']
                            new_file_size = request['size']
                            new_file_hash = request['hash']

                            response_data = {'code': "OK", 'response': ""}
                        else:
                            new_file_size = 0
                            response_data = {'code': "ERROR", 'response': "Existing file newer."}
                else:
                    response_data = {'code': "ERROR", 'response': "Unsupported object type: {}".format(request['obj_type'])}
            else:
                response_data = {'code': "ERROR", 'response': "Unsupported request"}

        elif request['request'] == 'get_events':
            if 'time' in request.keys():
                timestamp = request['time']
            else:
                timestamp = 0

            event_time_var = get_var_from_db("ev_time")
            event_time = event_time_var.get("time", 0)
            print("event_time:", event_time)

            if (time.time() - event_time) < EVENT_IDLE_TIMEOUT:
                # Last event time is too recent. Files could be coming in still.
                response_data['response'] = "Events in progress"
                response_data['code'] = "ERROR"
            else:
                response_data['response'] = json.dumps(get_events_from_db(timestamp))
                response_data['file_count'] = get_file_count_from_db()

        elif request['request'] == 'set_events':
            response_data = {'code': "ERROR", 'response': "Unknown request"}

            if 'events' in request.keys():
                try:
                    for event in request['events']:
                        if event['ev_type'] == 'DELETE':
                            if 'path' in event.keys():
                                full_path = os.path.join(ROOT, event['path'].strip('/'))
                                file_data = get_file_from_db(event['path'])
                                if os.path.exists(full_path):
                                    if file_data is None:
                                        # File exists but is not added to DB. Add it now.
                                        add_missing_path_to_db(request['path'])
                                        file_data = get_file_from_db(request['path'])

                                    do_delete_flag = True
                                    if len(file_data) > 0:
                                        if file_data['hash'] != event['hash']:
                                            response_data = {'code': "ERROR",
                                                             'response': "Not same file: {}".format(event['path'])}
                                            do_delete_flag = False

                                    if do_delete_flag:
                                        if os.path.isfile(full_path):
                                            os.remove(full_path)
                                            response_data = {'code': "OK", 'response': ""}
                                        elif os.path.isdir(full_path):
                                            shutil.rmtree(full_path)
                                            response_data = {'code': "OK", 'response': ""}
                                        else:
                                            response_data = {'code': "ERROR",
                                                             'response': "Unsupported file type: {}".format(
                                                                 event['path'])}
                                else:
                                    response_data = {'code': "ERROR",
                                                     'response': "No such file: {}".format(event['path'])}
                            else:
                                response_data = {'code': "ERROR", 'response': "Error parsing event. No path specified"}
                        else:
                            response_data = {'code': "ERROR",
                                             'response': "Unsupported event: {}".format(event['ev_type'])}

                except Exception as exc:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print("ERROR could not parse events on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))
                    response_data = {'code': "ERROR", 'response': "could not parse events."}
        else:
            response_data = {'code': "ERROR", 'response': "Unknown request"}
    else:
        response_data = {'code': "ERROR", 'response': "No request specified"}

    if response_data is not None:
        response = json.dumps(response_data) + '\n'
        yield response


def socket_server():
    """ Main server loop.

        Receives string data and sending to parser for processing.
        Receives raw data and sends it to file receiver to write it to file system
    """
    global new_file_temp_path
    global new_file_path
    global new_file_size
    global new_file_mtime
    global new_file_hash

    print("Listening for requests on port {}\n".format(PORT), flush=True)

    while not EXIT_FLAG:
        try:
            # Open the server socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", PORT))
                s.listen(1)     # Limit to one client at a time to avoid race conditions
                while not EXIT_FLAG:
                    # Wait for a connection from a client
                    conn, addr = s.accept()
                    with conn:
                        # Client is connected. Receive a message.
                        if new_file_size > 0:
                            print("Receiving new file:", new_file_temp_path)
                            f = open(new_file_temp_path, 'wb')
                            while new_file_size > 0:
                                data = conn.recv(1)
                                if not data:
                                    new_file_size = 0
                                    break

                                else:
                                    f.write(data)
                                    new_file_size -= len(data)

                                    if new_file_size <= 0:
                                        new_file_size = 0
                                        f.close()
                                        f = None

                                        # Writing done. Check hash.
                                        hash = calc_hash(new_file_temp_path)
                                        if hash != new_file_hash:
                                            print("ERROR: bad hash")
                                        else:
                                            old_file_full_path = os.path.join(ROOT, new_file_path.strip('/'))

                                            if os.path.isfile(old_file_full_path):
                                                file_data = get_file_from_db(new_file_path)
                                                current_mtime = file_data['mtime']
                                            else:
                                                current_mtime = 0

                                            if current_mtime < new_file_mtime:

                                                # New file is newer or old file does not exist.
                                                # Move it to specified location
                                                new_dir = os.path.dirname(old_file_full_path)
                                                if not os.path.isdir(new_dir):
                                                    os.makedirs(new_dir)

                                                shutil.move(new_file_temp_path, old_file_full_path)
                                            else:
                                                print("ERROR: Old file is newer")
                                        break
                            if f:
                                f.close()
                        else:
                            msg = ''
                            while not EXIT_FLAG:
                                data = conn.recv(1)
                                if not data:
                                    break
                                else:
                                    msg += data.decode()
                                    if msg.endswith('\n'):
                                        break

                            # Parse the received message and send response
                            try:
                                if len(msg) > 5:
                                    request = json.loads(msg)
                                    for answer in parse_client_request(request):
                                        try:
                                            response = answer.encode()
                                        except:
                                            response = answer

                                        conn.sendall(response)

                            except Exception as exc:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                print("ERROR parsing message on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))
                                conn.sendall("ERROR parsing message".encode())
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("ERROR opening port on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))
            time.sleep(10)


def query_remote_server(msg):
    response = ""

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            request = json.dumps(msg) + "\n"
            s.connect((HOST, PORT))
            s.sendall(request.encode())

            while True:
                data = s.recv(1)
                if data:
                    response += data.decode()
                    if response.endswith('\n'):
                        break
                else:
                    break
    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR communicating with server on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))
        return {}

    try:
        server_response = json.loads(response)
    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR parsing server response {}\n on line {}!\n\t{}".format(response, exc_tb.tb_lineno, exc))
        server_response = {}

    return server_response


def client_fetch_remote_file(relative_path):
    print("client_fetch_remote_file:", relative_path)
    file_name = os.path.basename(relative_path)
    new_file_full_path = os.path.join(ROOT, relative_path.strip('/'))
    temp_path = os.path.join(ROOT, '.dir-mirror', file_name)

    msg = {'request': 'get_file', 'path': relative_path}

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        request = json.dumps(msg) + "\n"
        s.connect((HOST, PORT))
        s.sendall(request.encode())

        response = ''
        while True:
            data = s.recv(1)
            if data:
                response += data.decode()
                if response.endswith('\n'):
                    break
            else:
                break
        print("Server response: {}".format(response))

        f = None
        try:
            file_data = json.loads(response)

            if 'code' in file_data.keys() and file_data.get('code', 'ERROR') == 'ERROR':
                print("ERROR receiving file {}\n Server responded: {}".format(relative_path, response))
            else:
                # Receive file
                size = file_data['size']

                f = open(temp_path, 'wb')
                while True:
                    data = s.recv(1024)
                    if data:
                        f.write(data)
                        size -= len(data)

                        if size <= 0:
                            break
                    else:
                        break

                f.close()

                # Calculate hash
                hash = calc_hash(temp_path)
                if hash != file_data['hash']:
                    print("ERROR receiving file {}\n\tHash does not match".format(relative_path))
                    os.remove(temp_path)
                else:
                    dir_name = os.path.dirname(new_file_full_path)
                    if not os.path.isdir(dir_name):
                        os.makedirs(dir_name)
                    shutil.move(temp_path, new_file_full_path)

        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("ERROR receiving file {}\n on line {}!\n\t{}".format(relative_path, exc_tb.tb_lineno, exc))
            if f is not None:
                f.close()
            os.remove(temp_path)


def client_send_local_file(relative_path):
    print("client_send_local_file: ", relative_path)

    full_path = os.path.join(ROOT, relative_path.strip('/'))

    if os.path.isfile(full_path):
        msg = get_file_from_db(relative_path)
        if msg is None:
            # File exists but is not added to DB. Add it now.
            add_missing_path_to_db(relative_path)
            msg = get_file_from_db(relative_path)

        msg['request'] = 'update_file'

        server_response = {}

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            request = json.dumps(msg) + "\n"
            s.connect((HOST, PORT))
            s.sendall(request.encode())
            response = ''
            while True:
                data = s.recv(1)
                if data:
                    response += data.decode()
                    if response.endswith('\n'):
                        break
                else:
                    break

            server_response = json.loads(response)

        if server_response.get('code', '') == 'OK':
            # Send file
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                f = open(full_path, 'rb')
                s.connect((HOST, PORT))
                data = True
                while data:
                    data = f.read(1024)
                    if data:
                        s.sendall(data)
                f.close()
        else:
            print("Not sending new file. Server says: {}".format(server_response) )


def client_sync_remote_file(remote_file_data):
    print("client_sync_remote_file:", remote_file_data)
    relative_path = remote_file_data['path']
    try:
        # Check if we have the file. Ignore folders as we will create the full branch anyway
        # and have no need for empty folders
        if remote_file_data['obj_type'] == 'file':
            full_path = os.path.join(ROOT, relative_path.strip('/'))
            if os.path.isfile(full_path):
                # We already have this file. Check if different, newer,...
                file_data = get_file_from_db(relative_path)
                if file_data is None:
                    # File exists but is not added to DB. Add it now.
                    add_missing_path_to_db(relative_path)
                    file_data = get_file_from_db(relative_path)

                if file_data['hash'] != remote_file_data['hash']:
                    remote_event_time = int(remote_file_data.get('time', 0))

                    if int(file_data.get('mtime', 0)) > int(remote_file_data.get('mtime', remote_event_time)):
                        # local file is newer send it
                        client_send_local_file(relative_path)
                    else:
                        # Remote file is newer. Fetch it.
                        client_fetch_remote_file(relative_path)
                else:
                    print("Not accepting new file. Same file already exists")
            else:
                # We do not have this file. Fetch it.
                client_fetch_remote_file(relative_path)

    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR parsing file data: {}\n on line {}!\n\t{}".format(remote_file_data, exc_tb.tb_lineno, exc))


def client_sync_local_files(remote_file_list):
    # Check which files we have and send ones that do not exist on the remote system
    print("Sync local files. Remote files:", remote_file_list, type(remote_file_list))
    # Create a dictionary of remote files only, not dirs
    remote_file_dict = {}
    if isinstance(remote_file_list, list) and len(remote_file_list) > 0:
        for obj in remote_file_list:
            try:
                if obj.get('obj_type', '') == 'file':
                    obj_properties = {'hash': obj.get('hash', ''), 'mtime': obj.get('mtime', '')}
                    remote_file_dict[obj['path']] = obj_properties

            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print("ERROR parsing object on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))

    # Now that we have a list of remote files, we can list local files and check for missing
    try:
        for loc_obj in get_all_files_from_db():
            if loc_obj['obj_type'] == 'file':
                if loc_obj['path'] in remote_file_dict.keys():
                    # This file exists on the server. Check if different.
                    remote_file = remote_file_dict[loc_obj['path']]
                    if loc_obj['hash'] != remote_file['hash']:
                        # Files are different. Check which is newer.
                        if int(loc_obj.get('mtime', 0)) > int(remote_file.get('mtime', 0)):
                            # Local file is newer
                            client_send_local_file(loc_obj['path'])
                        else:
                            # Get remote file
                            client_sync_remote_file(loc_obj)
                else:
                    client_send_local_file(loc_obj['path'])

    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR parsing local file list, on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))


def client_handle_remote_event(remote_event):
    # print("client_handle_remote_event. Remote event: ", remote_event)
    try:
        full_path = os.path.join(ROOT, remote_event['path'].strip('/'))

        if remote_event['ev_type'] == 'CREATE':
            print("client_handle_CREATE_event: ", remote_event)
            if remote_event['obj_type'] == 'file':
                print("handle_CREATE_file: ", full_path)
                client_sync_remote_file(remote_event)

        elif remote_event['ev_type'] == 'DELETE':
            if os.path.exists(full_path):
                # We have this file/folder. Delete it
                if os.path.isfile(full_path):
                    os.remove(full_path)
                elif os.path.isdir(full_path):
                    shutil.rmtree(full_path)

    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR parsing remote event on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))


def client_handle_local_event(local_event):
    # Only send DELETE events as event. CREATE file events send as update_file request
    print("client_handle_local_event:", local_event)

    try:
        if local_event['ev_type'] == 'CREATE':
            # only send created files. Directories will be created consequently.
            if local_event['obj_type'] == 'file':
                client_send_local_file(local_event['path'])

        elif local_event['ev_type'] == 'DELETE':
            msg = {'request': 'set_events', 'events': [local_event]}

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                request = json.dumps(msg) + "\n"
                s.connect((HOST, PORT))
                s.sendall(request.encode())
                response = ''
                while True:
                    data = s.recv(1024)
                    if data:
                        response += data.decode()
                        if response.endswith('\n'):
                            break
                    else:
                        break
                print("Server response:", response)
    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("ERROR handling local event on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))


def socket_client():
    """ Main client loop.

        Periodically asks remote server for new events and sends local events to server.
        Sends the server response to the response parser.
    """
    global client_sync_timeout

    last_remote_event_time = time.time()
    last_sync_time = time.time()
    # First ask for all available files. Get file without specifying path, so get all...
    msg = {'request': 'get_file'}
    response = query_remote_server(msg)

    if 'response' in response.keys():
        try:
            for remote_file_data in response['response']:
                client_sync_remote_file(remote_file_data)

            last_local_event_time = time.time()
            client_sync_local_files(response['response'])

            while True:
                time.sleep(1)

                # Check local events
                local_events = get_events_from_db(last_local_event_time)

                if len(local_events) > 0:
                    # New events occurred. Sync more often.
                    client_sync_timeout = 10

                    for event in local_events:
                        client_handle_local_event(event)
                        if event['time'] >= last_local_event_time:
                            last_local_event_time = event['time']

                if (time.time() - last_sync_time) > client_sync_timeout:
                    # Check remote events.
                    last_sync_time = time.time()
                    msg = {'request': 'get_events', 'time': last_remote_event_time}
                    response = query_remote_server(msg)

                    print('RESPONSE: ', response)

                    if response.get('code', 'ERROR') == 'OK' and 'response' in response.keys():
                        try:
                            event_list = json.loads(response['response'])
                            if isinstance(event_list, list) and len(event_list) > 0:
                                client_sync_timeout = 1
                                for event in event_list:
                                    client_handle_remote_event(event)

                                    if event['time'] > last_remote_event_time:
                                        last_remote_event_time = event['time']

                            server_file_count = response.get('file_count', 0)
                            local_file_count = get_file_count_from_db()
                            if local_file_count != server_file_count:
                                # The server and client are not in sync. Perform full sync.
                                print("ERROR in sync. Server file count: {}; local file count: {}".format(server_file_count, local_file_count))
                                msg = {'request': 'get_file'}
                                response = query_remote_server(msg)

                                if 'response' in response.keys():
                                    for remote_file_data in response['response']:
                                        client_sync_remote_file(remote_file_data)
                                    client_sync_local_files(response['response'])

                        except Exception as exc:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            print("ERROR parsing remote event list on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))

        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("ERROR parsing file list on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))
    else:
        print("Unexpected server response: {}".format(response))


def init_file_system_watcher():
    global EXIT_FLAG
    global watcher
    global t_fs_watcher

    print("Building file list. Please wait.")
    start_time = time.time()
    handle_obj_add("/")
    print("Finished adding files to database in {} seconds.".format(int(time.time() - start_time)))

    print("Watching {}".format(ROOT))
    event_mask = (inotify.constants.IN_CREATE
                  | inotify.constants.IN_MOVED_TO
                  | inotify.constants.IN_MOVED_FROM
                  | inotify.constants.IN_DELETE
                  | inotify.constants.IN_MODIFY)

    try:
        watcher = inotify.adapters.InotifyTree(ROOT, mask=event_mask)
    except Exception as e:
        EXIT_FLAG = True
        sys.exit("ERROR setting up file watcher: {}".format(e))

    t_fs_watcher = threading.Thread(target=file_system_event_watcher)
    t_fs_watcher.daemon = True
    t_fs_watcher.start()


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--root", help="Folder to monitor.", required=True, type=ascii)
parser.add_argument("--mode", help="Operational mode (server or client).", required=True, type=ascii)
parser.add_argument("--port", help="Communication port.", required=False, default="31313", type=int)
parser.add_argument("--host", help="If running in client mode, IP or URL to host.", required=False, type=ascii, default="")
args = parser.parse_args()
check_args()

try:
    while True:
        EXIT_FLAG = False
        try:
            init_database()
            init_file_system_watcher()
            if MODE == 'server':
                socket_server()
            else:
                socket_client()
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("ERROR initializing on line {}!\n\t{}".format(exc_tb.tb_lineno, exc))

        time.sleep(10)
finally:
    if os.path.exists(DATABASE):
        print("Deleting database:{}".format(DATABASE))
        os.remove(DATABASE)
