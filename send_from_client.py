#!/usr/bin/python3

import socket
import json

HOST = "0.0.0.0"
PORT = 1313

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    msg = {'request': 'get_file_content', 'path': '/Statistika za 1. kvartal 2021. godine.docx'}
    request = json.dumps(msg) + "\n"

    s.connect((HOST, PORT))
    s.sendall(request.encode())

    response = ""
    while True:
        data = s.recv(1)
        if data:
            response += data.decode()
        else:
            break

print('Received:', response)
