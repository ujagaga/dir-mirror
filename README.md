# dir-mirror
Mirror folder content across network or on same computer.

## What it's for
I needed a solution to keep two folders on different computers synchronized (identical content) without a third party server. One of those is an Orange Pi Zero, so an arm based computer.
There are many cloud solutions providing file sync functionality. Most of them have a GUI or web based configuration. On a server environment I can use neither. The solutions I found working 
in a server environment are mostly not available for arm architecture like Orange Pi Zero or Raspberry Pi Zero. One I did find working is unstable, so if a client disconnects, it can no longer 
connect unless the server is restarted.
This is why I decided to write my own in Python as it can be made cross platform and simpler to maintain.

## How it works
On one of the computers you run the app in server mode specifying folder to watch. The server will then build a list of files and provide a tcp connection, listening on a specified port.
On another computer you run the app in client mode, specify the folder to mirror, server address and port. The client app will build a list of files and folders and connect to the server.
The files with the same name will be overwritten with newer one (younger modify time) on both locations and missing ones will be copied. Any subsequent change will be propagated both ways 
keeping content in both locations identical.

## Current status
The app is in development and not yet ready for use. Currently it can monitor file system changes and maintain a list of files, folders and events. The tcp communication is yet to be implemented.

## Running
To run as server:

    ./dir-mirror --root <path to data folder> --mode server
  
To run as client:

    ./dir-mirror --root <path to data folder> --mode client --host <server address>
   
if on the same computer, then server address is "localhost", else an ip address.
    
## Preparing development or deployment environment
sudo apt install python3-pip
pip3 install inotify
sudo echo "fs.inotify.max_user_watches=524288" >> /etc/sysctl.conf
reboot
