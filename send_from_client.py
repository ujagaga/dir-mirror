#!/usr/bin/python3

import asyncio
import websockets


async def hello():
    uri = "ws://localhost:1313"
    async with websockets.connect(uri) as websocket:
        msg = "Test msg"

        await websocket.send(msg)
        print("Tx:", msg)

        response = await websocket.recv()
        print("Rx", response)

asyncio.get_event_loop().run_until_complete(hello())
