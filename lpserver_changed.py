#
#  Lazy Pirate server
#  Binds REQ socket to tcp://*:5555
#  Like hwserver except:
#   - echoes request as-is
#   - randomly runs slowly, or exits to simulate a crash.
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
from __future__ import print_function

from random import randint
import time
import zmq
import json

context = zmq.Context(1)
server = context.socket(zmq.REP)
server.bind("tcp://*:5555")

#cycles = 0
while True:
    request = server.recv_json()
#    cycles += 1
#     # Simulate various problems, after a few cycles
#     if cycles > 3 and randint(0, 3) == 0:
#         print("I: Simulating a crash")
#         break
#     elif cycles > 3 and randint(0, 3) == 0:
#         print("I: Simulating CPU overload")
#         time.sleep(2)
#
    e = json.loads(request)
    print("I: {} request ({})".format(e[1], e[0]))
    print(e[3])
    time.sleep(1) # Do some heavy work
    if e[2] == 0:
        words = "Then serve me! What can you do?"
    elif e[2] == 1:
        words = "Then get me some emails!"
    else:
        words = "What are you mumbling?!"
    r = json.dumps([e[0], "Master", e[2], words])
    server.send_json(r)

server.close()
context.term()
