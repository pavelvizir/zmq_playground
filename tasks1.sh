#!/usr/bin/bash
for i in $(seq 1 33); do python taskwork.py & echo taskwork $i started... ; done
python tasksink.py &
python taskvent.py
kill $(jobs -p) 2>/dev/null

