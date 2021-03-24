from kafka import KafkaProducer
import numpy as np
from sys import argv, exit
from time import time, sleep
import json

valid_devices = ['device1','device2', 'device3']

if len(argv) != 2 or argv[1] not in valid_devices:
	print(f'Valid device name not found \n Valid device names are: {valid_devices} \n')
	exit(1)

device = argv[1]

kafka_producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

while True:
	start_x, start_y = np.random.randint(50, size=(2))
	end_x, end_y = np.random.randint(50, size=(2))

	msg= f"{time()},{device},{start_x},{start_y},{end_x},{end_y}"
	
	print(f"Sending:\nTime:{time()}:,From: {device},\n Start({start_x},{start_y}),End({end_x},{end_y})\n")
	kafka_producer.send('location', bytes(msg, encoding='utf8'))
	sleep_time = np.random.rand()
	print(f"Device sleeping for {sleep_time} seconds before sending new data")
	sleep(sleep_time)