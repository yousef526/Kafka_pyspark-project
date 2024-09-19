
from confluent_kafka import Producer
import time



def main():
    config = {"bootstrap.servers":"localhost:9092"}
    producer = Producer(**config)
    with open(r"D:\iti items\Study\Kafka\Project\Scrapping\file1.txt", 'r') as file:
        reader = file.readlines()
        y = 1
        for row in reader:
            if y == 1:
                y+=1
                continue
            producer.produce(topic="Topic_1",value=row.encode("utf-8"))
            #time.sleep(0.5)
            


    producer.flush()


