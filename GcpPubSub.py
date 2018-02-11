# -*- coding: utf-8 -*-
# filename: GcpPubSub.py
# Author: Oliver

from google.cloud import pubsub as ps
import threading
import time
import json
import traceback

class SourceReader(object):
    def __init__(self, data_source):
        self.data_source = data_source

    def __del__(self):
        pass

    def return_data(self):
        try:
            data = ""
            with open(self.data_source, "r") as ds:
                data = ds.readlines()
        except Exception, e:
            traceback.print_exc()
        finally:
            if len(data) > 0:
                return data
            else:
                return ""


class Publisher(object):
    def publish_messages(self, project, topic_name, message):
        """Publishes multiple messages to a Pub/Sub topic."""
        publisher = ps.PublisherClient()
        topic_path = "projects/fcr-it/topics/olivertopic"
        #topic_path = publisher.topic_path(project, topic_name)
        publisher.publish(topic_path, data=message)


class Receiver(object):
    @staticmethod
    def receive_messages(project, subscription_name):
        """Receives messages from a pull subscription."""
        subscriber = ps.SubscriberClient()
        subscription_path = "projects/fcr-it/subscriptions/oliverpull"

        def callback(message):
            print('Received message: {}'.format(message))
            message.ack()

        subscriber.subscribe(subscription_path, callback=callback)

        print('Listening for messages on {}'.format(subscription_path))
        while True:
            time.sleep(2)


if __name__ == "__main__":
    threading.Thread(name="Receiver", target=Receiver.receive_messages, args=("fcr-it", "oliverpull")).start()
    time.sleep(2)

    #Receiver().receive_messages("fcr-it", "oliverpull")
    all_lines = SourceReader("/source/TradeTransactions.csv").return_data()
    title = all_lines.pop(0)
    title_menu = title.split(",")
    for line in all_lines:
        customer_field = line.split(",")
        customer_data = {}
        for i in range(1, len(customer_field) - 1):
            customer_data[title_menu[i]] = customer_field[i]
        print(customer_data)
        Publisher().publish_messages("fcr-it", "olivertopic", json.dumps(customer_data))
        time.sleep(1)