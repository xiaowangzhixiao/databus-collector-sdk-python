import requests
import queue
import threading
import time
import json

url = "http://dbc.free4inno.com"
q = queue.Queue()


class SendThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.startTime = time.time()

    def run(self):
        while True:
            size = q.qsize()
            if size > 800 or time.time() - self.startTime > 3:
                self.startTime = time.time()
                topic_data_dict = dict()
                content = '['
                for i in range(size):
                    r = q.get()
                    if r.topic not in topic_data_dict:
                        topic_data_dict[r.topic] = []
                    topic_data_dict[r.topic].append(r)
                    # print(r)
                for topic, r_list in topic_data_dict.items():
                    content = '['
                    for r in r_list:
                        content = content + r.to_json() + ','
                    content = content[:-1]
                    content += ']'
                    # print(content)
                    sync_send(topic, content)
            time.sleep(0.2)


send_thread = SendThread()
send_thread.start()


def send(a_record):
    q.put(a_record)


def sync_send(a_record):
    r = requests.post(url+'/rest/data',
                      data={'topic': a_record.topic, 'c': '[' + a_record.to_json() + ']'})
    return r
    # print(r.content)


def sync_send(topic, c):
    r = requests.post(url+'/rest/data',
                      data={'topic': topic, 'c': c})
    return r
    # print(r.content)


__all__ = ["record", "apiclient"]
