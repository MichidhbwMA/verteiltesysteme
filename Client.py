import socket
import struct
import threading

class PubSubClient:
    def __init__(self, broker_host='localhost', broker_port=42069):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))  # Binde an localhost und einen zufälligen Port
    
    def subscribe(self, topic):
        message = self.create_message('subscribe', topic, '')
        self.sock.sendto(message, (self.broker_host, self.broker_port))
    
    def unsubscribe(self, topic):
        message = self.create_message('unsubscribe', topic, '')
        self.sock.sendto(message, (self.broker_host, self.broker_port))

    def publish(self, topic, message):
        message = self.create_message('publish', topic, message)
        self.sock.sendto(message, (self.broker_host, self.broker_port))

    def create_message(self, message_type, topic, message):
        message_type = message_type.ljust(10).encode('utf-8')
        topic_len = len(topic)
        topic = topic.encode('utf-8')
        message = message.encode('utf-8')
        return struct.pack('!10s H', message_type, topic_len) + topic + message

    def receive(self):
        while True:
            data, addr = self.sock.recvfrom(65507)
            print(f"Received message: {data.decode('utf-8')}")

if __name__ == "__main__":
    client = PubSubClient()
    threading.Thread(target=client.receive).start()
    client.subscribe("example_topic")
    client.publish("example_topic", "Hello")
