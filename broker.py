import socket
import struct
import threading

class MessageBroker:
    def __init__(self, host='localhost', port=42069):
        self.host = host
        self.port = port
        self.subscriptions = {}
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))

    def start(self):
        print(f"Message Broker started on {self.host}:{self.port}")
        threading.Thread(target=self.listen).start()

    def listen(self):
        while True:
            data, addr = self.sock.recvfrom(65507)
            self.handle_message(data, addr)

    def handle_message(self, data, addr):
        message_type, topic, message = self.parse_message(data)
        
        if message_type == 'subscribe':
            self.subscribe(addr, topic)
        elif message_type == 'unsubscribe':
            self.unsubscribe(addr, topic)
        elif message_type == 'publish':
            self.publish(topic, message)
    
    def parse_message(self, data):
        message_type, topic_len = struct.unpack('!10s H', data[:12])
        topic_len = int(topic_len)
        topic = data[12:12+topic_len].decode('utf-8')
        message = data[12+topic_len:].decode('utf-8')
        return message_type.strip().decode('utf-8'), topic, message

    def subscribe(self, addr, topic):
        with self.lock:
            if topic not in self.subscriptions:
                self.subscriptions[topic] = set()
            self.subscriptions[topic].add(addr)
            print(f"{addr} subscribed to {topic}")

    def unsubscribe(self, addr, topic):
        with self.lock:
            if topic in self.subscriptions:
                self.subscriptions[topic].discard(addr)
                if not self.subscriptions[topic]:
                    del self.subscriptions[topic]
            print(f"{addr} unsubscribed from {topic}")

    def publish(self, topic, message):
        with self.lock:
            if topic in self.subscriptions:
                for subscriber in self.subscriptions[topic]:
                    self.sock.sendto(message.encode('utf-8'), subscriber)
                print(f"Message published to {topic}: {message}")

if __name__ == "__main__":
    broker = MessageBroker()
    broker.start()

