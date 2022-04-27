import sys
from datetime import datetime
from typing import NamedTuple

import pymq
from pymq import EventBus
from pymq.provider.redis import RedisConfig


class ChatMessage(NamedTuple):
    user: str
    text: str


class EnterEvent(NamedTuple):
    user: str


class LeaveEvent(NamedTuple):
    user: str


class ChatClient:
    user: str

    def __init__(self, bus: EventBus, user: str):
        self.user = user
        self.bus = bus

    def enter(self):
        # subscribe the handlers
        self.bus.subscribe(self.handle_message)
        self.bus.subscribe(self.handle_enter)
        self.bus.subscribe(self.handle_leave)

        # send enter event
        self.bus.publish(EnterEvent(self.user))

    def leave(self):
        # remove handlers
        self.bus.unsubscribe(self.handle_message)
        self.bus.unsubscribe(self.handle_enter)
        self.bus.unsubscribe(self.handle_leave)

        # send leave event
        self.bus.publish(LeaveEvent(self.user))

    def send(self, text: str):
        self.bus.publish(ChatMessage(self.user, text))

    def handle_enter(self, event: EnterEvent):
        if event.user == self.user:
            return

        print(f"[{datetime.now()}] {event.user} has entered the chat!")

    def handle_leave(self, event: LeaveEvent):
        if event.user == self.user:
            return

        print(f"[{datetime.now()}] {event.user} has left the chat!")

    def handle_message(self, message: ChatMessage):
        if message.user == self.user:
            return

        print(f"[{datetime.now()}] {message.user}: {message.text}")


def main():
    user = sys.argv[1].strip()

    bus = pymq.init(RedisConfig())
    client = ChatClient(bus, user)

    try:
        client.enter()
        print(f"hi {user}!")
        print("write quit to exit the program")
        print("start typing:")
        for line in sys.stdin:
            text = line.strip()
            if text == "quit":
                break
            client.send(text)
    finally:
        client.leave()
        pymq.shutdown()


if __name__ == '__main__':
    main()
