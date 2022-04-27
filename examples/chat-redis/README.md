# Chat using simple pub/sub

This example implements a simple chat channel where users can enter and leave that chat, and type messages to send to everyone.

## Setup

This example uses Redis as backend.
Either start redis via the CLI if you have the `redis-server` installed, or you can use Docker:

    docker run -p 6379:6379 redis

## Run a chat client

To enter the chat as alice, run:

    python3 chat.py alice

The program listens on stdin to send messages line by line.
The client will send enter and leave events when the program starts/stops.

## Demo

Here's a video:

https://user-images.githubusercontent.com/3996682/165539966-07252bc8-af75-46f4-b31c-bc59b1cdfb74.mp4

