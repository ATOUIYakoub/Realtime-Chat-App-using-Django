import json
from channels.generic.websocket import AsyncWebsocketConsumer
from confluent_kafka import Producer

class ChatConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    async def connect(self):
        self.sender_username = self.scope['url_route']['kwargs']['sender_username']
        self.receiver_username = self.scope['url_route']['kwargs']['receiver_username']
        self.room_name = f"chat_{self.sender_username}_{self.receiver_username}"
        await self.channel_layer.group_add(
            self.room_name,
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.room_name,
            self.channel_name
        )

    async def receive(self, text_data):
        text_data_json = json.loads(text_data)
        message = text_data_json["message"]
        sender_username = text_data_json["username"]
        
        # Ensure only the allowed users can send messages
        if sender_username == self.sender_username:
            # Produce message to Kafka topic
            self.producer.produce(self.room_name, key=sender_username, value=message)
            self.producer.flush()

    async def sendMessage(self, event):
        message = event["message"]
        username = event["username"]
        await self.send(text_data=json.dumps({"message": message, "username": username}))
