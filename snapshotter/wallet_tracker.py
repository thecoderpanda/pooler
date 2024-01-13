from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import threading
import logging
import pika
from snapshotter.settings.config import settings
from snapshotter.auth.helpers.redis_conn import RedisPoolCache as AuthRedisPoolCache
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import active_status_key
from snapshotter.utils.redis.redis_keys import epoch_id_epoch_released_key
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import epoch_process_report_cached_key
from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_key
import redis

# Configure logging
logging.basicConfig(level=logging.INFO)
rest_logger = logger.bind(module='Powerloom|Wallet_tracker')


# Initialize FastAPI app
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Class definitions
class Transaction(BaseModel):
    contract_interacted_with: str
    txhash: str
    timestamp: str

class User(BaseModel):
    wallet_address: str
    email: str

# Connect to Redis
def connect_to_redis():
    try:
        return redis.Redis(host=settings.redis.host, port=settings.redis.port, password=settings.redis.password, db=1, decode_responses=True)
    except redis.RedisError as e:
        logging.error(f"Failed to connect to Redis: {e}")
        raise

# Connect to RabbitMQ
def connect_to_rabbitmq():
    try:
        credentials = pika.PlainCredentials('guest', 'guest')  # Replace with your credentials
        return pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', credentials=credentials))
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Failed to connect to RabbitMQ: {e}")
        raise

# Initialize Redis and RabbitMQ connections
r = connect_to_redis()
rabbitmq_connection = connect_to_rabbitmq()
rabbitmq_channel = rabbitmq_connection.channel()
queue_name = "hello"

# RabbitMQ consumer setup
def rabbitmq_callback(ch, method, properties, body):
    logging.info(f"Received message from RabbitMQ: {body}")

def start_rabbitmq_listener():
    rabbitmq_channel.queue_declare(queue=queue_name, durable=False)
    rabbitmq_channel.basic_consume(queue=queue_name, on_message_callback=rabbitmq_callback, auto_ack=True)
    rabbitmq_channel.start_consuming()

# Start the RabbitMQ listener on app startup
@app.on_event("startup")
async def startup_event():
    thread = threading.Thread(target=start_rabbitmq_listener)
    thread.start()

# FastAPI endpoints
@app.post("/subscribe/")
def subscribe(user: User):
    key = f"user:{user.wallet_address}"
    if r.exists(key):
        return {"status": "error", "message": "User already exists"}

    r.hmset(key, user.dict())
    return {"status": "success", "message": "User subscribed successfully"}

@app.post("/transactions/")
def create_transaction(transaction: Transaction):
    key = f"transaction:{transaction.txhash}"
    if not r.exists(key):
        r.hmset(key, transaction.dict())
        return {"status": "success", "transaction": transaction}
    else:
        raise HTTPException(status_code=400, detail="Transaction already exists")

@app.get("/transactions/{txhash}")
def read_transaction(txhash: str):
    key = f"transaction:{txhash}"
    transaction = r.hgetall(key)
    if transaction:
        return transaction
    raise HTTPException(status_code=404, detail="Transaction not found")
