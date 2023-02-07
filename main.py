# Installing packages
import time
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel
from fastapi.background import BackgroundTasks

# Create application
app = FastAPI()

# Configure middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

# Configure redis database
redis = get_redis_connection(
    host='redis-10594.c212.ap-south-1-1.ec2.cloud.redislabs.com',
    port=10594,
    password='ZE39mh3gwi6JVUEjB53dSbxPJpwwUXY8',
    decode_responses=True
)


# Create product order model
class ProductOrder(HashModel):
    product_id: str
    quantity: int

    class Meta:
        database = redis


# Create order model
class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str

    class Meta:
        database = redis


# Endpoint to create order
@app.post('/order')
def create_order(product_order: ProductOrder, background_task: BackgroundTasks):
    req = requests.get(f'http://localhost:8000/product/{product_order.product_id}')
    product = req.json()
    fee = product['price'] * 0.2

    # create store order
    order = Order(
        product_id=product_order.product_id,
        price=product['price'],
        fee=fee,
        total=product['price'] + fee,
        quantity=product_order.quantity,
        status='pending'
    )

    # save order
    order.save()

    # simulating action on order: delay of 5 seconds
    # adding background task
    background_task.add_task(complete_order, order)

    # return order
    return order


@app.get('/order/{pk}')
def get_order(pk: str):
    return format_order_by_pk(pk)


@app.get('/orders')
def get_all_orders():
    return [format_order_by_pk(pk) for pk in Order.all_pks()]


def format_order_by_pk(pk: str):
    order = Order.get(pk)
    return {
        'id': order.pk,
        'product_id': order.product_id,
        'price': order.price,
        'fee': order.fee,
        'total': order.total,
        'quantity': order.quantity,
        'status': order.status
    }


def complete_order(order: Order):
    time.sleep(5)
    order.status = 'completed'
    order.save()
    redis.xadd(name='order-completed', fields=order.dict())


# Endpoint to delete order
@app.delete('/order/{pk}')
def delete_order(pk: str):
    return Order.delete(pk)
