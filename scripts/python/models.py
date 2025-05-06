import uuid
import datetime
import random
import time
import threading
import json
from kafka import KafkaProducer

## Class For Sensor Object
class Sensor():
    """
    Sensor class that imulate a heart beat sensor assigned to a specific user.
    Each sensor has a unique sensor_id and is capable of generating synthetic heart beat data
    """


    def __init__(self, user_id, kafka_bootstrap_servers='localhost:9092', topic="heart_beart"):
        """
        Initialize a SEensor instance for a given user.

        Parameters:
        user_id (str): Unique identifier of the user to whom the sensor is assigned.
        """
        self.user_id = user_id
        self.sensor_id = uuid.uuid4() #Generate unique sensor ID
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def check_heart_beat(self):
        """
        Simulate checking the user's hear beat.
        
        Returns:
        dict: A dictionary caontaining the user_id, sensor_id, current timestamp, and a simulated heart beat value.
        """
        heart_beat = int(random.uniform(60, 130)) #Simulated heart beat between 60 and 130bpm
        timestamp = datetime.datetime.now() #Current timespamp

        return {
            'user_id': self.user_id,
            'sensor_id': str(self.sensor_id),
            'timestamp': timestamp.isoformat(),
            'heartbeat': heart_beat
        }
    def publish_kafka(self, data):
        self.producer.send(self.topic, value=data)
        self.producer.flush()

        

    def start(self):
        """
        Start the sensor and return a heart beat reading by calling the check_heart_beat method.

        Returns:
        dict: A simulated heart beat reading .
        """
        reading = self.check_heart_beat()
        self.publish_kafka(reading)
        return reading



class Customer():
    """
    Customer class that represents a user who can install and start heart beat sensors.
    Each customer maintains their own set of sensors privately.
    """
    
    def __init__(self, user_id):
        """
        Initialize a Customer instance with a unique user ID and an empty set of sensors..

        Parameters:
        user_id (str): Unique identifier for the customer.
        """
        self.user_id = user_id
        self.sensors = {}

    def install_sensor(self):
        """
        Install a new Sensor for the customer and register it in the customer's  sensors dictionary.

        Returns:
        UUID: The sensor_id of the newly installed sensor.
        """
        sensor = Sensor(self.user_id)
        self.sensors[sensor.sensor_id] = sensor
        return sensor.sensor_id

    def start_sensor(self, sensor_id):
        """
        Start the specified sensor (by sensor_id) and retrieve a heart beat reading.

        Parameters:
        sensor_id (UUID): The unique identifier of the sensor to start.

        Returns:
        dict: A simulated heart beat reading from the sensor, if owned by the customer..
        """
        sensor = self.sensors.get(sensor_id)
        if sensor:
            return sensor.start()
        else: 
            print(f"No sensor found with id {sensor_id} for user {self.user_id}")



class Simulator():
    """
    Simulator class that manages multiple customers and simulates heart beat data generation.
    """
    customers = [] #list to store all customer instances
   

    def __init__(self, number_of_customer):
        """
        Initialize the Simulator with a specified number of customers.

        Parameters:
        number_of_customers (int): The number of customers to simulate.
        """
        self.number_of_customer = number_of_customer

    def simulate(self):
        """
        Simulate the installation of sensors and generation of heart beat readings
        for all customers. Each customer's sensor produces periodic readings.

        The simulation runs under the True while loop.
        """

        #create customers instances and store them
        for i in range(self.number_of_customer):
            Simulator.customers.append(Customer(f"CUST_{i}"))

       
        # Install one sensor per customer and launch a thread per sensor
        threads = []
        for customer in Simulator.customers:
            sensor_id = customer.install_sensor()
            t = threading.Thread(target=self.run_sensor, args=(customer, sensor_id))
            t.daemon = True  #makes threads exit when main program exits
            t.start()
            threads.append(t)
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nSimulation stopped.")

    def run_sensor(self, customer, sensor_id):
        """
        Target function for each sensor's thread — repeatedly fetch.

        Parameters:
        customer (Customer): The customer object that owns the sensor.
        sensor_id (UUID): The ID of the sensor to start.
        """

        while True:
            reading = customer.start_sensor(sensor_id)
            print(reading)
            time.sleep(0.5)