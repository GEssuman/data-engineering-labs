import uuid
import datetime
import random
import time

## Class For Sensor Object
class Sensor():
    """
    Sensor class that imulate a heart beat sensor assigned to a specific user.
    Each sensor has a unique sensor_id and is capable of generating synthetic heart beat data
    """


    def __init__(self, user_id):
        """
        Initialize a SEensor instance for a given user.

        Parameters:
        user_id (str): Unique identifier of the user to whom the sensor is assigned.
        """
        self.user_id = user_id
        self.sensor_id = uuid.uuid4() #Generate unique sensor ID

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
            'timestamp': timestamp,
            'heartbeat': heart_beat
        }

    def start(self):
        """
        Start the sensor and return a heart beat reading by calling the check_heart_beat method.

        Returns:
        dict: A simulated heart beat reading .
        """
        return self.check_heart_beat()



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

        #install sensor for each customer
        sensor_ids = []
        for customer in Simulator.customers:
            sensor_id = customer.install_sensor()
            sensor_ids.append((customer,sensor_id))


        # Simulate heartbeats for all sensors
        while True:
            for customer, sensor_id in sensor_ids:
                reading = customer.start_sensor(sensor_id)
                print(reading)

                time.sleep(0.5)