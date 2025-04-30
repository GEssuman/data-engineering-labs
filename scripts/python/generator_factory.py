from abc import abstractmethod, ABC
import pathlib
import csv
import random
from datetime import datetime, timedelta
import uuid
from model import UserEvent, ProductPurchaseEvent, ProductViewEvent, CUSTOMER_NAMES, PRODUCTS

class UserEventGenerator(ABC):
    products = list(PRODUCTS.keys())
    def __init__(self):
        self.events = []
        self.fieldnames = []
        self.filename = ""
        self.sub_folder = ""
    
    @abstractmethod
    def prepare_event(self) -> list[UserEvent]:
        pass

    def do_export(self, folder: pathlib.Path):
        """Export the event into a csv file"""
        folder = folder / self.sub_folder
        folder.mkdir(exist_ok=True, parents=True)
        file_path = folder/self.filename

        
    
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for event in self.events:
                writer.writerow(event.to_dict())

        print(f"Exported to {file_path}")

class ProductViewGenerator(UserEventGenerator):
    # sub_folder = "/product-purchased-events"
    def __init__(self):
        super().__init__()
        self.sub_folder = "product-view-events"
        self.filename = f"product_view_events_{uuid.uuid4()}.csv"
        self.fieldnames =  ['event_type', 'product_name', 'unit_price', 'customer_surname', 'customer_firstname', 'date_viewed']
    
    def prepare_event(self) -> list[UserEvent]:
        """Return list of product view by user event"""
        v_prod_name =random.choice(UserEventGenerator.products)

        today = datetime.today()
        num_of_events  = random.randint(25, 50)
        for _ in range(num_of_events):
            event = ProductViewEvent(
                product_name=v_prod_name,
                customer_firstname = random.choice(CUSTOMER_NAMES).split(' ')[1],
                customer_surname = random.choice(CUSTOMER_NAMES).split(' ')[0],
                unit_price=PRODUCTS[v_prod_name],
                date_viewed=(today - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 60))).strftime("%d-%m-%Y, %H:%M:%S"))
            self.events.append(event)

        return self.events
    
     
class ProductPurchaseGenerator(UserEventGenerator):
    def __init__(self):
        super().__init__()
        self.sub_folder = "product-purchase-events"
        self.filename = f"product_purchase_event_{uuid.uuid4()}.csv"
        self.fieldnames = ['event_type', 'product_name', 'customer_surname', 'customer_firstname', 'date_purchased', 'unit_price', 'quanity']
        
    def prepare_event(self) -> UserEvent:
        """Return list of product purchases by user event"""

        today = datetime.today()
        num_of_events  = random.randint(25, 50)
        for _ in range(num_of_events):
            v_prod_name =random.choice(UserEventGenerator.products)

            event = ProductPurchaseEvent(
                product_name= v_prod_name,
                customer_firstname = random.choice(CUSTOMER_NAMES).split(' ')[1],
                customer_surname = random.choice(CUSTOMER_NAMES).split(' ')[0],
                unit_price=PRODUCTS[v_prod_name],
                quanity = random.randint(1, 5),
                date_purchased=(today - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 60))).strftime("%d-%m-%Y, %H:%M:%S"))
                
            self.events.append(event)
     
     
class GeneratorFactory(ABC):
    @abstractmethod
    def get_generators(self) -> list[UserEventGenerator]:
        pass



class ConcreteGeneratorFactory(GeneratorFactory):
    def __init__(self):
        self.generators: list[UserEventGenerator] = [
            ProductViewGenerator(),
            ProductPurchaseGenerator()
        ]

    def get_generators(self) -> list[UserEventGenerator]:
        return self.generators

def generate_and_export_all_events(factory: GeneratorFactory, export_folder: pathlib.Path):
    generators = factory.get_generators()
    
    for generator in generators:
        generator.prepare_event()
        generator.do_export(export_folder)

