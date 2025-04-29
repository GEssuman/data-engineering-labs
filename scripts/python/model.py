from abc import abstractmethod, ABC
import pathlib
import csv
import random
from datetime import datetime, timedelta
import uuid


class UserEvent(ABC):
    event_type='event_type'

    def to_dict(self)->dict:
        pass

class ProductViewEvent(UserEvent):
    def __init__(self, product_name, customer_name, date_viewed):
        self.event_type = 'product_view'
        self.product_name = product_name
        self.customer_name = customer_name
        self.date_viewed = date_viewed


    def to_dict(self):
        return {
            'event_type': self.event_type,
            'product_name': self.product_name,
            'customer_name': self.customer_name,
            'date_viewed': self.date_viewed
        }


    def __repr__(self):
        return f"{self.event_type}- {self.product_name}- {self.customer_name}- {self.date_viewed}"

class ProductPurchaseEvent(UserEvent):
    def __init__(self, product_name, customer_name, date_purchased, unit_cost, quanity):
        self.event_type = 'product_purchase'
        self.product_name = product_name
        self.customer_name = customer_name
        self.date_purchased = date_purchased
        self.unit_cost = unit_cost
        self.quanity = quanity

    def to_dict(self):
        return {
            'event_type': self.event_type,
            'product_name': self.product_name,
            'customer_name': self.customer_name,
            'date_purchased': self.date_purchased,
            'unit_cost': self.unit_cost,
            'quanity': self.quanity
        }
    
    def __repr__(self):
        return f"{self.event_type}- {self.product_name}- {self.customer_name}- {self.date_purchased}- { self.unit_cost}- {self.quanity}"
        

class UserEventGenerator(ABC):

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
        self.fieldnames =  ['event_type', 'product_name', 'customer_name', 'date_viewed']
    
    def prepare_event(self) -> list[UserEvent]:
        """Return list of product view by user event"""
        products = ['Phone', 'Laptop', 'Tablet', 'Headphone']
        customers = ['Kwadwo', 'Evans', 'Julliet', 'Abigail', 'Isaiah']
        today = datetime.today()
        num_of_events  = random.randint(25, 50)
        for _ in range(num_of_events):
            event = ProductViewEvent(
                product_name=random.choice(products),
                customer_name= random.choice(customers),
                date_viewed=(today - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 60))).strftime("%d-%m-%Y, %H:%M:%S"))
            self.events.append(event)

        return self.events
    
     
class ProductPurchaseGenerator(UserEventGenerator):
    def __init__(self):
        super().__init__()
        self.sub_folder = "product-purchase-events"
        self.filename = f"product_purchase_event_{uuid.uuid4()}.csv"
        self.fieldnames = ['event_type', 'product_name', 'customer_name', 'date_purchased', 'unit_cost', 'quanity']
        self.product_prices = {
            'Phone': 35.0,
            'Laptop': 45.0,
            'Tablet': 30.0,
            'Headphone': 25.0
        }
        
    def prepare_event(self) -> UserEvent:
        """Return list of product purchases by user event"""
        products = list(self.product_prices.keys())

        customers = ['Kwadwo', 'Evans', 'Julliet', 'Abigail', 'Isaiah']
        today = datetime.today()
        num_of_events  = random.randint(25, 50)
        for _ in range(num_of_events):
            v_prod_name =random.choice(products)

            event = ProductPurchaseEvent(
                product_name= v_prod_name,
                customer_name= random.choice(customers),
                unit_cost=self.product_prices[v_prod_name],
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

if __name__ == "__main__":
    # folder_path = pathlib.Path("../../e-commerce-user-events/product-view-events")
    # folder_path_2 = pathlib.Path("../../e-commerce-user-events/product-purchase-events")



    output_path = pathlib.Path("../../e-commerce-user-events")
    factory = ConcreteGeneratorFactory()
    generate_and_export_all_events(factory, output_path)
    
    
    
    
    # generator = ProductViewGenerator()
    # generator_2 = ProductPurchaseGenerator()

    # generator.prepare_event()
    # generator.do_export(folder_path)


    # generator_2.prepare_event()
    # generator_2.do_export(folder_path_2)