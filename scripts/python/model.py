from abc import abstractmethod, ABC
import pathlib
import csv
import random
from datetime import datetime, timedelta


class UserEvent(ABC):
    event_type='event_type'

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

    
    def __repr__(self):
        return f"{self.event_type}- {self.product_name}- {self.customer_name}- {self.date_purchased}- { self.unit_cost}- {self.quanity}"
        

class UserEventGenerator(ABC):
    @abstractmethod
    def prepare_event(self) -> list[UserEvent]:
        pass

    @abstractmethod
    def do_export(self, folder: pathlib.Path):
        pass

class ProductViewGenerator(UserEventGenerator):
    def __init__(self):
        self.events = []
    
    
    def prepare_event(self) -> list[UserEvent]:
        """Return list of product view by user event"""
        products = ['Phone', 'Laptop', 'Tablet', 'Headphone']
        customers = ['Kwadwo', 'Evans', 'Julliet', 'Abigail', 'Isaiah']
        today = datetime.today()
        num_of_events  = random.randint(0, 10)
        for _ in range(num_of_events):
            event = ProductViewEvent(
                product_name=random.choice(products),
                customer_name= random.choice(customers),
                date_viewed=(today - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 60))).strftime("%d-%m-%Y, %H:%M:%S"))
            self.events.append(event)

        return self.events
    
    def do_export(self, folder: pathlib.Path):
        """Export the event into a csv file"""
        folder.mkdir(exist_ok=True, parents=True)
        file_path = folder/"product_view_event.csv"

        
    
        with open(file_path, 'a', newline='') as csvfile:
            fieldnames = ['event_type', 'product_name', 'customer_name', 'date_viewed']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_path.exists():
                writer.writeheader()

            for event in self.events:
                writer.writerow(event.to_dict())

        print(f"Exported to {file_path}")
     
class ProductPurchaseGenerator(UserEventGenerator):
     def prepare_event() -> UserEvent:
        """Return list of product purchases by user event"""
        pass
     
     def do_export(self, folder: pathlib.Path) :
        """Export the event into a csv file"""
        pass
     
class GeneratorFactory(ABC):
    def get_product_view_generator(self) -> UserEventGenerator:
        pass
    # def get_product_purchase_generator(self):


if __name__ == "__main__":
    folder_path = pathlib.Path("../../e-commerce-user-events/product-view-events")
    generator = ProductViewGenerator()
    generator.prepare_event()
    generator.do_export(folder_path)