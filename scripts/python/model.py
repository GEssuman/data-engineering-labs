from abc import abstractmethod, ABC



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
        

