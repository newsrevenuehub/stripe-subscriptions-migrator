import csv
from datetime import datetime

import pytz
import stripe
from environs import Env
from config import FIELDNAMES

### Setup

env = Env()
env.read_env()

# TODO: cancel the thing

stripe.api_key = env("STRIPE_KEY")

with open("subscriptions.csv", "w") as csvfile:
    csv_writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
    csv_writer.writeheader()

    csv_record = dict()

    subscriptions = stripe.Subscription.list()
    for subscription in subscriptions.auto_paging_iter():
        print(subscription)
        subscription = subscription.to_dict()

        customer_id = subscription["customer"]
        csv_record["customer_id"] = customer_id

        csv_record["email"] = stripe.Customer.retrieve(customer_id).email
        csv_record["amount"] = subscription["quantity"] * subscription["plan"]["amount"] / 100
        csv_record["interval"] = subscription["plan"]["interval"]

        current_period_end = subscription["current_period_end"]
        csv_record["current_period_end"] = datetime.fromtimestamp(current_period_end)

        csv_record["subscription_id"] = subscription["id"]
        csv_record["plan_name"] = subscription["plan"].get("name", None) or subscription["plan"].get("nickname", None) or ""

        csv_writer.writerow(csv_record)
