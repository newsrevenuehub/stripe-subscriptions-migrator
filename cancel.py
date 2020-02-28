import csv
import logging

import stripe
from environs import Env

### Setup

logger = logging.getLogger()
logger.setLevel("INFO")
formatter = logging.Formatter(fmt="%(levelname)s %(name)s/%(module)s:%(lineno)d - %(message)s")
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

env = Env()
env.read_env()

stripe.api_key = env("STRIPE_KEY")

### Process the CSV

with open("subscriptions.csv") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print(f"processing record for {row['email']} (${row['amount']} each {row['interval']})...")

        print(f"canceling the Stripe subscription {row['subscription_id']}...")
        stripe.Subscription.delete(row["subscription_id"])
