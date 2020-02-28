import csv
import logging
import sys
from datetime import datetime

import arrow
import pytz
import stripe
from environs import Env
from npsp import RDO, Contact, SalesforceConfig, SalesforceConnection

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

sf_config = SalesforceConfig(
    client_id=env("SALESFORCE_CLIENT_ID"),
    client_secret=env("SALESFORCE_CLIENT_SECRET"),
    username=env("SALESFORCE_USERNAME"),
    password=env("SALESFORCE_PASSWORD"),
    host=env("SALESFORCE_HOST"),
    api_version=env("SALESFORCE_API_VERSION"),
)
sf_connection = SalesforceConnection(config=sf_config)

interval_map = {"year": "yearly", "month": "monthly"}

### Process the CSV

with open("subscriptions.csv") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print(f"processing record for {row['email']} (${row['amount']} each {row['interval']})...")

        print(f"canceling the Stripe subscription {row['subscription_id']}...")
        stripe.Subscription.delete(row["subscription_id"])

        # check for dupe
        if (RDO.get(stripe_customer_id=row["customer_id"], sf_connection=sf_connection)) is not None:
            print("Exiting; WARNING: duplicate!")
            sys.exit(-1)

        contact = Contact.get_or_create(sf_connection=sf_connection, email=row["email"])
        now = datetime.now(tz=pytz.utc).strftime("%Y-%m-%d %I:%M:%S %p %Z")

        if contact.last_name == "Subscriber":
            rdo_name = f"{now} for {row['email']}"
        else:
            rdo_name = f"{now} for {contact.first_name} {contact.last_name}"

        rdo = RDO(contact=contact, sf_connection=sf_connection)
        rdo.stripe_customer_id = row["customer_id"]
        rdo.name = rdo_name
        rdo.description = f"{row['subscription_id']} ({row['plan_name']})"
        rdo.lead_source = "Stripe"
        rdo.installment_period = interval_map[row["interval"]]
        rdo.amount = row["amount"]
        rdo.open_ended_status = "Open"

        current_period_end = arrow.get(row["current_period_end"])
        rdo.date_established = current_period_end.strftime("%Y-%m-%d")

        rdo.save()
