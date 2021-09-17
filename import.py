import csv
import stripe
import logging
import sys
from datetime import datetime
import os.path
from config import FIELDNAMES

import arrow
import pytz
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

sf_config = SalesforceConfig(
    client_id=env("SALESFORCE_CLIENT_ID"),
    client_secret=env("SALESFORCE_CLIENT_SECRET"),
    username=env("SALESFORCE_USERNAME"),
    password=env("SALESFORCE_PASSWORD"),
    host=env("SALESFORCE_HOST"),
    api_version=env("SALESFORCE_API_VERSION"),
)
sf_connection = SalesforceConnection(config=sf_config)
sf_connection.test_connection()

interval_map = {"year": "yearly", "month": "monthly"}

stripe.api_key = env("STRIPE_KEY")

### Process the CSV


def add_email_to_stripe(stripe_customer_id, email):
    customer = stripe.Customer.retrieve(stripe_customer_id)
    if not customer.email:
        print(f"Stripe customer {stripe_customer_id} doesn't have email; adding")
        stripe.Customer.modify(stripe_customer_id, email=email)
        return

    if customer.email.lower() != email.lower():
        print(f"Exiting; WARNING: Stripe customer email doesn't match: {email} vs. {customer.email}")
        sys.exit(-1)


with open("subscriptions.csv") as csvfile:
    num_lines = sum(1 for line in csvfile)
    csvfile.seek(0)
    print(f"Processing file {os.path.realpath(csvfile.name)} with {num_lines} rows...")

    reader = csv.DictReader(csvfile)
    assert set(reader.fieldnames) == set(FIELDNAMES)

    for row in reader:
        print(f"processing record for {row['email']} (${row['amount']} each {row['interval']})...")

        add_email_to_stripe(row["customer_id"], row["email"])
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
        rdo.stripe_customer_id = row["customer_id"].strip()
        rdo.name = rdo_name
        rdo.description = f"{row['subscription_id']} ({row['plan_name']})"
        rdo.lead_source = "Stripe"
        rdo.installment_period = interval_map[row["interval"].strip()]
        rdo.amount = row["amount"].strip()
        rdo.open_ended_status = "Open"

        current_period_end = arrow.get(row["current_period_end"].strip())
        rdo.date_established = current_period_end.strftime("%Y-%m-%d")
        rdo.day_of_month = current_period_end.strftime("%-d")

        rdo.save()
