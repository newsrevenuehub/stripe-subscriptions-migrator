These scripts are migrating from Stripe Subscriptions to Salesforce Recurring Donation objects.

To run you'll need Docker (and `docker-compose`).

First create a file called `env-docker` with your Salesforce and Stripe credentials. It will look something like:

```
STRIPE_KEY=sk_test_foo
SALESFORCE_CLIENT_ID=bar
SALESFORCE_CLIENT_SECRET=secret
SALESFORCE_USERNAME=foo@bar.org
SALESFORCE_PASSWORD=password_then_token
SALESFORCE_HOST=test.salesforce.com
SALESFORCE_API_VERSION=v48.0
```

Then run `make`. This will drop you into a shell in the Docker container.

It's run as two scripts for safety. The first (`extract.py`) captures all of the subscriptions and saves them to a CSV. You can review that CSV to make sure it has exactly what you were hoping to get out of Stripe. So run `python extract.py`. That will produce a file called `subscriptions.csv`. Each time you run the script it will overwrite that CSV.

Once you're happy with the contents of the CSV you can run the migration script (`python migrate.py`).

The `migrate.py` script will process each record of the CSV by first canceling the subscription and then entering it into Salesforce. It tries to avoid entering duplicates by checking for an existing Salesforce record with the same Stripe Customer ID. There could be legitimate reasons why someone would have more than one recurring donation with the same Stripe Customer ID. In that case this script would break and would need to be modified to properly process the records.

The fields are converted as follows:

- Recurring Donation Name = current date followed by first+last if present, email otherwise
- Description = Stripe Subcription ID followed by the Stripe Plan Name
- Lead Source = Stripe
- Date Established = Stripe Subscription Current Period End
- Open Ended Status is always Open
- Amount is the Stripe Amount multiplied by Stripe Subscription Quantity
