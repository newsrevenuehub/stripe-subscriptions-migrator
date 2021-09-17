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

First run `dos2unix subscriptions.csv`. This will remove/convert any DOS characters that may complicate the rest of the process.

It's run as three scripts for safety.

1. The first (`extract.py`) captures all of the subscriptions and saves them to a CSV. You can review that CSV to make sure it has exactly what you were hoping to get out of Stripe. So run `python extract.py`. That will produce a file called `subscriptions.csv`. Each time you run the script it will overwrite that CSV. This first script is non-destructive and can be run as many times as needed.
1. Once you're happy with the contents of the CSV you can run the script to cancel the Stripe subscriptions (`python cancel.py`).
1. Then run the command to import all of the subscriptions into Salesforce: `python import.py`. It will try to avoid entering duplicates by checking for an existing Salesforce record with the same Stripe Customer ID. There could be legitimate reasons why someone would have more than one recurring donation with the same Stripe Customer ID. In that case this script would break and would need to be modified to properly process the records.

The fields are converted as follows:

- Recurring Donation Name = current date followed by first+last if present, email otherwise
- Description = Stripe Subcription ID followed by the Stripe Plan Name
- Lead Source = Stripe
- Date Established = Stripe Subscription Current Period End
- Open Ended Status is always Open
- Amount is the Stripe Amount multiplied by Stripe Subscription Quantity
