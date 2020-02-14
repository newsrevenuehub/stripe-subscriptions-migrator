These scripts are meant to migrate from Stripe Subscriptions to Salesforce recurring donatiion objects. 

It's run as two scripts for safety. The first (`extract.py`) captures all of the subscriptions and saves them to a CSV. You can review that CSV to make sure it has exactly what you were hoping to get out of Stripe. 

Once you're happy with the contents of the CSV you can run the script script (`migrate.py`). 

The `migrate.py` script will process each record of the CSV by first canceling the subscription and then entering it into Salesforce. It tries to avoid entering a duplicate by checking for an existing Salesforce record with the same Stripe Customer ID. There could be legitimate reasons why someone would have more than one recurring donation with the same Stripe Customer ID. In that case this script would break and would need to be modified ot properly process the records. 

The fields are converted as follows:

- Recurring Donation Name = current date followed by first+last if present, email otherwise
- Description = Stripe Subcription ID followed by the Stripe Plan Name
- Lead Source = Stripe
- Date Established = Stripe Subscription Current Period End
- Open Ended Status is always Open
- Amount is the Stripe Amount * Stripe Subscription Quantity
