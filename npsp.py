from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from io import StringIO
from typing import Optional, Union

import requests
from fuzzywuzzy import process
from pydantic import EmailStr, HttpUrl
from pytz import timezone
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

ZONE = timezone(os.environ.get("TIMEZONE", "US/Central"))


TWOPLACES = Decimal(10) ** -2  # same as Decimal('0.01')

# this should match whatever record type Salesforce's NPSP is
# configured to use for opportunities on an RDO
DEFAULT_RDO_TYPE = os.environ.get("DEFAULT_RDO_TYPE", "Membership")

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class SalesforceException(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.content = None
        self.response = None


class SalesforceConfig:
    def __init__(self, client_id=None, client_secret=None, username=None, password=None, api_version="v45.0", host="test.salesforce.com"):
        self.api_version = api_version
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.host = host


class SalesforceConnection:

    """
    Represents the Salesforce API.
    """

    def __init__(self, config):

        # TODO just attach the config object directly?
        self.host = config.host
        self.api_version = config.api_version

        self.session = requests.Session()
        self.retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504], method_whitelist=False)
        self.session.mount(f"https://{self.host}", HTTPAdapter(max_retries=self.retries))

        self.payload = {
            "grant_type": "password",
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "username": config.username,
            "password": config.password,
        }
        token_path = "/services/oauth2/token"
        self.url = f"https://{self.host}{token_path}"

        self._instance_url = None
        self.headers = None
        self._api_call_count = 0

    def _increment_api_call_count(self):
        self._api_call_count += 1

    def _get_token(self):

        r = self.session.post(self.url, data=self.payload)
        self._increment_api_call_count()
        self.check_response(r)
        response = json.loads(r.text)

        self._instance_url = response["instance_url"]
        access_token = response["access_token"]

        self.headers = {"Authorization": f"Bearer {access_token}", "X-PrettyPrint": "1", "Content-Type": "application/json"}

    @property
    def instance_url(self):
        if not self._instance_url:
            self._get_token()
        return self._instance_url

    def test_connection(self):
        self.instance_url

    @staticmethod
    def check_response(response=None, expected_statuses: list = None) -> bool:
        """
        Check the response from API calls to determine if they succeeded and
        if not, why.
        """
        if expected_statuses is None:
            expected_statuses = [200]

        code = response.status_code
        if code in expected_statuses:
            return True
        try:
            content = json.loads(response.content.decode("utf-8"))
        except Exception as e:
            logger.exception("Exception in check_response")

        e = SalesforceException(f"Expected one of {expected_statuses} but got {code}")
        try:
            e.content = content[0]
        except NameError:
            e.content = None
        except KeyError:
            e.content = content
        e.response = response
        logger.info("response.text: %s", response.text)
        raise e

    def query(self, query, path=None):

        """
        Call the Salesforce API to do SOQL queries.
        """
        if path is None:
            path = f"/services/data/{self.api_version}/query"

        url = f"{self.instance_url}{path}"
        if query is None:
            payload = {}
        else:
            payload = {"q": query}
        logger.debug(query)
        r = self.session.get(url, headers=self.headers, params=payload)
        self._increment_api_call_count()

        try:
            self.check_response(r)
        except SalesforceException as e:
            if e.content["errorCode"] == "INVALID_SESSION_ID":
                # token has probably expired; get a new one
                self._get_token()
                self._increment_api_call_count()
                r = self.session.get(url, headers=self.headers, params=payload)
                self.check_response(response=r, expected_statuses=[200])
            else:
                raise
        response = json.loads(r.text)
        # recursively get the rest of the records:
        if response["done"] is False:
            return response["records"] + self.query(query=None, path=response["nextRecordsUrl"])
        logger.debug(response)
        return response["records"]

    def post(self, path, data, expected_statuses=None):
        """
        Call the Salesforce API to make inserts.
        """
        if expected_statuses is None:
            expected_statuses = [201]
        url = f"{self.instance_url}{path}"
        resp = self.session.post(url, headers=self.headers, data=json.dumps(data))
        self._increment_api_call_count()
        try:
            self.check_response(response=resp, expected_statuses=expected_statuses)
        except SalesforceException as e:
            if e.content["errorCode"] == "INVALID_SESSION_ID":
                # token has probably expired; get a new one
                self._get_token()
                self._increment_api_call_count()
                resp = self.session.post(url, headers=self.headers, data=json.dumps(data))
                self.check_response(response=resp, expected_statuses=expected_statuses)
            else:
                raise
        response = json.loads(resp.text)
        logger.debug(response)
        return response

    def patch(self, path, data, expected_statuses=None):
        """
        Call the Saleforce API to make updates.
        """
        if expected_statuses is None:
            expected_statuses = [204]

        url = f"{self.instance_url}{path}"
        logger.debug(data)
        response = self.session.patch(url, headers=self.headers, data=json.dumps(data))
        self._increment_api_call_count()

        try:
            self.check_response(response=response, expected_statuses=expected_statuses)
        except SalesforceException as e:
            if e.content["errorCode"] == "INVALID_SESSION_ID":
                # token has probably expired; get a new one
                self._get_token()
                self._increment_api_call_count()
                response = self.session.patch(url, headers=self.headers, data=json.dumps(data))
                self.check_response(response=response, expected_statuses=expected_statuses)
            else:
                raise
        return response

    def updates(self, objects, changes):

        if not objects:
            raise SalesforceException("at least one object must be specified")

        data = dict()
        # what should this value be?
        data["allOrNone"] = False
        records = list()
        for item in objects:
            record = dict()
            record["attributes"] = {"type": item.api_name}
            record["id"] = item.id_
            for k, v in changes.items():
                record[k] = v
            records.append(record)
        data["records"] = records
        path = f"/services/data/{self.api_version}/composite/sobjects/"
        response = self.patch(path, data, expected_statuses=[200])
        response = json.loads(response.text)
        logger.debug(response)
        error = False
        for item in response:
            if item["success"] is not True:
                logger.warning("%s", item["errors"])
                error = item["errors"]
        if error:
            raise SalesforceException(f"Failure on update: {error}")

        return response

    def get(self, cls, identifier, external_id=None, expected_statuses=None, fields=None):
        """
        Call the Saleforce API to retrieve objects.
        """
        if expected_statuses is None:
            expected_statuses = [200, 404]

        path = f"/services/data/{self.api_version}/sobjects/{cls.api_name}"
        if external_id:
            path += f"/{external_id}/{identifier}"
        else:
            path += f"/{identifier}"

        url = f"{self.instance_url}{path}"
        if fields:
            url += "?{','.join(fields)}"
        logger.debug(url)
        response = self.session.get(url, headers=self.headers)
        self._increment_api_call_count()

        try:
            self.check_response(response=response, expected_statuses=expected_statuses)
        except SalesforceException as e:
            if e.content["errorCode"] == "INVALID_SESSION_ID":
                # token has probably expired; get a new one
                self._get_token()
                self._increment_api_call_count()
                response = self.session.get(url, headers=self.headers)
                self.check_response(response=response, expected_statuses=expected_statuses)

        if response.status_code == 404 and 404 in expected_statuses:
            return None
        resp = json.loads(response.text)
        return resp

    # composite

    def composite(self):

        foo = {
            "compositeRequest": [
                {"method": "GET", "url": "/services/data/v45.0/sobjects/Identity__c/Email__c/danielc@pobox.com", "referenceId": "Identity"},
                {"method": "GET", "url": "/services/data/v45.0/sobjects/Contact/@{Identity.ContactId__c}", "referenceId": "Contact"},
                {"method": "GET", "url": "/services/data/v45.0/sobjects/Account/@{Contact.AccountId}", "referenceId": "Account"},
            ]
        }
        path = f"/services/data/{self.api_version}/composite/"
        # TODO: retry on expired token
        self.post(path, foo, expected_statuses=[200])

    def save(self, sf_object):

        if sf_object.id_:
            logger.info("%s object %s already exists; updating...", sf_object.api_name, sf_object.id_)
            path = f"/services/data/{self.api_version}/sobjects/{sf_object.api_name}/{sf_object.id_}"
            logger.debug("patch data %s", sf_object._format())
            response = self.patch(path=path, data=sf_object._format())
            try:
                self.check_response(response=response, expected_statuses=[204])
            except SalesforceException as e:
                if e.content["errorCode"] == "INVALID_SESSION_ID":
                    # token has probably expired; get a new one
                    self._get_token()
                    response = self.patch(path=path, data=sf_object._format())
                    self.check_response(response=response, expected_statuses=[204])
                else:
                    raise
            sf_object.tainted = []
            return sf_object

        logger.info("%s object doesn't exist; creating...", sf_object.api_name)
        path = f"/services/data/{self.api_version}/sobjects/{sf_object.api_name}"
        logger.debug(sf_object._format())
        response = self.post(path=path, data=sf_object._format())
        sf_object.id_ = response["id"]
        sf_object.created = True
        return sf_object

    def delete(self, sf_object) -> None:

        logger.warning("Removing %s %s ...", sf_object.api_name, sf_object.id_)
        path = f"/services/data/{self.api_version}/sobjects/{sf_object.api_name}/{sf_object.id_}"
        url = f"{self.instance_url}{path}"
        response = self.session.delete(url, headers=self.headers)
        self._increment_api_call_count()
        try:
            self.check_response(response=response, expected_statuses=[204])
        except SalesforceException as e:
            if e.content["errorCode"] == "INVALID_SESSION_ID":
                # token has probably expired; get a new one
                self._get_token()
                self._increment_api_call_count()
                response = self.session.delete(url, headers=self.headers)
                self.check_response(response=response, expected_statuses=[204])
            else:
                raise


class SalesforceObject:
    """
    This is the parent of all the other Salesforce objects.
    """

    def _format(self) -> dict:
        raise NotImplementedError

    def __repr__(self):
        obj = self._format()
        obj["Id"] = self.id_
        return json.dumps(obj)

    def __str__(self):
        return self.id_

    def __init__(self, sf_connection):
        self.id_ = None
        self.created = False
        self.tainted = []
        self.sf = sf_connection

    def delete(self):
        self.sf.delete(self)

    def save(self):
        self.sf.save(self)


class Opportunity(SalesforceObject):

    api_name = "Opportunity"

    def __init__(
        self,
        sf_connection,
        record_type_name="Membership",
        contact=None,
        stage_name="Pledged",
        close_date=None,
        account_id=None,
        contact_id_for_role=None,
        campaign_id=None,
        name=None,
        lead_source=None,
        recurring_donation_frequency=None,
        primary_contact=None,
    ):
        super().__init__(sf_connection)

        if contact and account_id:  # this is the contact object because we want some other fields on it later
            raise SalesforceException("Account and Contact can't both be specified")

        today = datetime.now(tz=ZONE).strftime("%Y-%m-%d")
        if close_date is None:
            self.close_date = today
        else:
            self.close_date = close_date

        if account_id is not None:
            self.account_id = account_id
        elif contact is not None:
            self.account_id = contact.account_id
            if not name:  # specify a name if one isn't given on constructor
                self.name = f"{contact.first_name} {contact.last_name} ({contact.email})"
        else:
            self.account_id = None

        self.id_ = None
        self._amount = 0
        self.campaign_id = campaign_id
        self.record_type_name = record_type_name
        self.stage_name = stage_name
        self.type_ = "Single"
        self.stripe_customer_id = None
        self.lead_source = lead_source
        self.description = None
        self.agreed_to_pay_fees = False
        self.encouraged_by = None
        self.stripe_card = None
        self.stripe_transaction_id = None
        self.expected_giving_date = None
        self.closed_lost_reason = None
        self.amazon_order_id = None
        self.contact_id_for_role = contact_id_for_role
        self.name = name
        self.recurring_donation_frequency = None
        self.recurring_donation_frequency = recurring_donation_frequency
        self.record_type_id = None
        self.primary_contact = primary_contact

    @classmethod
    def get(cls, sf_connection, id_=None) -> Optional[Opportunity]:

        sf = sf_connection

        if not single_option_given([id_]):
            raise SalesforceException("exactly one of id_ and must be specified")

        if id_:
            response = sf.get(cls, identifier=id_)
            if not response:
                return None
        else:
            query = f"""
                SELECT Id, Amount, CloseDate, AccountId, CampaignId, RecordType.Name, StageName, Name, Type,
                    LeadSource, npe01__Contact_Id_for_Role__c, Recurring_Donation_Frequency__c,
                    npsp__Primary_Contact__c
                FROM Opportunity
                WHERE EventbriteSync__EventbriteId__c = '{eventbritesync_eventbriteid}'
            """
            response = sf.query(query)

            if not response:
                return None

            if len(response) > 1:
                raise SalesforceException("More than one Opportunity found")
            response = response[0]

        opportunity = Opportunity(
            close_date=response["CloseDate"],
            account_id=response["AccountId"],
            campaign_id=response["CampaignId"],
            stage_name=response["StageName"],
            name=response["Name"],
            contact_id_for_role=response["npe01__Contact_Id_for_Role__c"],
            recurring_donation_frequency=response["Recurring_Donation_Frequency__c"],
            primary_contact=response["npsp__Primary_Contact__c"],
            sf_connection=sf_connection,
        )
        if "RecordType" in response:
            opportunity.record_type_name = response["RecordType"]["Name"]
        else:
            opportunity.record_type_id = response["RecordTypeId"]

        opportunity._amount = response["Amount"]
        opportunity.type_ = response["Type"]
        opportunity.lead_source = response["LeadSource"]

        opportunity.id_ = response["Id"]
        return opportunity

    @classmethod
    def get_or_create(
        cls,
        contact_id_for_role,
        account_id,
        amount,
        close_date,
        name,
        campaign_id,
        sf_connection,
        stage_name,
        lead_source="Eventbrite",
        record_type_name="Donation",
    ):
        opportunity = cls.get(sf_connection=sf_connection)
        if opportunity:
            return opportunity

        opportunity = Opportunity(
            sf_connection=sf_connection,
            name=name,
            close_date=close_date,
            contact_id_for_role=contact_id_for_role,
            account_id=account_id,
            campaign_id=campaign_id,
            stage_name=stage_name,
            lead_source=lead_source,
            record_type_name=record_type_name,
        )
        opportunity._amount = amount

        opportunity.save()
        return opportunity

    @classmethod
    def list(cls, sf_connection, account_id=None, begin=None, end=None, stage_name="Pledged", stripe_customer_id=None):

        # TODO a more generic dserializing method
        # TODO allow filtering by anything that uses equality?

        sf = sf_connection

        # if account id is specified then we want all transactions regardless of stage:
        if account_id is not None:
            where = f"""
            WHERE AccountId = '{account_id}'
            """
        elif stripe_customer_id is None:
            where = f"""
            WHERE Expected_Giving_Date__c <= {end}
            AND Expected_Giving_Date__c >= {begin}
            AND StageName = '{stage_name}'
        """
        else:
            where = f"""
                WHERE Stripe_Customer_ID__c = '{stripe_customer_id}'
                AND StageName = '{stage_name}'
            """

        query = f"""
            SELECT
                Id,
                Amount,
                Name,
                Stripe_Customer_ID__c,
                StageName,
                Description,
                Stripe_Agreed_to_pay_fees__c,
                CloseDate,
                CampaignId,
                RecordType.Name,
                Type,
                LeadSource,
                Encouraged_to_contribute_by__c,
                Stripe_Transaction_ID__c,
                Stripe_Card__c,
                AccountId,
                npsp__Closed_Lost_Reason__c,
                Expected_Giving_Date__c,
                Amazon_Order_Id__c,
                Recurring_Donation_Frequency__c,
                npe01__Contact_Id_for_Role__c,
                npsp__Primary_Contact__c
            FROM Opportunity
            {where}
        """

        response = sf.query(query)
        logger.debug(response)

        results = list()
        for item in response:
            y = cls(sf_connection=sf_connection)
            y.id_ = item["Id"]
            y.name = item["Name"]
            y.amount = item["Amount"]
            y.stripe_customer_id = item["Stripe_Customer_ID__c"]
            y.description = item["Description"]
            y.agreed_to_pay_fees = item["Stripe_Agreed_to_pay_fees__c"]
            y.stage_name = item["StageName"]
            y.close_date = item["CloseDate"]
            y.record_type_name = item["RecordType"]["Name"]
            y.expected_giving_date = item["Expected_Giving_Date__c"]
            y.campaign_id = item["CampaignId"]
            y.type_ = item["Type"]
            y.lead_source = item["LeadSource"]
            y.encouraged_by = item["Encouraged_to_contribute_by__c"]
            y.stripe_transaction_id = item["Stripe_Transaction_ID__c"]
            y.stripe_card = item["Stripe_Card__c"]
            y.account_id = item["AccountId"]
            y.closed_lost_reason = item["npsp__Closed_Lost_Reason__c"]
            y.amazon_order_id = item["Amazon_Order_Id__c"]
            y.contact_id_for_role = item["npe01__Contact_Id_for_Role__c"]
            y.recurring_donation_frequency = item["Recurring_Donation_Frequency__c"]
            y.primary_contact = item["npsp__Primary_Contact__c"]
            results.append(y)

        return results

    @property
    def amount(self):
        return str(Decimal(self._amount).quantize(TWOPLACES))

    @amount.setter
    def amount(self, amount):
        self._amount = amount

    def _format(self) -> dict:
        return {
            "AccountId": self.account_id,
            "Amount": self.amount,
            "CloseDate": self.close_date,
            "CampaignId": self.campaign_id,
            "RecordType": {"Name": self.record_type_name},
            "Name": self.name,
            "StageName": self.stage_name,
            "Type": self.type_,
            "Stripe_Customer_ID__c": self.stripe_customer_id,
            "LeadSource": self.lead_source,
            "Description": self.description,
            "Stripe_Agreed_to_pay_fees__c": self.agreed_to_pay_fees,
            "Encouraged_to_contribute_by__c": self.encouraged_by,
            "Stripe_Transaction_ID__c": self.stripe_transaction_id,
            "Stripe_Card__c": self.stripe_card,
            "npsp__Closed_Lost_Reason__c": self.closed_lost_reason,
            "Amazon_Order_Id__c": self.amazon_order_id,
            "npe01__Contact_Id_for_Role__c": self.contact_id_for_role,
            "npsp__Primary_Contact__c": self.primary_contact,
        }

    @classmethod
    def update_card(cls, sf_connection, opportunities, card_details):
        if not opportunities:
            raise SalesforceException("at least one Opportunity must be specified")
        sf = sf_connection
        return sf.updates(opportunities, card_details)

    def __str__(self):
        return f"{self.id_}: {self.name} for {self.amount} ({self.description})"

    def save(self):

        # TODO this will fail if name hasn't been set
        # truncate to 80 chars:
        self.name = self.name[:80]

        if self.account_id is None:
            raise SalesforceException("Account ID must be specified")
        if not self.name:
            raise SalesforceException("Opportunity name must be specified")

        try:
            self.sf.save(self)
            # TODO should the client decide what's retryable?
        except SalesforceException as e:
            if e.content["errorCode"] == "MALFORMED_ID":
                if e.content["fields"][0] == "CampaignId":
                    logger.warning("bad campaign ID; retrying...")
                    self.campaign_id = None
                    self.save()
                elif e.content["fields"][0] == "Referral_ID__c":
                    logger.warning("bad referral ID; retrying...")
                    self.save()
                else:
                    raise
            else:
                raise


class RDO(SalesforceObject):
    """
    Recurring Donation objects.
    """

    api_name = "npe03__Recurring_Donation__c"

    def __init__(self, sf_connection, id_=None, contact=None, account_id=None):
        super().__init__(sf_connection=sf_connection)

        if account_id and contact:
            raise SalesforceException("Account and Contact can't both be specified")

        today = datetime.now(tz=ZONE).strftime("%Y-%m-%d")

        if contact is not None:
            self.contact_id = contact.id_
            self.name = f"{today} for {contact.first_name} {contact.last_name} ({contact.email})"
            self.account_id = None
        elif account_id is not None:
            self.account_id = account_id
            self.name = None
            self.contact_id = None
        else:
            self.name = None
            self.account_id = None
            self.contact_id = None

        self.id_ = id_
        self.installments: Union[int, None] = None
        self.open_ended_status = None
        self.installment_period = None
        self.campaign_id = None
        self._amount = 0
        self.type_ = "Recurring Donation"
        self.date_established = today
        self.stripe_customer_id = None
        self.lead_source = None
        self.description = None
        self.agreed_to_pay_fees = False
        self.encouraged_by = None
        self.record_type_name = None
        self.day_of_month = None

        self.next_payment_date = None
        self.name = None

    @classmethod
    def get(cls, sf_connection, id_=None, stripe_customer_id=None):

        if not single_option_given([id_, stripe_customer_id]):
            raise SalesforceException("exactly one of id_ and stripe_customer_id must be specified")

        response = None
        sf = sf_connection
        if id_:
            response = sf.get(cls, identifier=id_)
        elif stripe_customer_id:
            query = f"""
                SELECT
                    Id,
                    npe03__Installment_Period__c,
                    npe03__Amount__c,
                    Type__c,
                    Stripe_Customer_Id__c,
                    npe03__Open_Ended_Status__c,
                    npe03__Next_Payment_Date__c,
                    npe03__Contact__c,
                    Name

                FROM {cls.api_name}
                WHERE Stripe_Customer_Id__c = '{stripe_customer_id}'

            """

            response = sf.query(query)
            if response is None or len(response) == 0:
                return None
            if len(response) > 1:
                raise SalesforceException("More than one RDO found")
            response = response[0]

        rdo = cls(sf_connection=sf_connection)
        rdo.id_ = response["Id"]
        rdo.installment_period = response["npe03__Installment_Period__c"]
        rdo.amount = response["npe03__Amount__c"]
        rdo.type_ = response["Type__c"]
        rdo.next_payment_date = response["npe03__Next_Payment_Date__c"]
        rdo.stripe_customer_id = response["Stripe_Customer_Id__c"]
        rdo.open_ended_status = response["npe03__Open_Ended_Status__c"]
        rdo.contact_id = response["npe03__Contact__c"]
        rdo.name = response["Name"]

        return rdo

    @classmethod
    def list(cls, sf_connection, contact_id):
        sf = sf_connection

        query = f"""
            SELECT
                Id,
                npe03__Installment_Period__c,
                npe03__Amount__c,
                Type__c,
                Stripe_Customer_Id__c,
                npe03__Open_Ended_Status__c,
                npe03__Next_Payment_Date__c,
                npe03__Contact__c,
                Name

            FROM {cls.api_name}
            WHERE npe03__Contact__c = '{contact_id}'

        """
        response = sf.query(query)
        logger.debug(response)

        results = list()
        for item in response:
            y = cls(sf_connection=sf_connection)
            y.id_ = item["Id"]
            y.installment_period = item["npe03__Installment_Period__c"]
            y.amount = item["npe03__Amount__c"]
            y.type_ = item["Type__c"]
            y.next_payment_date = item["npe03__Next_Payment_Date__c"]
            y.stripe_customer_id = item["Stripe_Customer_Id__c"]
            y.open_ended_status = item["npe03__Open_Ended_Status__c"]
            y.contact_id = item["npe03__Contact__c"]
            y.name = item["Name"]
            results.append(y)

        return results

    def _format(self) -> dict:

        # TODO be sure to reverse this on deserialization
        amount = self.amount

        # TODO should this be in the client?
        if self.installments:
            amount = str(float(self.amount) * int(self.installments))

        recurring_donation = {
            "npe03__Organization__c": self.account_id,
            "npe03__Recurring_Donation_Campaign__c": self.campaign_id,
            "npe03__Contact__c": self.contact_id,
            "npe03__Amount__c": amount,
            "npe03__Date_Established__c": self.date_established,
            "Name": self.name,
            "Stripe_Customer_Id__c": self.stripe_customer_id,
            "Lead_Source__c": self.lead_source,
            "Stripe_Description__c": self.description,
            "Stripe_Agreed_to_pay_fees__c": self.agreed_to_pay_fees,
            "Encouraged_to_contribute_by__c": self.encouraged_by,
            "npe03__Open_Ended_Status__c": self.open_ended_status,
            "npe03__Installments__c": self.installments,
            "npe03__Installment_Period__c": self.installment_period,
            "Type__c": self.type_,
        }

        # figure out if the system has enhanced recurring donations
        query = "SELECT npsp__IsRecurringDonations2Enabled__c FROM npe03__Recurring_Donations_Settings__c"
        response = self.sf.query(query)
        enhanced = response[0]["npsp__IsRecurringDonations2Enabled__c"]

        if enhanced:
            logger.info(f"enhanced recurring donations")
            recurring_donation["npsp__Day_of_Month__c"] = self.day_of_month
            recurring_donation["npsp__InstallmentFrequency__c"] = 1
            recurring_donation["npsp__StartDate__c"] = self.date_established

        return recurring_donation

    def __str__(self):
        return f"{self.id_}: {self.name} for {self.amount} ({self.description})"

    # TODO sensible way to cache this to prevent it from being run multiple times when nothing
    # has changed? The opportunities themselves may've changed even when the RDO hasn't so
    # this may not be doable.

    def opportunities(self):
        query = f"""
            SELECT Id, Amount, Name, Stripe_Customer_ID__c, Description,
            Stripe_Agreed_to_pay_fees__c, CloseDate, CampaignId,
            RecordType.Name, Type, LeadSource,
            Encouraged_to_contribute_by__c, Stripe_Transaction_ID__c,
            Stripe_Card__c, AccountId, npsp__Closed_Lost_Reason__c,
            Expected_Giving_Date__c,
            StageName,
            FROM Opportunity
            WHERE npe03__Recurring_Donation__c = '{self.id_}'
        """
        # TODO must make this dynamic
        response = self.sf.query(query)
        results = list()
        for item in response:
            y = Opportunity(sf_connection=self.sf)
            y.id_ = item["Id"]
            y.name = item["Name"]
            y.amount = item["Amount"]
            y.stripe_customer_id = item["Stripe_Customer_ID__c"]
            y.description = item["Description"]
            y.agreed_to_pay_fees = item["Stripe_Agreed_to_pay_fees__c"]
            y.stage_name = item["StageName"]
            y.close_date = item["CloseDate"]
            y.record_type_name = item["RecordType"]["Name"]
            y.expected_giving_date = item["Expected_Giving_Date__c"]
            y.campaign_id = item["CampaignId"]
            y.type_ = item["Type"]
            y.lead_source = item["LeadSource"]
            y.encouraged_by = item["Encouraged_to_contribute_by__c"]
            y.stripe_transaction_id = item["Stripe_Transaction_ID__c"]
            y.stripe_card = item["Stripe_Card__c"]
            y.account_id = item["AccountId"]
            y.closed_lost_reason = item["npsp__Closed_Lost_Reason__c"]
            results.append(y)
        return results

    @property
    def amount(self):
        return str(Decimal(self._amount).quantize(TWOPLACES))

    @amount.setter
    def amount(self, amount):
        self._amount = amount

    def save(self):
        # truncate to 80 characters
        self.name = self.name[:80]

        if self.account_id is None and self.contact_id is None:
            raise SalesforceException("One of Contact ID or Account ID must be specified.")

        try:
            self.sf.save(self)
        except SalesforceException as e:
            if e.content["errorCode"] == "MALFORMED_ID":
                if e.content["fields"][0] == "npe03__Recurring_Donation_Campaign__c":
                    logger.warning("bad campaign ID; retrying...")
                    self.campaign_id = None
                    self.save()
                elif e.content["fields"][0] == "Referral_ID__c":
                    logger.warning("bad referral ID; retrying...")
                    self.save()
                else:
                    raise
            else:
                raise

        # since NPSP doesn't let you pass through the record
        # type ID of the opportunity (it will only use one hard-coded value)
        # we set them for all of the opportunities here. But if the RDO
        # is open ended then it'll create new opportunities of the wrong
        # type on its own. We warn about that.
        #
        # You should fix this through
        # process builder/mass action scheduler or some other process on the
        # SF side
        if self.record_type_name == DEFAULT_RDO_TYPE or self.record_type_name is None:
            return
        if self.open_ended_status == "Open":
            logger.info("RDO %s is open-ended so new opportunities won't have type %s", self, self.record_type_name)
            return
        logger.info("Setting record type for %s opportunities to %s", self, self.record_type_name)
        update = {"RecordType": {"Name": self.record_type_name}}
        self.sf.updates(self.opportunities(), update)


class Account(SalesforceObject):

    api_name = "Account"

    def __init__(self, sf_connection):
        super().__init__(sf_connection)

        self.id_ = None
        self.name = None
        self.website = None
        self.shipping_street = None
        self.shipping_city = None
        self.shipping_postalcode = None
        self.shipping_state = None
        self.record_type_name = "Household"
        self.record_type_id = None

    def _format(self) -> dict:
        return {
            "Website": self.website,
            "RecordType": {"Name": self.record_type_name},
            "Name": self.name,
            "ShippingStreet": self.shipping_street,
            "ShippingCity": self.shipping_city,
            "ShippingPostalCode": self.shipping_postalcode,
            "ShippingState": self.shipping_state,
        }

    def __str__(self):
        return f"{self.id_}: {self.name} ({self.website})"

    @classmethod
    def get_or_create(
        cls,
        sf_connection,
        record_type_name="Household",
        website=None,
        name=None,
        shipping_city=None,
        shipping_street=None,
        shipping_state=None,
        shipping_postalcode=None,
        record_type_id=None,
    ):
        account = cls.get(record_type_name=record_type_name, website=website, sf_connection=sf_connection)
        if account:
            return account
        account = Account(sf_connection=sf_connection)
        account.website = website
        account.name = name
        account.shipping_city = shipping_city
        account.shipping_postalcode = shipping_postalcode
        account.shipping_state = shipping_state
        account.shipping_street = shipping_street
        account.record_type_name = record_type_name
        account.record_type_id = record_type_id
        account.save()
        return account

    @classmethod
    def get(cls, sf_connection, record_type_name="Household", id_=None, website=None):
        sf = sf_connection

        if not single_option_given([id_, website]):
            raise SalesforceException("exactly one of id_ and website must be specified")

        if id_:
            response = sf.get(cls, identifier=id_)
            if response is None:
                return None
            account = Account(sf_connection=sf_connection)
            account.website = response["Website"]
            account.id_ = response["Id"]
            account.record_type_id = response["RecordTypeId"]
            account.name = response["Name"]
            account.shipping_street = response["ShippingStreet"]
            account.shipping_city = response["ShippingCity"]
            account.shipping_postalcode = response["ShippingPostalCode"]
            account.shipping_state = response["ShippingState"]

            return account

        else:
            query = f"""
                SELECT Id, Name, Website
                FROM Account WHERE
                RecordType.Name IN ('{record_type_name}')
            """
            response = sf.query(query)

            # We do a fuzzy search on the website and if the top hit
            # has a confidence of 95 or higher we use it.
            website_idx = {
                x["Website"]: {"id": x["Id"], "name": x["Name"]} for x in response if x["Website"] is not None and x["Website"] != "NULL"
            }
            url_list = list(website_idx.keys())

            extracted = process.extractOne(website, url_list)
            logger.debug(extracted)
            if extracted is None:
                return None
            url, confidence = extracted
            if confidence < 95:
                return None

            account = Account(sf_connection=sf_connection)
            account.id_ = website_idx[url]["id"]
            account.name = website_idx[url]["name"]
            account.website = url

            # TODO: deserialize the rest?

            return account


class Contact(SalesforceObject):

    api_name = "Contact"

    def __init__(self, sf_connection, id_=None, last_name="Subscriber"):
        super().__init__(sf_connection)

        self.id_ = id_
        self.sf_connection = sf_connection
        self.account_id = None
        self.first_name = None
        self.last_name = last_name
        self.email = None
        self.lead_source = "Stripe"
        self.duplicate_found = False
        self.work_email = None
        self.mailing_city = None
        self.mailing_country = None
        self.mailing_postal_code = None
        self.mailing_street = None
        self.mailing_state = None
        self.opp_amount_last_year = None  # read only

    #    def __str__(self):
    #        serializable = self.__dict__.copy()
    #        del serializable["sf"]
    #        del serializable["sf_connection"]
    #        return json.dumps(serializable)

    @property
    def name(self):
        return f"{self.first_name} {self.last_name}"

    @staticmethod
    def parse_all_email(email, results):
        """
        This field is a CSV. So we parse that to make sure we've got an exact match and not just a substring match.
        """
        filtered_results = list()
        for item in results:
            all_email = item["Concatenated_Emails__c"].lower()
            buffer = StringIO(all_email)
            reader = csv.reader(buffer, skipinitialspace=True)
            if email.lower() in list(reader)[0]:
                filtered_results.append(item)
        return filtered_results

    @property
    def mailing_address(self):
        return f"{self.mailing_street}, {self.mailing_city}, {self.mailing_state}, {self.mailing_postal_code}, {self.mailing_country}"

    def _format(self) -> dict:
        return {
            "Email": self.email,
            "FirstName": self.first_name,
            "LastName": self.last_name,
            "LeadSource": self.lead_source,
            "AccountId": self.account_id,
            "MailingPostalCode": self.mailing_postal_code,
            "MailingCity": self.mailing_city,
            "MailingCountry": self.mailing_country,
            "MailingState": self.mailing_state,
            "MailingStreet": self.mailing_street,
            "npe01__WorkEmail__c": self.work_email,
        }

    @classmethod
    def get_or_create(cls, email, sf_connection, first_name=None, last_name=None):
        last_name = last_name or "Subscriber"  # SF requires a last name and this is already being used by the MC connector
        contact = cls.get(sf_connection=sf_connection, email=email.lower())
        if contact:
            return contact
        contact = Contact(sf_connection=sf_connection)
        contact.email = email.lower()
        contact.first_name = first_name
        contact.last_name = last_name
        contact.save()
        return contact

    @classmethod
    def get(cls, sf_connection, id_=None, email=None):

        sf = sf_connection

        if id_ is None and email is None:
            raise SalesforceException("id_ or email must be specified")
        if id_ and email:
            raise SalesforceException("id_ and email can't both be specified")
        if id_:
            query = f"""
                    SELECT Id, AccountId, FirstName, LastName, LeadSource, Stripe_Customer_ID__c, MailingPostalCode,
                    npo02__OppAmountLastYear__c,
                    Email, npe01__WorkEmail__c, MailingCity, MailingState, MailingStreet, MailingCountry
                    FROM Contact
                    WHERE id = '{id_}'
                    """
            response = sf.query(query)
            # should only be one result here because we're
            # querying by id
            response = response[0]
            contact = Contact(sf_connection=sf_connection)
            contact.id_ = response["Id"]
            contact.account_id = response["AccountId"]
            contact.first_name = response["FirstName"]
            contact.last_name = response["LastName"]
            contact.email = response["Email"]
            contact.lead_source = response["LeadSource"]
            contact.mailing_postal_code = response["MailingPostalCode"]
            contact.work_email = response["npe01__WorkEmail__c"]
            contact.mailing_city = response["MailingCity"]
            contact.mailing_state = response["MailingState"]
            contact.mailing_street = response["MailingStreet"]
            contact.mailing_country = response["MailingCountry"]
            contact.opp_amount_last_year = response["npo02__OppAmountLastYear__c"]

            return contact

        query = f"""
                SELECT Id, AccountId, FirstName, LastName, LeadSource, MailingPostalCode,
                npo02__OppAmountLastYear__c,
                Concatenated_Emails__c, Email, npe01__WorkEmail__c,
                MailingCity, MailingState, MailingStreet, MailingCountry
                FROM Contact
                WHERE Concatenated_Emails__c
                LIKE '%{email}%'
                """

        response = sf.query(query)
        if not response:
            return None
        response = cls.parse_all_email(email=email.lower(), results=response)
        if not response:
            return None
        contact = Contact(sf_connection=sf_connection)
        if len(response) > 1:
            contact.duplicate_found = True
            logger.warning("Multiple contacts found for %s", email)
        response = response[0]
        contact.id_ = response["Id"]
        contact.account_id = response["AccountId"]
        contact.first_name = response["FirstName"]
        contact.last_name = response["LastName"]
        contact.email = response["Email"]
        contact.lead_source = response["LeadSource"]
        contact.work_email = response["npe01__WorkEmail__c"]
        contact.mailing_postal_code = response["MailingPostalCode"]
        contact.mailing_city = response["MailingCity"]
        contact.mailing_state = response["MailingState"]
        contact.mailing_street = response["MailingStreet"]
        contact.mailing_country = response["MailingCountry"]
        contact.opp_amount_last_year = response["npo02__OppAmountLastYear__c"]

        return contact

    #    def __str__(self):
    #        return f"{self.id} ({self.account_id}): {self.first_name} {self.last_name}"

    def save(self):
        self.sf.save(self)
        # TODO this is a workaround for now because creating a new
        # contact will also create a new account and we need that account ID
        # so we have to re-fetch the contact to get it
        tmp_contact = self.get(sf_connection=self.sf_connection, id_=self.id_)
        self.account_id = tmp_contact.account_id


class Affiliation(SalesforceObject):
    """
    This object is a link between a contact and an account.
    """

    api_name = "npe5__Affiliation__c"

    def __init__(self, sf_connection, contact_id=None, account_id=None, role=None):
        super().__init__(sf_connection)
        # TODO allow id to be set in __init__?
        self.id_ = None
        self.contact_id = contact_id
        self.account_id = account_id
        self.role = role

    @classmethod
    def get(cls, contact_id, account_id, sf_connection):

        sf = sf_connection

        query = f"""
            SELECT Id, npe5__Role__c from npe5__Affiliation__c
            WHERE npe5__Contact__c = '{contact_id}'
            AND npe5__Organization__c = '{account_id}'
        """
        response = sf.query(query)

        if not response:
            return None

        if len(response) > 1:
            raise SalesforceException("More than one affiliation found")
        role = response[0]["npe5__Role__c"]

        affiliation = Affiliation(sf_connection=sf_connection, contact_id=contact_id, account_id=account_id, role=role)
        affiliation.id_ = response[0]["Id"]
        return affiliation

    @classmethod
    def get_or_create(cls, sf_connection, account_id=None, contact_id=None, role=None):
        affiliation = cls.get(sf_connection=sf_connection, account_id=account_id, contact_id=contact_id)
        if affiliation:
            return affiliation
        affiliation = Affiliation(sf_connection=sf_connection, account_id=account_id, contact_id=contact_id, role=role)
        affiliation.save()
        return affiliation

    def __str__(self):
        return f"{self.id_}: {self.contact_id} is affiliated with {self.account_id} ({self.role})"

    def _format(self) -> dict:
        return {"npe5__Contact__c": self.contact_id, "npe5__Role__c": self.role, "npe5__Organization__c": self.account_id}


class Task(SalesforceObject):

    api_name = "Task"

    def __init__(self, sf_connection, owner_id=None, what_id=None, subject=None):
        super().__init__(sf_connection)
        self.owner_id = owner_id
        self.what_id = what_id
        self.subject = subject

    def __str__(self):
        return f"{self.subject}"

    def _format(self) -> dict:
        return {"OwnerId": self.owner_id, "WhatId": self.what_id, "Subject": self.subject}


class User(SalesforceObject):

    api_name = "User"

    def __str__(self):
        return f"{self.id_}: {self.username}"

    @classmethod
    def get(cls, username, sf_connection):

        sf = sf_connection

        query = f"""
            SELECT Id, Username FROM User
            WHERE username = '{username}'
        """
        response = sf.query(query)

        if not response:
            return None

        user = User(sf_connection=sf)
        user.id_ = response[0]["Id"]
        user.username = response[0]["Username"]
        return user


class Campaign(SalesforceObject):

    api_name = "Campaign"

    def __init__(self, sf_connection, type_="Event", status="Planned", name=None, record_type_name="Event", start_date=None):
        super().__init__(sf_connection)
        self.name = name
        self.start_date = start_date
        self.status = status
        self.type_ = type_
        self.record_type_name = record_type_name

    def __str__(self):
        return f"{self.id_}: {self.name} {self.eventbritesync_eventbriteid}"

    @classmethod
    def get(cls, sf_connection, id_=None):

        sf = sf_connection
        query = None

        if id_ is None and eventbritesync_eventbriteid is None:
            raise SalesforceException("id_ or eventbritesync_eventbriteid must be specified")
        if id_ and eventbritesync_eventbriteid:
            raise SalesforceException("id_ and eventbritesync_eventbriteid can't both be specified")
        if id_:
            query = f"""
            SELECT Id, Name, StartDate, Status, Type
            FROM Campaign
            WHERE Id = '{id_}'
            """
        elif eventbritesync_eventbriteid:
            query = f"""
            SELECT Id, Name, StartDate, Status, Type
            FROM Campaign
            WHERE EventbriteSync__EventbriteId__c = '{eventbritesync_eventbriteid}'
            """

        response = sf.query(query)

        if not response:
            return None

        campaign = Campaign(sf_connection=sf_connection)
        campaign.id_ = response[0]["Id"]
        campaign.start_date = response[0]["StartDate"]
        campaign.status = response[0]["Status"]
        campaign.type_ = response[0]["Type"]
        return campaign

    @classmethod
    def get_or_create(cls, sf_connection, name):
        campaign = cls.get(sf_connection=sf_connection)
        if campaign:
            return campaign
        campaign = Campaign(sf_connection=sf_connection, name=name)
        campaign.save()
        return campaign

    def _format(self) -> dict:
        return {
            "Status": self.status,
            "Type": self.type_,
            "RecordType": {"Name": self.record_type_name},
            "Name": self.name,
            "StartDate": self.start_date,
        }


class CampaignMember(SalesforceObject):

    api_name = "CampaignMember"

    def __init__(self, contact_id, campaign_id, sf_connection, status="Sent"):
        super().__init__(sf_connection)
        self.id_ = None
        self.contact_id = contact_id
        self.campaign_id = campaign_id
        self.status = status
        self.sf_connection = sf_connection

    @classmethod
    def get(cls, campaign_id, contact_id, sf_connection):

        sf = sf_connection

        query = f"""
            SELECT Id, ContactId, CampaignId, Status FROM CampaignMember
            WHERE ContactId = '{contact_id}' AND CampaignId = '{campaign_id}'
        """
        response = sf.query(query)

        if not response:
            return None

        if len(response) > 1:
            raise SalesforceException("More than one campaign member found")

        contact_id = response[0]["ContactId"]
        campaign_id = response[0]["CampaignId"]
        status = response[0]["Status"]

        campaign_member = CampaignMember(sf_connection=sf_connection, contact_id=contact_id, campaign_id=campaign_id, status=status)
        campaign_member.id_ = response[0]["Id"]
        return campaign_member

    @classmethod
    def get_or_create(cls, sf_connection, contact_id, campaign_id, status="Sent"):
        campaign_member = cls.get(sf_connection=sf_connection, contact_id=contact_id, campaign_id=campaign_id)
        if campaign_member:
            return campaign_member
        campaign_member = CampaignMember(sf_connection=sf_connection, contact_id=contact_id, status=status, campaign_id=campaign_id)
        campaign_member.save()
        return campaign_member

    def save(self):
        # why haven't I had to do this before?
        # if I don't I get:
        # "Unable to create/update fields: ContactId, CampaignId. Please check
        # the security settings of this field and verify that it is read/write
        # for your profile or permission set."
        if self.id_:
            logger.info("%s object already exists; updating...", self.api_name)
            path = f"/services/data/{self.sf_connection.api_version}/sobjects/{self.api_name}/{self.id_}"
            serialized = self._format()
            del serialized["CampaignId"]
            del serialized["ContactId"]
            try:
                self.sf.patch(path=path, data=serialized)
            except SalesforceException as e:
                logger.error(e.response.text)
                raise
            return self
        else:
            self.sf.save(self)

    def _format(self) -> dict:
        return {"ContactId": self.contact_id, "Status": self.status, "CampaignId": self.campaign_id}


def single_option_given(iterable):
    i = iter(iterable)
    return any(i) and not any(i)


class Identity(SalesforceObject):

    api_name = "Identity__c"

    def __init__(
        self,
        sf_connection,
        email,
        contact_id,
        auth0_user_id=None,
        auth0_verified=False,
        lead_source=None,
        marketing_consent=False,
        #        auth0_username=None,
        id_=None,
    ):
        super().__init__(sf_connection)
        self.id_ = id_
        self.email = email.lower()
        self.contact_id = contact_id
        self.auth0_user_id = auth0_user_id
        self.auth0_verified = auth0_verified
        self.lead_source = lead_source
        self.marketing_consent = marketing_consent
        #        self.auth0_username = auth0_username
        self.sf_connection = sf_connection

    def __str__(self):
        return f"{self.id_}/{self.email}/{self.auth0_user_id}"

    def construct_request(self, method, identifier=None, external_id=None, parent=None, parent_field=None):
        url = None
        reference_id = None
        url = f"/services/data/{self.sf_connection.api_version}/sobjects/"
        if external_id and identifier:
            url += f"{self.api_name}/{external_id}/{identifier}"
            reference_id = self.__class__.__name__
        elif not external_id and identifier:
            url += f"{self.api_name}/{identifier}"
            reference_id = self.__class__.__name__
        elif parent:
            url = (
                f"/services/data/{self.sf_connection.api_version}/sobjects/{parent.__name__}/@{{{self.__class__.__name__}.{parent_field}}}"
            )
            reference_id = parent.__name__
        request = {"method": method, "url": url, "referenceId": reference_id}
        return request

    @classmethod
    def list(cls, sf_connection: SalesforceConnection, contact_id: str):
        url = f"{sf_connection._instance_url}/services/data/{sf_connection.api_version}/sobjects/Contact/{contact_id}/Identities__r"
        logger.debug(url)
        response = sf_connection.session.get(url, headers=sf_connection.headers)
        sf_connection._increment_api_call_count()
        try:
            sf_connection.check_response(response=response, expected_statuses=[200])
        except SalesforceException as e:
            if e.content["errorCode"] == "INVALID_SESSION_ID":
                # token has probably expired; get a new one
                sf_connection._get_token()
                sf_connection._increment_api_call_count()
                response = sf_connection.session.get(url, headers=sf_connection.headers)
                sf_connection.check_response(response=response, expected_statuses=[200])

        response = json.loads(response.text)
        identities = list()
        for item in response["records"]:
            identity = Identity(sf_connection=sf_connection, email=item["Email__c"], contact_id=item["ContactId__c"])
            identity.id_ = item["Id"]
            identity.auth0_user_id = item["Auth0_User_ID__c"]
            identity.auth0_verified = item["Auth0_Verified__c"]
            identity.contact_id = item["ContactId__c"]
            identity.email = item["Email__c"]
            identity.lead_source = item["Lead_Source__c"]
            identity.marketing_consent = item["Marketing_Consent__c"]
            #            identity.auth0_username = item["Auth0_Username__c"]
            identities.append(identity)

        return identities

    @classmethod
    def get(cls, sf_connection, id_=None, email=None, auth0_user_id=None, prefetch=None):

        if not single_option_given([id_, email, auth0_user_id]):
            raise SalesforceException("exactly one of id_, email and auth0_user_id must be specified")

        email = email.lower() if email else None

        sf = sf_connection

        # "compositeRequest": [
        #     {
        #         "method": "GET",
        #         "url": "/services/data/v45.0/sobjects/Identity__c/Email__c/danielc@pobox.com",
        #         "referenceId": "Identity",
        #     },
        #     {
        #         "method": "GET",
        #         "url": "/services/data/v45.0/sobjects/Contact/@{Identity.ContactId__c}",
        #         "referenceId": "Contact",
        #     },
        #     {
        #         "method": "GET",
        #         "url": "/services/data/v45.0/sobjects/Account/@{Contact.AccountId}",
        #         "referenceId": "Account",
        #     },
        # ]

        # composite
        # all_requests = list()
        # if prefetch and email:
        #     first_request = construct_request(method="GET", identifier='Email__c', external_id="Email__c")
        #     all_requests.append(first_request)

        # for item in prefetch:
        #     request = construct_request(method="GET", identifier='Contact')
        #     all_requests.append(request)

        response = None

        if id_:
            response = sf.get(cls, identifier=id_)
        elif email:
            response = sf.get(cls, identifier=email.lower(), external_id="Email__c")
        elif auth0_user_id:
            response = sf.get(cls, identifier=auth0_user_id, external_id="Auth0_User_ID__c")

        # TODO: should get() return None or exception?
        if not response:
            return None
        identity = Identity(sf_connection=sf_connection, email=response["Email__c"], contact_id=response["ContactId__c"])
        identity.id_ = response["Id"]
        identity.auth0_user_id = response["Auth0_User_ID__c"]
        identity.auth0_verified = response["Auth0_Verified__c"]
        identity.contact_id = response["ContactId__c"]
        identity.email = response["Email__c"]
        identity.lead_source = response["Lead_Source__c"]
        identity.marketing_consent = response["Marketing_Consent__c"]
        #        identity.auth0_username = response["Auth0_Username__c"]

        return identity

    @classmethod
    def get_or_create(cls, email: EmailStr, sf_connection: SalesforceConnection, contact_id: str = None) -> Identity:
        # TODO: should get() return None or throw an exception?
        identity = cls.get(email=email.lower(), sf_connection=sf_connection)
        if identity:
            return identity
        identity = Identity(email=email.lower(), contact_id=contact_id, sf_connection=sf_connection)
        identity.save()
        return identity

    # TODO: implement
    @classmethod
    def upsert(cls):
        pass

    def _format(self) -> dict:
        return {
            "Auth0_User_ID__c": self.auth0_user_id,
            "Auth0_Verified__c": self.auth0_verified,
            "ContactId__c": self.contact_id,
            "Email__c": self.email,
            "Lead_Source__c": self.lead_source,
            "Marketing_Consent__c": self.marketing_consent,
            #            "Auth0_Username__c": self.auth0_username,
        }


class IdentityContactJunction(SalesforceObject):

    api_name = "IdentityContactJunction__c"

    def __init__(self, sf_connection, identity_id, contact_id, use=None, id_=None):
        super().__init__(sf_connection)
        self.id_ = id_
        self.identity_id = identity_id
        self.contact_id = contact_id
        self.use = use

    @classmethod
    def get(cls, sf_connection, id_=None, contact_id=None, identity_id=None, use=None):

        sf = sf_connection
        response = None
        if id_:
            response = sf.get(cls, identifier=id_)
            if not response:
                return None

        elif contact_id and identity_id and use:
            query = f"""
            SELECT Use__c, Id
            FROM IdentityContactJunction__c
            WHERE ContactId__c = '{contact_id}' AND IdentityId__c = '{identity_id}' AND Use__c = '{use}'
            """
            response = sf.query(query)
            if not response:
                return None
            if len(response) > 1:
                raise SalesforceException("More than one IdentityContactJunction found")
            response = response[0]
        else:
            raise SalesforceException("Wrong combination of parameters")
        return IdentityContactJunction(
            id_=response["Id"], sf_connection=sf_connection, contact_id=contact_id, identity_id=identity_id, use=use
        )

    @classmethod
    def list(cls, sf_connection, contact_id):
        sf = sf_connection

        query = f"""
            SELECT
                Id, Use__c, IdentityId__c
            FROM {cls.api_name}
            WHERE ContactId__c = '{contact_id}'
        """
        response = sf.query(query)
        logger.debug(response)

        results = list()
        for item in response:
            y = cls(sf_connection=sf_connection, identity_id=item["IdentityId__c"], contact_id=contact_id)
            y.id_ = item["Id"]
            y.use = item["Use__c"]
            results.append(y)

        return results

    @classmethod
    def get_or_create(cls, sf_connection, contact_id, identity_id, use):
        # TODO: should get() return None or throw an exception?
        icj = cls.get(sf_connection=sf_connection, contact_id=contact_id, identity_id=identity_id, use=use)
        if icj:
            return icj
        icj = IdentityContactJunction(sf_connection=sf_connection, contact_id=contact_id, identity_id=identity_id, use=use)
        icj.save()
        return icj

    def _format(self) -> dict:
        return {"ContactId__c": self.contact_id, "IdentityId__c": self.identity_id, "Use__c": self.use}


class OpportunityContactRole(SalesforceObject):

    api_name = "OpportunityContactRole"

    def __init__(self, sf_connection: SalesforceConnection, contact_id: str, id_=None):
        super().__init__(sf_connection)
        self.id_ = id_
        self.contact_id = contact_id

    @classmethod
    def list(cls, sf_connection: SalesforceConnection, contact_id: str):
        sf = sf_connection

        query = f"""
            SELECT
                Id, ContactId
            FROM {cls.api_name}
            WHERE ContactId = '{contact_id}'
        """
        response = sf.query(query)
        logger.debug(response)

        results = list()
        for item in response:
            y = cls(sf_connection=sf_connection, contact_id=item["ContactId"], id_=item["Id"])
            results.append(y)

        return results

    def _format(self) -> dict:
        return {"ContactId": self.contact_id}
