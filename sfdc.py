from math import ceil

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceExpiredSession
import json
import requests
import csv
import datetime
import itertools
import base64
import re
import transformations


class SFDCClient(object):
    conn = None
    logger = None
    schema = None
    username = None
    password = None
    token = None
    domain = None
    mappings = None
    file_objects = ['ContentDocument', 'ContentVersion', 'Attachment']
    # fields_to_skip = {"Attachment": ["Body"]}
    fields_to_skip = {}

    def __init__(self, username, password, token, domain, logger):
        self.logger = logger
        # TODO: Custom Mappings
        # with open('mappings.json') as json_mappings_file:
        #     self.mappings = json.load(json_mappings_file)
        try:
            self.username = username
            self.password = password
            self.token = token
            self.domain = domain
            self.create_connection()

        except Exception as e:
            self.logger.error('Error logging into Salesforce: %s', e)
            print('Error logging into Salesforce')

    def create_connection(self):
        self.conn = Salesforce(username=self.username, password=self.password, security_token=self.token,
                               domain=self.domain)

    def check_connection(self):
        try:
            res = self.conn.query("SELECT Id FROM User LIMIT 1")
        except SalesforceExpiredSession:
            self.create_connection()
        except Exception as e:
            self.logger.error('Error connecting to Salesforce: %s', e)

    def get_records(self, sfdc_object, limit=None, where_clause=None, field_list=None):
        all_fields_string = self.get_all_fields_string(sfdc_object)
        # TODO: this is a quick fix, not foolproof!
        if field_list:
            for field in field_list:
                if field not in all_fields_string:
                    field_list.remove(field)
        field_list_string = ','.join(field_list) if field_list else all_fields_string
        soql = "SELECT %s FROM %s" % (field_list_string, sfdc_object)
        if where_clause is not None:
            soql += ' WHERE %s' % where_clause
        if limit is not None:
            soql += ' LIMIT %s' % limit
        if sfdc_object == 'ContentVersion':
            soql = soql.replace('VersionData,', '')
        if sfdc_object == 'Attachment':
            soql = soql.replace('Body,', '')
        res = self.conn.bulk.__getattr__(sfdc_object).query(soql)
        return res

    def get_recordtypes(self, sfdc_object):
        soql = "SELECT Id, DeveloperName, Name FROM RecordType where SobjectType = '%s'" % sfdc_object
        res = self.conn.query(soql)
        return res["records"]

    def get_inactive_users(self):
        soql = "SELECT Id FROM User where isActive = false"
        res = self.conn.query(soql)
        return res["records"]

    def upload_records(self, sfdc_object, records,  external_id, upsert=True):
        try:
            if upsert:
                res = self.conn.bulk.__getattr__(sfdc_object).upsert(records, external_id)
            else:
                res = self.conn.bulk.__getattr__(sfdc_object).insert(records)
            self.logger.info(
                "Uploaded a batch of %s, please check the Bulk Data Load job status in Salesforce for results.",
                sfdc_object)
            return list(itertools.zip_longest(records, res, fillvalue=''))
        except Exception as e:
            self.logger.error('Error uploading %s into Salesforce: %s', sfdc_object, e)
            # self.logger.error('with records: %s', sfdc_records)
            return None

    def get_schema(self, sfdc_objects):
        schema = {}
        additional_objects = ['ContentVersion', 'Attachment', 'ContentDocumentLink']
        for obj in sfdc_objects:
            schema[obj] = {'fields': {}}
            for field in self.get_all_fields(obj):
                schema[obj]['fields'][field] = {}
        for obj in additional_objects:
            schema[obj] = {'fields': {}}
            for field in self.get_all_fields(obj):
                schema[obj]['fields'][field] = {}
        return schema

    def get_all_fields(self, sfdc_object):
        desc = self.conn.__getattr__(sfdc_object).describe()
        field_names = []
        for field in desc['fields']:
            if field['type'] != 'address' and (sfdc_object not in self.fields_to_skip \
                                               or field['name'] not in self.fields_to_skip[sfdc_object]):
                field_names.append(field['name'])

        return field_names

    def get_all_fields_string(self, sfdc_object):
        fields = self.get_all_fields(sfdc_object)
        return ','.join(fields)

    def get_fields_to_skip(self, object_name):
        if object_name not in self.fields_to_skip:
            return None
        else:
            return self.fields_to_skip[object_name]

    def get_filebody(self, content_link):
        url = "https://%s%s" % (self.conn.sf_instance, content_link)
        # print('Retrieving: ', url)
        try:
            response = requests.get(url, headers={"Authorization": "OAuth " + self.conn.session_id,
                                                  "Content-Type": "application/octet-stream"}, timeout=30)

            if response.ok:
                return response.content
            else:
                self.logger.error('Error retrieving file contents for %s', content_link)
                return
        except Exception as e:
            self.logger.error('Error retrieving file body for %s, %s', content_link, e)
            return


    def get_record_count(self, sfdc_object):
        soql = "SELECT count() FROM %s " % sfdc_object
        res = self.conn.query(soql)
        return res["totalSize"]

    def get_contentdocumentlinks(self, content_document_ids):
        soql = "SELECT %s  FROM ContentDocumentLink " \
               "WHERE ContentDocumentId IN (%s)" % (self.get_all_fields_string('ContentDocumentLink'),
                                                    ', '.join("'{0}'".format(w) for w in content_document_ids))
        res = self.conn.query(soql)
        return res["records"]


    @staticmethod
    def create_content(content, body, external_id):
        byte_string = str(base64.b64encode(body))
        # Trim the leading b' and trailing apostrophe.
        byte_string = byte_string[2:-1]
        cv = {
            "title": content["Title"],
            'PathOnClient': content["PathOnClient"],
            "Description": content["Description"],
            "VersionData": byte_string,
            "ContentUrl": content["ContentUrl"],
            "OwnerId": content["OwnerId"],
            "CreatedById": content["OwnerId"],
            "CreatedDate": content["CreatedDate"],
            "FirstPublishLocationId": content["FirstPublishLocationId"],
            external_id: content["Id"],
            "TagCsv": content["TagCsv"],
        }
        return cv

    @staticmethod
    def create_attachment(attachment, body):
        byte_string = str(base64.b64encode(body))
        # Trim the leading b' and trailing apostrophe.
        byte_string = byte_string[2:-1]
        cv = {
            "ParentId": attachment["ParentId"],
            'Body': byte_string,
            "ContentType": attachment["ContentType"],
            "Description": attachment["Id"],
            "Name": attachment["Name"],
            "OwnerId": attachment["OwnerId"],
            "IsPrivate": True if attachment["IsPrivate"] == '1' else False
        }
        return cv

    def upload_contentversions(self, attachments, use_bulk=True):
        try:
            self.check_connection()
            if use_bulk:
                ress = self.conn.bulk.ContentVersion.insert(attachments)
            else:
                ress = self.conn.ContentVersion.create(attachments[0])
                ress = [ress]
            success = True
            for res in ress:
                if not res["success"]:
                    success = False
                    self.logger.error('Error uploading ContentVersion into Salesforce: %s', json.dumps(res["errors"]))
            if success:
                self.logger.info(
                    "Uploaded a batch of ContentVersions, please check the Bulk Data Load job status in Salesforce for results.")
            return list(itertools.zip_longest(attachments, ress, fillvalue=''))
        except Exception as e:
            print(attachments[0]['APS_External_Id__c'])
            self.logger.error('Error uploading ContentVersions into Salesforce: %s', e)
            return None

    def upload_attachments(self, attachments, use_bulk=True):
        try:
            self.check_connection()
            ress = self.conn.bulk.Attachment.insert(attachments)
            success = True
            for res in ress:
                if not res["success"]:
                    success = False
                    self.logger.error('Error uploading Attachment into Salesforce: %s', json.dumps(res["errors"]))
            if success:
                self.logger.info(
                    "Uploaded a batch of Attachments, please check the Bulk Data Load job status in Salesforce for results.")
            return itertools.zip_longest(attachments, ress, fillvalue='')
        except Exception as e:
            self.logger.error('Error uploading Attachments into Salesforce: %s', e)
            return None
