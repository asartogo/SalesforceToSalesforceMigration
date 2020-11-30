import multiprocessing
import threading

from progress.bar import Bar
import sfdc
import db
import json
import time
import datetime
import logging
import argparse
from math import ceil
import transformations


def group_records(records, group_count):
    # group records in batches of threadcount so we can run in parallel
    grouped = []
    current_index = 0
    for r in records:
        (quotient, remainder) = divmod(current_index, group_count)
        if remainder == 0:
            grouped.append([])
        grouped[quotient].append(r)
        current_index += 1
    return grouped


def fetch_contentversions(sf, rec):
    body_url = '/services/data/v42.0/sobjects/ContentVersion/%s/VersionData' % rec["Id"]
    print(body_url)
    body = sf.get_filebody(body_url)
    if body is None:
        return None
    return sf.create_content(rec, body, config["externalIds"]["ContentVersion"])


def upload_contentversions(sf, attachments, use_bulk=True):
    res = sf.upload_contentversions(attachments, use_bulk)
    if res is not None:
        db.update_external_ids("ContentVersion", res, config["externalIds"]["ContentVersion"])


def fetch_attachments(sf, rec):
    body_url = '/services/data/v42.0/sobjects/Attachment/%s/Body' % rec["Id"]
    body = sf.get_filebody(body_url)
    return sf.create_attachment(rec, body)


def upload_attachments(sf, attachments):
    res = sf.upload_attachments(attachments)
    if res is not None:
        db.update_external_ids("Attachment", res, config["externalIds"]["Attachment"])


def bar_next(progress_bar, increment):
    for i in range(increment):
        progress_bar.next()


# process command line arguments
app_description = """This program migrates data from one Salesforce source to a Salesforce destination:\n
                     python3 migrate.py --download 
                     or
                     python3 migrate.py --upload 
                     to invoke the respective functions. download first and then when done upload.
                     or
                     python3 migrate.py --compare 
                     to compares the records (entities) in the source and destination orgs and print out the results in
                      the log file.
                     """
parser = argparse.ArgumentParser(description=app_description)
parser.add_argument('--download', action='store_true',
                    help='Download data from Salesforce.com Source entities specified in config.json into local sqlite'
                         ' database')
parser.add_argument('--upload', action='store_true',
                    help='Upload from downloaded data into Salesforce.com Destination org per mapping specified in'
                         ' mappings.json')
parser.add_argument('--compare', action='store_true',
                    help='Compares the records (entities) in the source and destination orgs and prints out the results'
                         ' in the log file')
args = parser.parse_args()

if args.upload is False and args.download is False and args.compare is False:
    print(app_description)
    exit()

with open('config.json') as json_config_file:
    config = json.load(json_config_file)

# print(config)
time_pattern = '%Y-%m-%dT%H:%M:%SZ'
now = datetime.datetime.now()
logFileName = now.strftime(time_pattern) + '.log'
logging.basicConfig(filename=config["logFilePath"] + logFileName, filemode='w',
                    format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO)
logger = logging.getLogger('migration')

db = db.Db('./db/sfdc.db', logger)

sfdc_upload_batch_size = 10000
sfdc_domain = None
if config["salesforceIsSandboxSource"]:
    sfdc_domain = "test"

sfSource = sfdc.SFDCClient(config["salesforceLoginSource"], config["salesforcePasswordSource"],
                           config["salesforceTokenSource"], "test" if config["salesforceIsSandboxSource"] else None,
                           logger)
sfDestination = sfdc.SFDCClient(config["salesforceLoginDestination"], config["salesforcePasswordDestination"],
                                config["salesforceTokenDestination"],
                                "test" if config["salesforceIsSandboxDestination"] else None, logger)
if sfSource.conn is None or sfDestination.conn is None:
    exit()
schema = sfSource.get_schema(config["entities"])
db.create_connection(schema)

if args.compare:
    if config["includeAttachments"]:
        config["entities"].extend(['ContentVersion', 'Attachment'])
    print('Downloading data from source')
    for sfdc_object in config["entities"]:
        total_records = 0
        bar = Bar(sfdc_object, max=1)
        where_clause = None
        if sfdc_object == 'ContentVersion':
            where_clause = " isLatest = true  AND FileExtension != 'snote'"

        if config["queryFilter"] is not None:
            if where_clause is not None:
                where_clause += " AND "
            else:
                where_clause = ""
            where_clause += config["queryFilter"]

        source_records = sfSource.get_records(sfdc_object, config["recordLimit"], where_clause=where_clause,
                                              field_list=['Id'])
        dest_records = sfDestination.get_records(sfdc_object, config["recordLimit"], where_clause=where_clause,
                                                 field_list=['Id'])

        bar.next()
        bar.finish()
        # logger.info('Downloaded %s %s from Source', len(records), sfdc_object)

if args.download:

    if config["clearDatabase"]:
        db.delete_tables()

    logger.info('Creating local database')

    db.create_tables()
    if config["includeAttachments"]:
        config["entities"].extend(['ContentVersion', 'Attachment'])

    logger.info('Downloading Salesforce.com data for %s', config["entities"])

    for sfdc_object in config["entities"]:

        total_records = 0
        bar = Bar(sfdc_object, max=1)
        where_clause = None
        if sfdc_object == 'ContentVersion':
            where_clause = " isLatest = true  AND FileExtension != 'snote'"

        if config["queryFilter"] is not None:
            if where_clause is not None:
                where_clause += " AND "
            else:
                where_clause = ""
            where_clause += config["queryFilter"]

        records = sfSource.get_records(sfdc_object, config["recordLimit"], where_clause=where_clause)
        db.insert_records(sfdc_object, records)

        bar.next()
        bar.finish()
        logger.info('Downloaded %s %s', len(records), sfdc_object)

if args.upload:

    for sfdc_object in config["entities"]:
        # TODO: This needs to be coded, you are seeing old code for desk to sfdc migration!
        batch_size = sfdc_upload_batch_size
        if sfdc_object in config["customBatchSizes"].keys():
            batch_size = config["customBatchSizes"][sfdc_object]

        record_count = db.get_record_count(sfdc_object)
        logger.info('Found %s %s to upload.', str(record_count), sfdc_object)
        # let's upload in batches of 10000
        bar = Bar(sfdc_object, max=record_count)
        total_batches = int(ceil(record_count / batch_size))

        for batch in range(total_batches):
            records = db.get_records(sfdc_object, batch_size, batch_size * batch)
            res = sfDestination.upload_records(sfdc_object, records, config["includeAuditFields"])
            bar_next(bar, batch_size)
            # now update the external id with the Salesforce Id
            if res is not None:
                db.update_external_ids(sfdc_object, res, config["externalIds"][sfdc_object])
        bar.finish()

        print("Finished uploading data, please check the Bulk Data Load job status in Salesforce for results.")

    if config["attachments"] is not None:
        id_map = {}
        bar = Bar("Retrieving Ids", max=len(config["attachments"]))

        for sfdc_object in config["attachments"]:
            external_id_name = config["externalIds"][sfdc_object]
            records = sfDestination.get_records(sfdc_object=sfdc_object,
                                                where_clause=" %s <> NULL " % external_id_name,
                                                field_list=['Id', external_id_name, 'OwnerId'])
            for record in records:
                id_map[record[external_id_name]] = {"Id": record['Id'],
                                                    "Type": sfdc_object,
                                                    "OwnerId": record['OwnerId'] if "OwnerId" in record else None
                                                    }
            bar.next()
        bar.finish()
        total_records = 0
        # first process contentdocument records
        # records = db.get_records('ContentVersion', where_clause=" newId IS NULL AND (FirstPublishLocationId LIKE '001%' OR FirstPublishLocationId LIKE '00Q%' OR FirstPublishLocationId LIKE '003%'  OR FirstPublishLocationId LIKE '02s%' OR FirstPublishLocationId LIKE '006%')")
        records = db.get_records('ContentVersion', where_clause=" newId IS NULL ")
        all_attachments = []
        batch = 0
        bar = Bar("ContentDocuments", max=len(records))
        grouped = group_records(records, config["threads"])
        for group in grouped:
            params = []
            rec_map = {}
            attachments = []
            for rec in group:
                params.append((sfSource, rec))
                rec_map[rec["Id"]] = rec
                attachment = fetch_contentversions(sfSource, rec)
                if attachment is not None:
                    attachments.append(attachment)
                else:
                    logger.error('Body of contentdocument %s is blank', rec["Id"])
            # pool = multiprocessing.Pool(processes=config["threads"])
            # attachments = pool.starmap(fetch_contentversions, params, chunksize=1)
            # pool.close()
            for attachment in attachments:

                if attachment["VersionData"] is None:
                    continue

                if attachment["FirstPublishLocationId"] is None or attachment['FirstPublishLocationId'] not in id_map:
                    if attachment["FirstPublishLocationId"] is None or not attachment['FirstPublishLocationId'].startswith('005'):
                        attachment['FirstPublishLocationId'] = config["defaultDocumentLibrary"]
                    else:
                        attachment['FirstPublishLocationId'] = config["defaultUserId"]

                    logger.error('Could not find a ContentDocument parent Id for %s',
                                 attachment['FirstPublishLocationId'])
                else:
                    attachment['FirstPublishLocationId'] = id_map[attachment['FirstPublishLocationId']]["Id"]
                # move teh below block outside the else on final upload
                if attachment['OwnerId'] not in id_map:
                    logger.info('Could not find a ContentDocument OwnerId for %s', attachment['OwnerId'])
                    attachment['OwnerId'] = config["defaultUserId"]
                else:
                    attachment['OwnerId'] = id_map[attachment['OwnerId']]["Id"]
                attachment['CreatedById'] = attachment['OwnerId']

                # it it a large file? upload separately
                if rec_map[attachment[config["externalIds"]["ContentVersion"]]]['ContentSize'] > '10000000':
                    threading.Timer(1.0, upload_contentversions, [sfDestination, [attachment], False]).start()
                    continue
                else:
                    all_attachments.append(attachment)

                if batch >= config["customBatchSizes"]["Attachment"] - 1:
                    # print(all_attachments)
                    if len(all_attachments) > 0:
                        threading.Timer(1.0, upload_contentversions, [sfDestination, all_attachments]).start()
                    batch = 0
                    all_attachments = []
                else:
                    batch += 1
            bar_next(bar, config["threads"])
        if len(all_attachments) > 0:
            upload_contentversions(sfDestination, all_attachments)
        bar.finish()
        print(
            "Finished uploading ContentVersion, please check the Bulk Data Load job status in Salesforce for results.")

        # first process contentdocument link records
        # download all content document links, this is a complex process as they need to be query by document ids
        # documents = sfSource.get_records('ContentDocument', field_list=['Id'])
        db.db.execute('SELECT DISTINCT ContentDocumentId Id from ContentVersion WHERE newId IS NOT NULL')
        documents = db.db.fetchall()

        batch_size = 150
        total_batches = int(ceil(len(documents) / batch_size))
        # group them by 150 records
        documents_grouped = group_records(documents, total_batches)
        bar = Bar("ContentDocumentLinks Download", max=len(documents))
        for group in documents_grouped:
            links = sfSource.get_contentdocumentlinks([r['Id'] for r in group])
            db.insert_records('ContentDocumentLink', links)
            bar_next(bar, batch_size)
        bar.finish()

        # then map them to the new ids and upload them
        db.db.execute(
            "SELECT LinkedEntityId, CV.ContentDocumentId ContentDocumentId, ShareType, Visibility, CV.newId newId "
            "FROM ContentDocumentLink "
            "INNER JOIN ContentVersion CV ON CV.ContentDocumentId = ContentDocumentLink.ContentDocumentId "
            "WHERE CV.newId IS NOT NULL")
        records = db.db.fetchall()
        # get the  content version records from salesforce so we can derive the new ContentDocumentId
        content_versions = sfDestination.get_records("ContentVersion", field_list=["Id", "ContentDocumentId"],
                                                     where_clause=" isLatest = true  AND FileExtension != 'snote' ")
        content_versions_map = {}
        for cv in content_versions:
            content_versions_map[cv["Id"]] = cv["ContentDocumentId"]

        cls = []
        for record in records:
            if record["newId"] not in content_versions_map:
                logger.error('content_versions_map does not contain %s', record["newId"])
                continue
            # transform the Ids
            # print(record)
            cl = {
                "ShareType": record["ShareType"],
                "Visibility": record["Visibility"],
                "LinkedEntityId": None,
                "ContentDocumentId": content_versions_map[record["newId"]]
            }
            if record["LinkedEntityId"] in id_map:
                cl["LinkedEntityId"] = id_map[record["LinkedEntityId"]]["Id"]
                cls.append(cl)
            else:
                logger.error('Could not find a ContentDocumentLink linked Id for %s', record["LinkedEntityId"])
        ress = sfDestination.upload_records('ContentDocumentLink', cls, False, upsert=False)

        for res in ress:
            if not res[1]['success']:
                if res[1]['errors'] and "already linked" not in res[1]['errors'][0]['message']:
                    logger.error('Error uploading ContentDocumentLink for ContentDocumentId %s and LinkedEntityId %s '
                                 'with error %s ', res[0]['ContentDocumentId'], res[0]['LinkedEntityId'],
                                 res[1]['errors'][0]['message'])

            # print(res[0], res[1])

        print("Finished uploading data, please check the Bulk Data Load job status in Salesforce for results.")

        # then process attachment records
        records = db.get_records('Attachment', where_clause=" newId IS NULL ")
        all_attachments = []
        batch = 0
        bar = Bar("Attachments", max=len(records))
        grouped = group_records(records, config["threads"])
        for group in grouped:
            params = []
            for rec in group:
                params.append((sfSource, rec))
            pool = multiprocessing.Pool(processes=config["threads"])
            attachments = pool.starmap(fetch_attachments, params, chunksize=1)
            pool.close()
            for attachment in attachments:
                if attachment['ParentId'] not in id_map:
                    logger.error('Could not find a Attachment parent Id for %s', attachment['ParentId'])
                else:
                    obj_type = id_map[attachment['ParentId']]["Type"]
                    parent_owner_id = id_map[attachment['ParentId']]["OwnerId"]
                    attachment['ParentId'] = id_map[attachment['ParentId']]["Id"]
                    if obj_type != 'Task' and obj_type != 'Event':
                        if attachment['OwnerId'] not in id_map and parent_owner_id is not None:
                            attachment['OwnerId'] = parent_owner_id
                        else:
                            logger.error('Could not find a Attachment OwnerId for %s', attachment['OwnerId'])
                            attachment['OwnerId'] = config["defaultUserId"]
                    else:
                        # tasks and events are different
                        attachment['OwnerId'] = parent_owner_id

                    all_attachments.append(attachment)

            if batch > 0:  # sfdc_upload_batch_size / 10:  # arbitrary break down by 1000
                # print(all_attachments)
                threading.Timer(1.0, upload_attachments, [sfDestination, all_attachments]).start()
                batch = 0
                all_attachments = []
            else:
                batch += config["threads"]
            bar_next(bar, len(group))
        if len(all_attachments) > 0:
            upload_attachments(sfDestination, all_attachments)
        bar.finish()
        print("Finished uploading attachments, please check the Bulk Data Load job status in Salesforce for results.")

# print some success/error info
warning_count = 0
error_count = 0
with open(config["logFilePath"] + logFileName, 'r') as f:
    for line in f.readlines():
        words = line.split('|')
        for word in words:
            if word == ' ERROR ':
                error_count += 1
            if word == ' WARNING ':
                warning_count += 1
print('Process finished with %s errors and %s warnings' % (error_count, warning_count))
if warning_count > 0 or error_count > 0:
    print("Please check the  log for details: %s" % (config["logFilePath"] + logFileName))
