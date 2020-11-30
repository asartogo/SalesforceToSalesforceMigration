

def transform_data(records, sfdc_object, namespaces, sfdc):

    if is_managed_object(sfdc_object, namespaces):
        records = convert_managed_to_unmanaged_field_names(records, sfdc_object, sfdc, namespaces)
        return records
    # if sfdc_object == 'Account':
    #    return records
    # TODO: non-managed objects


def convert_managed_to_unmanaged_field_names(records, sfdc_object, sfdc, namespaces):
    new_records = []
    describe = sfdc.conn.__getattr__(sfdc_object).describe()
    createable_fields = {}
    for field in describe['fields']:
        if field['createable']:
            createable_fields[field['name']] = {'type': field['type'], 'referenceTo': field['referenceTo']}
            # print(field['type'])
    # recordtypes
    recordtypes = sfdc.get_recordtypes(sfdc_object)
    recordtypesMap = {}
    if recordtypes:
        # find the mathing recordtypes on the new object...
        new_recordtypes  = sfdc.get_recordtypes(transform_object(sfdc_object, namespaces))
        for recordtype in recordtypes:
            for new_recordtype in new_recordtypes:
                if recordtype['DeveloperName'] == new_recordtype['DeveloperName']:
                    recordtypesMap[recordtype['Id']] = new_recordtype['Id']

    # inactive users
    inactive_users = []
    for user in sfdc.get_inactive_users():
        inactive_users.append(user['Id'])

    for record in records:
        new_record = {"Mig_Original_Id__c": record['Id']}
        for field in record.keys():
            if field not in createable_fields.keys():
                continue
            new_field_name = field
            for namespace in namespaces:
                if namespace in field:
                    new_field_name = field.replace(namespace, "")
            new_record[new_field_name] = convert_field_type(record[field], createable_fields[field]['type'])

            if createable_fields[field]['type'] == 'reference' and is_managed_object(createable_fields[field]['referenceTo'], namespaces):
                if new_record[new_field_name] is None or new_record[new_field_name] == '':
                    new_record.pop(new_field_name)
                else:
                    # do an upsert with external ids on lookups to manage objects
                    lookup_field_name = new_field_name.replace('__c', '__r')
                    new_record[lookup_field_name] = {"Mig_Original_Id__c": new_record[new_field_name]}
                    new_record.pop(new_field_name)
                    new_field_name = lookup_field_name

            # mismatched fields
            if new_field_name in new_record.keys():
                if sfdc_object == 'P2Express__Online_Application_Type__c':
                    if new_field_name == "DefaultMerchantProduct__r":
                        new_record["P2mig_DefaultMerchantProduct__r"] = new_record[new_field_name]
                        new_record.pop(new_field_name)
                    elif new_field_name == "DefaultStage__r":
                        new_record["P2mig_DefaultStage__r"] = new_record[new_field_name]
                        new_record.pop(new_field_name)
                if sfdc_object == 'P2Express__POS_Solution__c':
                    if new_field_name == "Deal__r":
                        new_record["P2mig_Deal__r"] = new_record[new_field_name]
                        new_record.pop(new_field_name)
                if sfdc_object == 'p2verify__Verification__c':
                    if new_field_name == "Deal__r":
                        new_record["P2mig_Deal__r"] = new_record[new_field_name]
                        new_record.pop(new_field_name)
                    elif new_field_name == "TIN__c":
                        # this data is corrupted
                        new_record.pop(new_field_name)



        if 'RecordTypeId' in new_record.keys():
            if new_record['RecordTypeId'] is not None and new_record['RecordTypeId'] != '':
                new_record['RecordTypeId'] = recordtypesMap[record['RecordTypeId']]
            else:
                new_record.pop('RecordTypeId')

        if 'OwnerId' in new_record.keys():
            if new_record['OwnerId'] in inactive_users:
                # change to integration user
                new_record['OwnerId'] = '00561000002tNs0'
                # new_record.pop('OwnerId')
        new_records.append(new_record)
    #if 'Merchant_Attachment__c' in sfdc_object:
    #    print(new_records)
    #    exit()
    return new_records


def is_managed_object(sfdc_object, namespaces):
    if isinstance(sfdc_object, list):
        sfdc_object = sfdc_object[0]
    is_managed = False
    for namespace in namespaces:
        if namespace in sfdc_object:
            is_managed = True
    return is_managed


def convert_field_type(val, field_type):
    #percent
    #boolean
    #double
    #currency
    if val is None:
        return None
    if field_type == 'boolean':
        return val == '1'
    if field_type == 'percent' or field_type == 'double' or field_type == 'currency':
        return float(val)
    return val
