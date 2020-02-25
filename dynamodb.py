import boto3
import csv 
import json

#################################### Cleaning CSV ##############################################
fields = [] 
rows = [] 
header = []
fname = 'EDH_REQUEST_MANAGER_ACTION_LOOKUP_MASTER_TABLE.csv'
filename = open(fname, 'r')
data = csv.reader(filename)
for row in data:
    rows.append(row)

for row in rows[0]:
    header.append([row.split(" ")[0],row.split(" ")[1]])

row_data = (rows[1:])

#################################### Create Table ##############################################
sort_con = input("Is there any Sort Key (Y/N): ")
client = boto3.client('dynamodb')
tname = fname.split('.')
table_name = tname[0]
head_type = (header[0][1].replace('(','').replace(')',''))
sort_type = (header[1][1].replace('(','').replace(')',''))

try:
    if sort_con == 'Y': 
        primary_key = header[0][0]
        sort_key = header[1][0]
        client.create_table(
            AttributeDefinitions=[{
                'AttributeName': primary_key, 
                'AttributeType': head_type
            },
            {
                'AttributeName': sort_key, 
                'AttributeType': sort_type
            }
            ], 
            TableName=table_name, 
            KeySchema=[{
                'AttributeName': primary_key, 
                'KeyType': 'HASH'
            },
            {
                'AttributeName': sort_key, 
                'KeyType': 'RANGE'
            }], 
            ProvisionedThroughput={
                'ReadCapacityUnits': 1, 
                'WriteCapacityUnits': 10
            }
        )
        print('Table Created with Sort Key')
    else:
        primary_key = header[0][0]
        client.create_table(
            AttributeDefinitions=[{
                'AttributeName': primary_key, 
                'AttributeType': head_type
            }
            ], 
            TableName=table_name, 
            KeySchema=[{
                'AttributeName': primary_key, 
                'KeyType': 'HASH'
            }], 
            ProvisionedThroughput={
                'ReadCapacityUnits': 1, 
                'WriteCapacityUnits': 10
            }
        )
        print('Table Created')
except:
    print('Table already exist')


#################################### Insert item in Table #####################################
res = boto3.client('dynamodb')
for rd in range(len(row_data)):
    item_dict = {}

    for head in range(len(header)):
        head_type = (header[head][1]).replace('(','').replace(')','')
        
        if row_data[rd][head] == '':
            pass
        else:
            
            if head_type == 'M':
                _t = (json.loads(row_data[rd][head]))
                item_dict[header[head][0]]={head_type:_t}
            elif head_type == 'L':
                t = (row_data[rd][head]).replace(" ","").replace("[","").replace("]","")
                tt = (t.split(","))
                d = []
                for i in tt:
                    d.append(json.loads(i))
                item_dict[header[head][0]]={head_type:d}
            else:
                item_dict[header[head][0]]={head_type:row_data[rd][head]}
            
    res.put_item(
    TableName=table_name,
    Item=item_dict
    ) 
    print('Item Inserted')