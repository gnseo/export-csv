import sys
sys.path.insert(0, './pip')

import requests
import json
from urllib.parse import quote
import gzip
import xml.etree.ElementTree as etree
import boto3
import csv

ACCESS_CONTROL_ALLOW_ORIGIN = 'Access-Control-Allow-Origin'
ALL = '*'
s3 = boto3.resource("s3")

def diveIn(xml_tree,level=1):

  for child in xml_tree:
    if level > 1:
      return diveIn(child,level-1)
    break
  else:
    return (False,xml_tree)

  if level == 1:
    return (True,xml_tree)

def getQuery(event):
  if event["httpMethod"] == "GET":
    return event["queryStringParameters"]
  else:
    return json.loads(event["body"])

def concat_params(params,return_string=True):
  list_params = []
  for key,value in params.items():
    if isinstance(value,list):
      for i in value:
        list_params.append("{0}={1}".format(key,quote(i)))
    elif isinstance(value,dict):
      list_params.extend(concat_params(value,False))
    else:
      list_params.append("{0}={1}".format(key,quote(value)))

  if return_string == False:
    return list_params
  else:
    return "&".join(list_params)

def handler(event, context):
  print(event)
  q = getQuery(event)

  return_headers = { ACCESS_CONTROL_ALLOW_ORIGIN: ALL, "Access-Control-Expose-Headers": "" }

  files_suffix = q.get("files_suffix", "")
  file_name = q.get("file_name", None)
  fieldnames = q.get("fieldnames", None)
  arrdata = q.get("arrdata", None)
  if arrdata:
    if file_name is None:
      return {
        'statusCode': 500,
        'headers': return_headers,
        'body': json.dumps({"errorMessage": "file_name is required"}),
      }
    if fieldnames is None:
      return {
        'statusCode': 500,
        'headers': return_headers,
        'body': json.dumps({"errorMessage": "fieldnames is required"}),
      }
    result = {"url": upload_to_s3("{0}_{1}".format(file_name,files_suffix), fieldnames, arrdata, "jenax/pcr/files/")}

    return {
      'statusCode': 200,
      'headers': return_headers,
      'body': json.dumps(result),
    }

  url = q["api_url"]
  params = q["params"]

  url = "{0}?{1}".format(url,concat_params(params))

  #"http://{ip}/PKOET00000_Default/Services/MDM/POP_TO_ERP_REMPUSFL.asmx/cBLkUpItem_B_LOOK_GOOD_ISSUED_LIST?pvStrGlobalCollection=REMPUSPOP&pvStrGlobalCollection=39.117.251.230&pvStrGlobalCollection=2&pvStrGlobalCollection=39.117.251.230&pvStrGlobalCollection=PKOET00000&pvStrGlobalCollection=KRW&pvStrGlobalCollection=REMPUSPOP&pvStrGlobalCollection=39.117.251.230&pvStrGlobalCollection=a&pvStrGlobalCollection=b&pvStrGlobalCollection=KO&pvStrGlobalCollection=324000000000&pvStrGlobalCollection=d&pvStrGlobalCollection=e&pvStrGlobalCollection=yyyy-MM-dd&pvStrGlobalCollection=-&pvStrGlobalCollection=1900-01-01&pvStrGlobalCollection=f&pvStrGlobalCollection=unierp&pvStrGlobalCollection=g&pvStrGlobalCollection=h&pvStrGlobalCollection=POP_TO_ERP_REMPUS&pvStrGlobalCollection=1&pvStrGlobalCollection=3600&pvStrGlobalCollection=U&pvStrGlobalCollection=k&pvStrGlobalCollection=l&pvStrGlobalCollection=DD&pvStrGlobalCollection=V27AdminDB&pvStrGlobalCollection=20090207&pvStrGlobalCollection=POP_TO_ERP_REMPUS&pvStrGlobalCollection=PKOET00000_Default&pvStrGlobalCollection=F&pvStrGlobalCollection=3600&pvStrGlobalCollection=ko-KR&pvStrGlobalCollection=en-US&pvStrGlobalCollection=PKOET00000&I0_FromDate=2018-09-10&I1_ToDate=2018-09-12&_=1536736658814"

  res = requests.get(url)

  print(res.headers)
  root = etree.fromstring(res.text)

  dic_data = {}
  filenames = {}

  exist,result = diveIn(root,3)
  if exist:
    exist, c1 = diveIn(root,1)
    for c2 in c1:#has 2 children
      #tag1: {http://www.unierp.com/}BlkGoodIssuedProd
      #tag2: {http://www.unierp.com/}BlkGoodIssuedProdInputMaterial

      arr_data = []
      filename = c2.tag.replace("{http://www.unierp.com/}","")
      fieldnames = []
      fieldname = ""
      donotappendfieldname = False
      for c3 in c2:
        dic_row = {}
        for c4 in c3:
          fieldname = c4.tag.replace("{http://www.unierp.com/}","")
          if donotappendfieldname == False:
            fieldnames.append(fieldname)
          dic_row[fieldname] = c4.text
        donotappendfieldname = True
        arr_data.append(dic_row)

      #dic_data[filename] = arr_data
      filenames[filename] = {
        #"data": arr_data,
        "url": upload_to_s3("{0}_{1}".format(filename,files_suffix), fieldnames, arr_data, "rempus/tgid/files/")
      }
  #print(dic_data)

  #return_headers.update(res.headers)

  return {
    'statusCode': 200,
    'headers': return_headers,
    'body': json.dumps(filenames),
  }

def upload_to_s3(filename,fieldnames,data,key_prefix=""):

  with open('/tmp/{}.csv'.format(filename), 'w', newline='', encoding='utf-8') as csvfile:
    csvfile.write(u'\ufeff')
    #fieldnames = ['first_name', 'last_name']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')

    writer.writeheader()
    #writer.writerow({'first_name': 'Baked', 'last_name': 'Beans'})
    for row in data:
      writer.writerow(row)

  s3_bucket_name = "bsg-static-files"
  temp_filename = "{}.csv".format(filename)
  key_name = "{}{}".format(key_prefix,temp_filename)

  #with open('/tmp/{}'.format(temp_filename), 'rb') as csvfile:
  #  res = s3.Bucket(s3_bucket_name).put_object(ACL="public-read", Key="{}".format(key_name), Body=csvfile)
  s3.meta.client.upload_file('/tmp/{}'.format(temp_filename), s3_bucket_name, key_name)

  bucket_location = boto3.client('s3').get_bucket_location(Bucket=s3_bucket_name)
  #object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(
  #  bucket_location['LocationConstraint'],
  #  s3_bucket_name,
  #  key_name)
  object_url = "https://static.kieat.icu/{0}".format(key_name)

  return object_url
