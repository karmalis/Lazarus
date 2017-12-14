
from pymongo import MongoClient
from datetime import datetime

from vcfreader import VCFReader
from variantmodel import VariantConnector

import requests

vcffile = VCFReader("./RD/150407216.vcf")
vcffile.parse()

vconnector = VariantConnector({
		"host" : "mongodb://localhost:27017",
		"database" : "lazarus",
		"vcollection" : "variants",
		"ncollection" : "notifications"
	})
vconnector.connect()


test = ""
for vcobj in vcffile.getResult():
	vobj = vcobj.queryMyVariant()
	vconnector.process(vobj)
	break

# print(test)

# r = requests.get("http://myvariant.info/v1/variant/"+test)
# print(r.status_code)
# print(r.json())

# client = MongoClient("mongodb://localhost:27017")
# db = client.test

# result = db.restaurants.delete_many({"borough": "Manhattan"})
# print(result.deleted_count)
