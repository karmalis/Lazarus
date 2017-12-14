
from pymongo import MongoClient
from vcfstruct import VariantStruct

import time
import calendar
from datetime import datetime

from pprint import pprint

class VariantConnector:

	def __init__(self, cparams):
		self.cparams = cparams

	def isConnected(self):
		return self.client != None

	def connect(self):
		try:
			self.client = MongoClient(self.cparams["host"])
			self.db = self.client[self.cparams["database"]]

			if not self.cparams["vcollection"] in self.db.collection_names():
				self.db.create_collection(self.cparams["vcollection"])

			if not self.cparams["ncollection"] in self.db.collection_names():
				self.db.create_collection(self.cparams["ncollection"])

		except Exception as e:
			print("Could not connect to database".format(e))

		return self.isConnected()

	def fetch(self, querystring):
		if not self.isConnected():
			raise Exception("Not connected")

		document = self.db[self.cparams["vcollection"]].find_one({ "querystring" : querystring })
		
		params = {
			"vcfdata" : None,
			"dbsnp" : document["dbsnp"],
			"clinvar" : document["clinvar"]
		}

		vstruct = VariantStruct(params)
		#TODO: Convert to a proper obj
		return vstruct

	def add(self, vobj):
		if not self.isConnected():
			raise Exception("Not connected")

		dID = 0
		document = {
			"querystring": vobj.vcfdata.getQueryParam(),
			"dbsnp": vobj.dbsnp,
			"clinvar": vobj.clinvar
		}
		
		try:
			dID = self.db[self.cparams["vcollection"]].insert_one(document).inserted_id
		except Exception as e:
			print("Could not add document: ", e)

		return dID

	def process(self, vobj):
		doc = self.fetch(vobj.vcfdata.getQueryParam())
		if doc == None:
			return self.add(vobj)
		else:
			result = self.compareclinvar(doc, vobj)

			if result['changed']:
				to_insert = {
					"querystring": vobj.vcfdata.getQueryParam(),
					"change": result
				}
				test = self.db[self.cparams["ncollection"]].insert_one(to_insert)
				print(test)			


	def compareclinvar(self, cvarD, cvarF):
		#print(cvarA['clinvar']['rcv'][0])
		#print(cvarB['clinvar']['rcv'][0])

		if not 'rcv' in cvarD.clinvar or not 'rcv' in cvarF.clinvar:
			return {
				'changed' : False,
				'message' : 'Either local or file does not contain a rcv parameter'
			}

		timestampD = calendar.timegm(datetime.strptime(
			cvarD.clinvar['rcv'][0]['last_evaluated'],
			"%Y-%m-%d"
			).timetuple())

		timestampF = calendar.timegm(datetime.strptime(
			cvarF.clinvar['rcv'][0]['last_evaluated'],
			"%Y-%m-%d"
			).timetuple())

		timestampD = int(timestampD)
		timestampF = int(timestampF)

		if timestampF == timestampD:
			return {
				'changed' : False
			}

		if timestampD > timestampF:
			return {
				'changed' : True,
				'recent' : 'local'
			}

		return {
			'changed' : True,
			'recent' : 'file',
			"change" : {
				"from" : cvarD.clinvar['rcv'][0],
				"to" : cvarF.clinvar['rcv'][0] 
			},
		}


# class VariantModel:

# 	vobj = ""
# 	cparams = {}

# 	client = None
# 	db = None


# 	def __init__(self, cparams, vobj):
# 		self.vobj = vobj
# 		self.cparams = cparams

# 	def connect(self):
# 		try:
# 			self.client = MongoClient(self.cparams["host"])
# 		except Exception as e:
# 			print("Could not connect to database: ", e)
# 			return False

# 		self.db = self.client[self.cparams["database"]]

# 		if not self.db:
# 			raise Exception("Could not select database", self.cparams["database"])
# 			return False

# 		return True

# 	def addToDB(self):
# 		dID = 0
# 		document = {
# 			"querystring": self.vobj.vcfdata.getQueryParam(),
# 			"dbsnp": self.vobj.dbsnp,
# 			"clinvar": self.vobj.clinvar
# 		}
# 		#print(document)
# 		dID = self.db.variants.insert_one(document).inserted_id

# 		return dID







