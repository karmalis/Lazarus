
import requests

class VariantStruct:

	def __init__(self, params):
		self.dbsnp = params["dbsnp"]
		self.clinvar = params["clinvar"]
		self.vcfdata = params["vcfdata"]

class VCFStruct:

	myvariant_url = "http://myvariant.info/v1/variant/"

	def __init__(self, params):
		print(params)
		self.chrom = params["chrom"]
		self.cpos = params["cpos"]
		self.ref  = params["ref"]
		self.alt  = params["alt"]
		self.rsid = params["rsid"]

	def getQueryParam(self):
		return 	"chr" + self.chrom + ":g." + self.cpos + self.ref + ">" + self.alt

	def queryMyVariant(self):
		print(self.myvariant_url + self.getQueryParam())
		r = requests.get(self.myvariant_url + self.getQueryParam())
		if r.status_code != 200:
			print("Something went wrong when querying MyVariant")

		jdata = r.json()

		params = {
			"vcfdata" : self,
			"dbsnp" : jdata["dbsnp"],
			"clinvar" : jdata["clinvar"]
		}

		variantobj = VariantStruct(params)

		return variantobj








