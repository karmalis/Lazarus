
from vcfstruct import VCFStruct

class VCFReader:

	result = []

	def __init__(self, filepath):
		print("Initialising")
		self.filepath = filepath

	def parse(self):
		with open(self.filepath, "r") as f:
			for line in f:
				
				aline = line.strip()				
				if aline[:1] == "#": # Line is a comment
					continue

				raw_params = aline.split("\t")
				if len(raw_params) == 0: 
					continue

				params = { 	"chrom"	: raw_params[0], 
							"cpos"	: raw_params[1],
							"ref" 	: raw_params[3],
							"alt"	: raw_params[4],
							"rsid"	: raw_params[2]}

				vcfobj = VCFStruct(params)
				self.result.append(vcfobj)

	def getResult(self):
		return self.result


		


