from vcfreader import VCFReader
from variantmodel import VariantConnector

import argparse
from pathlib import Path
from pprint import pprint


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-d', help="Directory of the VCF files")
	args = parser.parse_args()

	if args.d is None:
		parser.print_help()
	else:
		try:
			vconnector = VariantConnector({
					"host" : "mongodb://localhost:27017",
					"database" : "lazarus",
					"vcollection" : "variants",
					"ncollection" : "notifications"
				})
			vconnector.connect()

			pathlist = Path(args.d).glob("**/*.vcf")
			for path in pathlist:
				vcffile = VCFReader(str(path))
				vcffile.parse()

				for vcobj in vcffile.getResult():
					vobj = vcobj.queryMyVariant()
					vconnector.process(vobj)

		except Exception as e:
			print(e)
		


# vcffile = VCFReader("./RD/150407216.vcf")
# vcffile.parse()




# test = ""
# for vcobj in vcffile.getResult():
# 	vobj = vcobj.queryMyVariant()
# 	vconnector.process(vobj)
# 	break





###################

# print(test)

# r = requests.get("http://myvariant.info/v1/variant/"+test)
# print(r.status_code)
# print(r.json())

# client = MongoClient("mongodb://localhost:27017")
# db = client.test

# result = db.restaurants.delete_many({"borough": "Manhattan"})
# print(result.deleted_count)
