from pymongo import MongoClient

import logging
import requests
import os
from pathlib import Path

from util import format_hgvs
from util import path_leaf


def reader(directory, archive):
    vconnector = VariantConnector({
        "host": "mongodb://localhost:27017",
        "database": "lazarus",
        "vcollection": "variants",
        "ncollection": "notifications"
    })
    vconnector.connect()

    pathlist = Path(directory).glob("**/*.vcf")
    for path in pathlist:
        logging.info("Parsing file: " + str(path))

        variant = VariantParser(str(path), vconnector)
        try:
            variant.parse()
            variant.process()
        except Exception as e:
            logging.error("Exception raised while parsing: " + str(path))
            logging.exception(e)
            continue
        else:
            filename = path_leaf(path)
            try:
                os.replace(path, archive + filename)
            except OSError as ose:
                logging.error("Exception raised while archiving: " + str(path))
                logging.exception(ose)


class VariantDatabaseException(Exception):
    pass


class VariantConnector(object):

    def __init__(self, cparams):
        self._cparams = cparams
        self._client = None
        self._db = None

    def is_connected(self):
        return self._client is not None

    def connect(self):
        try:
            self._client = MongoClient(self._cparams["host"])
            self._db = self._client[self._cparams["database"]]

            if not self._cparams["vcollection"] in self._db.collection_names():
                self._db.create_collection(self._cparams["vcollection"])

            if not self._cparams["ncollection"] in self._db.collection_names():
                self._db.create_collection(self._cparams["ncollection"])

        except Exception as e:
            logging.error("Could not connect to the database: " + str(e))

    def find(self, qstring, where="vcollection"):
        if not self.is_connected():
            raise VariantDatabaseException("Not connected")

        document = self._db[self._cparams[where]].find_one({"qstring": qstring})
        if document is None:
            return None

        if where == "vcollection":
            return VariantModel(params=None, connector=self, db_variant=document)
        elif where == "ncollection":
            return NotificationModel(params=None, connector=self, db_doc=document)
        else:
            raise NotImplementedError("No other type fetching is implemented in the connector")

    def write(self, document, where="vcollection", update=False):
        if not self.is_connected():
            raise VariantDatabaseException("Not connected")

        check = self.find(document["qstring"])

        if check is None:
            retID = self._db[self._cparams[where]].insert_one(document).inserted_id
        elif update:
            # TODO Check if we need updating
            return -2
        else:
            return -1

        return retID


class NotificationModel(object):

    def __init__(self, params, connector, db_doc):
        pass
        # TODO: Implement this


class VariantModel(object):

    _variant_info_url = "http://myvariant.info/v1/variant/"

    def __init__(self, params=None, connector=None, db_variant=None):

        self._qstring = ""

        if params is not None:
            self.chrom = params["chrom"]
            self.cpos = params["cpos"]
            self.ref = params["ref"]
            self.alt = params["alt"]
            self.rsid = params["rsid"]

        self._connector = connector

        if db_variant is not None:
            self._myvariant = db_variant
        else:
            self._myvariant = {}

    def get_variant_query_string(self):
        if self._qstring == "":
            self._qstring = format_hgvs(self.chrom, self.cpos, self.ref, self.alt)

        return self._qstring

    def find_in_db(self, qstring):
        return self._connector.find(qstring)

    def write_to_db(self):
       return self._connector.write(self._myvariant)

    @property
    def myvariant(self):
        return self._myvariant

    def query_my_variant(self):
        r = requests.get(self._variant_info_url + self.get_variant_query_string())

        if r.status_code != 200:
            logging.warning("Could retrieve myvariant.info data. Status code " + str(r.status_code) +
                            " Variant query: " + self.get_variant_query_string())

            return False

        json_data = r.json()

        if "success" in json_data and not json_data["success"]:
            logging.warning("myvariant.info data did not provide information on: " + self.get_variant_query_string())
            return False

        if "dbsnp" in json_data:
            self._myvariant["dbsnp"] = json_data["dbsnp"]
        else:
            logging.warning("myvariant.info does not contain any `dbsnp` data on " + self.get_variant_query_string())

        if "clinvar" in json_data:
            self._myvariant["clinvar"] = json_data["clinvar"]
        else:
            logging.warning("myvariant.info does not contain any `clinvar` data on " + self.get_variant_query_string())

        self._myvariant["qstring"] = self.get_variant_query_string()
        self._myvariant["vcf_file_params"] = {
            "chrom": self.chrom,
            "cpos": self.cpos,
            "ref": self.ref,
            "alt": self.alt,
            "rsid": self.rsid
        }

        return True


class VariantParser(object):

    def __init__(self, file, connector):
        self._file = file
        self._connector = connector
        self._presults = []

    def parse(self):
        with open(self._file, "r") as f:
            for line in f:

                aline = line.strip()
                if aline[:1] == "#":
                    # Line starts with a # - a comment
                    continue

                raw_params = aline.split("\t")
                if len(raw_params) == 0:
                    # Found an empty line
                    continue

                params = {"chrom": raw_params[0],
                          "cpos": raw_params[1],
                          "ref": raw_params[3],
                          "alt": raw_params[4],
                          "rsid": raw_params[2]
                          }

                variant = VariantModel(params, self._connector)
                self._presults.append(variant)

    def process(self):
        logging.info("Processing variant data")
        if len(self._presults) == 0:
            logging.warning("Cannot process 0 results, skipping")
            return False

        for vmodel in self._presults:
            if vmodel.query_my_variant():
                vmodel.write_to_db()

        logging.info("Variant data processing complete")



