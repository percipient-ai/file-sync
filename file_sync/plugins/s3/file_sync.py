import boto3
from datetime import datetime
from itertools import groupby
import hashlib
import logging
import os
import pytz
from urllib.parse import urlparse

logging.getLogger(__name__).addHandler(logging.NullHandler())
LOG = logging.getLogger("root")


def plugin_instance():
    return S3FileSync()


class S3FileSync:
    def init(self, config):
        self.config = config

    def discover(self, watermark_ts=None, plugin_config=None):
        search_base = plugin_config["search_base"]
        LOG.info(f"Scanning: {search_base}")

        _, bucket, prefix = self.s3open(search_base)
        LOG.debug("Bucket = {}, prefix = {}".format(bucket, prefix))

        # Returns a list of tuples of the form: (<relative path after prefix>, < timestamp>).
        paths = self.find_by_prefix(bucket, prefix+"/")

        # Filter only those paths that have not been seen previously
        watermark_mtime = watermark_ts.timestamp() if watermark_ts is not None else 0
        path_tups = list(filter(lambda ds_tup : ds_tup[1] > watermark_mtime, paths))

        retval = []
        m = hashlib.md5()
        tups_by_dataset = groupby(path_tups, key=lambda tup: tup[0][0 : tup[0].index('/')])

        for dataset, path_tups in tups_by_dataset:
            file_names = []
            for tup in path_tups:
                if tup[0].endswith(".READY_FOR_PROCESSING"):
                    ts = datetime.fromtimestamp(tup[1], pytz.utc)
                else:
                    file_names.append(os.path.basename(tup[0]))

            m.update(dataset.encode('utf'))
            unique_id = m.hexdigest()[:16]

            ds_details = {
                "dataset_name": dataset,
                "file_names": file_names,
            }

            retval.append((ts, unique_id, ds_details))

        LOG.info("Found {} new dataset(s).".format(len(retval)))

        return retval

    # Generates necessary boto3 resources to list objects under a prefix. Returns s3client, bucket_object and prefix_string
    @staticmethod
    def s3open(url):
        pr = urlparse(url)
        prefix = pr.path[1:] if pr.path.startswith("/") else pr.path
        bkt = pr.netloc
        s3r = boto3.resource("s3")
        s3bkt = s3r.Bucket(bkt)
        return s3r, s3bkt, prefix

    # Returns a list of tuples of the form: (<relative path after prefix>, < timestamp>).
    @staticmethod
    def find_by_prefix(bkt, prefix):
        return [
            (obj.key.replace(prefix, ""), obj.last_modified.timestamp())
            for obj in bkt.objects.filter(Prefix=prefix)
        ]
