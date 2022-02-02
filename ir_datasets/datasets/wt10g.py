import re
import io
import os
import gzip
import codecs
from collections import Counter
from contextlib import contextmanager, ExitStack
from pathlib import Path
from typing import NamedTuple
from glob import glob
import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract, TarExtract
from ir_datasets.formats import TrecQrels, TrecQueries, TrecColonQueries, BaseDocs, GenericQuery, BaseQrels
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import Docstore, PickleLz4FullStore


_logger = ir_datasets.log.easy()


NAME = 'wt10g'


QREL_DEFS = {
    1: 'Relevant',
    0: 'Not Relevant',
}


from .gov import GovDocs

class WTDocs(GovDocs):
    pass


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    collection = GovDocs(dlc['docs'])
    base = Dataset(collection, documentation('_'))

    subsets['trec-web-2000'] = Dataset(
        collection,
        TrecQueries(GzipExtract(dlc['trec-web-2000/queries']), namespace='gov/trec-web-2000', lang='en'),
        TrecQrels(GzipExtract(dlc['trec-web-2000/qrels']), QREL_DEFS),
        documentation('trec-web-2000')
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
