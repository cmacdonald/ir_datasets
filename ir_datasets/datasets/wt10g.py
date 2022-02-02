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


from .gov import GovDoc, GovDocs

class WTDocs(GovDocs):
    
    def docs_namespace(self):
        return NAME

    def _extract_next_block(self, inp, START, END):
        # if START and END appear in inp, then return (everything after END in inp, the content between START and END),
        # or if they don't appear, return (inp, None).
        i_start = inp.find(START)
        i_end = inp[i_start:].find(END)
        #print(START, END, i_start, i_end)
        if i_start == -1 or i_end == -1:
            return inp.strip(), None
        i_end += i_start
        return inp[i_end+len(END): ], inp[i_start+len(START):i_end]

    def _docs_iter(self):
        dirs = sorted(Path(self.docs_dlc.path()).glob('WTX???'))
        for source_dir in dirs:
            for source_file in sorted(source_dir.glob('*.gz')):
                yield from self._docs_ctxt_iter_gov(source_file)

    def _process_gov_doc(self, raw_doc):
        # read the file by exploiting the sequence of blocks in the document -- this ends
        # up being several times faster than reading line-by-line
        #print(raw_doc)
        raw_doc, doc_id = self._extract_next_block(raw_doc, b'<DOCNO>', b'</DOCNO>')
        assert doc_id is not None
        doc_id = doc_id.strip().decode()
        doc_body, doc_hdr = self._extract_next_block(raw_doc, b'<DOCHDR>', b'</DOCHDR>')
        assert doc_hdr is not None
        for encoding in ['utf8', 'ascii', 'latin1']:
            try:
                doc_url, doc_hdr = doc_hdr.decode(encoding).split('\n', 1)
                break
            except UnicodeDecodeError:
                continue
        content_type_match = re.search('^content-type:(.*)$', doc_hdr, re.I|re.M)
        content_type = 'text/html' # default to text/html
        if content_type_match:
            content_type = content_type_match.group(1)
            if ';' in content_type:
                content_type, _ = content_type.split(';', 1)
        content_type = content_type.strip()
        return GovDoc(doc_id, doc_url, doc_hdr, bytes(doc_body), content_type)


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    collection = WTDocs(dlc['docs'])
    base = Dataset(collection, documentation('_'))

    subsets['trec-web-2000'] = Dataset(
        collection,
        TrecQueries(GzipExtract(dlc['trec-web-2000/queries']), namespace='wt10g/trec-web-2000', lang='en'),
        TrecQrels(GzipExtract(dlc['trec-web-2000/qrels']), QREL_DEFS),
        documentation('trec-web-2000')
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
