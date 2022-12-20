import contextlib
from pathlib import Path
from typing import NamedTuple
import ir_datasets
from ir_datasets.formats.jsonl import JsonlDocs
from ir_datasets.util import ZipExtractCache, DownloadConfig, RelativePath
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import CsvQueries, CsvDocs, TrecQrels, TrecScoredDocs
from ir_datasets.util.fileio import GzipExtract

NAME = 'miracl'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    1: "Relevant",
    0: "Not Relevant",
}

DUA = ("To use the TODO dataset, you must read and accept the TODO, found here: "
       "<https://TODO>")

class MiraclPassage(NamedTuple):
    doc_id: str
    text: str
    text: str

def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    subsets = {}

    sources = [
        # language, nbr of files, nbr of passages
        ('ar', 5, 2_061_414),
        ('bn', 1, 297_265),
        ('en', 54, 32_893_221),
        ('en', 21, 10_373_953),
        ('fa', 5, 2_207_172),
        ('fi', 4, 1_883_509),
        ('fr', 30, 14_636_953),
        ('hi', 2, 506_264),
        ('id', 3, 1_446_315),
        ('ja', 14, 6_953_614),
        ('ko', 3, 1_486_752),
        ('ru', 20, 9_543_918),
        ('sw', 1, 131_924),
        ('te', 2, 518_079),
        ('th', 2, 542_166),
        ('zh', 10, 4_934_368)
    ]

    for source, nbr_files, nbr_docs in sources:

        #https://huggingface.co/datasets/miracl/miracl-corpus/resolve/main/miracl-corpus-v1.0-ar/docs-0.jsonl.gz
        #docs_files = [GzipExtract(RelativePath(dlc, 'miracl-corpus-v1.0-%s/docs-%d.jsonl.gz' % (source, file_num))) for file_num in range(nbr_files)]
        docs = JsonlDocs([dlc['miracl-%s-%d'% (source, 0)] ], 
            namespace=source, 
            lang=source, 
            doc_cls=MiraclPassage,
            count_hint=nbr_docs)

        # docs = CsvDocs(
        #     RelativePath(source_dlc, f"{zip_dir_name}/documents.csv"), 
        #     namespace=source, lang=source[:2], 
        #     count_hint=ir_datasets.util.count_hint(f'{NAME}/{source}'), 
        #     docstore_path=ir_datasets.util.home_path()/NAME/f'{source}.pklz4')
        subsets[source] = Dataset(docs, documentation(source))
        
    base = Dataset(documentation('_'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


collection, subsets = _init()
