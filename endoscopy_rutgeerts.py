import re
import traceback
from db3 import *
from ks3 import *
from common import *

p06 = None
try: p06 = slurpj("enclave/p06.json")
except: pass

TRC = False

SCHEMA = 'ibd'
DEST_TABLE = '[dm_cadc].[ibd].[tmp_endoscopy_rutgeerts]'

#-----------------------------------------------------------------------------
# db

def make_tmp_table(db_spec):
    # drop if exists; ok if doesn't.
    db_drop_table(db_spec, SCHEMA, db_table_from_fqtn(DEST_TABLE)) 
    ddl = slurp('sql/make-tmp-endoscopy-rutgeerts-table.sql')
    db_stmt(db_spec, ddl)

#-----------------------------------------------------------------------------
# regex

reg = r'(rutgeerts|rutgeert|rutgers)\s*(score\s*|was\s*|is\s*|of\s*){0,4}(i?[0-4])'
reg_obj = re.compile(reg, flags=re.IGNORECASE|re.DOTALL)

#-----------------------------------------------------------------------------

def _get_max_score(rut_scores):
    '''Given a list of Rutgeerts scores like '2', 'i2', 'i3', 'I0'
    return the maximum.'''
    stripped = list(map(lambda x: int(x.lower().replace('i','')), rut_scores))
    return rut_scores[stripped.index(max(stripped))]

def find_score(text):
    '''
    NOTE: Currently, this function makes a key assumption, which is:
    if there are multiple scores discovered, returns the _most severe_
    score. This assumption is based on the RTM recommendation to 
    extract from 'anywhere' in the report, which suggests that 
    historical scores are notable, the most severe being the most notable.
    '''
    score = None
    rslt = reg_obj.findall(text)
    if rslt:
        # findall result is a list of tuples.
        # score if present will always be last, based on regex.
        # Throw in a lower() in case the 'i' is capitalized.
        scores = list(map(lambda x: x[2].lower(), rslt))
        score = _get_max_score(scores)
    if TRC: print('find_score: ' + str(rslt))
    # Rutgeerts score always starts with letter i; append if needed.
    if score and score[0] != 'i':
        score = 'i' + score
    return score if score else 'NOT FOUND'

def db_get_pertinent_reports(db_spec=p06):
    qy = ("select empi, [Procedure Date] as proc_date, "
          " [Procedure Code] as proc_code, order_proc_id,"
          " notes as rpt"
          " from dm_cadc.ibd.endoscopy_unfinished"
          " where notes like '%rutgeert%' "
          " or notes like '%rutgers%' ")
    rslt = db_qy(db_spec, qy)
    return rslt

def doall(db_spec=p06):
    out = []
    data = db_get_pertinent_reports(db_spec)
    for row in data:
        score = find_score(row['rpt'])
        if TRC: print('Score was: ' + str(score))
        row['rutgeerts'] = score
        del row['rpt']
        out.append(row)
    make_tmp_table(db_spec)
    db_insert_many(db_spec, DEST_TABLE, out)
    return len(data)

