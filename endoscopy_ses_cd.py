import copy
import json
import re
import sys
from db3 import *
from ks3 import *
from common import *

p06 = None
try:
    p06 = slurpj("enclave/p06.json")
except:
    pass

# trace flag
TRC = False

#-----------------------------------------------------------------------------
# db

DEST_TABLE = 'dm_cadc.ibd.tmp_endoscopy_ses_cd'

def make_tmp_table(db_spec):
    schema = 'ibd'
    # drop if exists; ok if doesn't.
    db_drop_table(db_spec, schema, db_table_from_fqtn(DEST_TABLE)) 
    ddl = slurp('sql/make-tmp-endoscopy-ses-cd-table.sql')
    db_stmt(db_spec, ddl)

#------------------------------------------------------------------------------
# anchors
# These are points in report where we want to start looking for a score.

ANCHORS = [r'ses-cd',
           r"simple endoscopic score for crohn's disease"]

#------------------------------------------------------------------------------
# fsm
# implementation of ses-cd final score finite-state machine 

SKIP_WORDS = ['scoring', 'score', 'was']
PERTINENT_PRELUDES = ['total', 'aggregate']
SUBSCORE_PRELUDES = ['ileum', 'right', 'colon', 'transverse', 'left', 'rectum']

def is_skip_word(t):
    return t in SKIP_WORDS

def is_pertinent_prelude(t):
    return t in PERTINENT_PRELUDES

def is_subscore_prelude(t):
    return t in SUBSCORE_PRELUDES

def at_just_entered(token):
    '''
    Note: 'entered' state is different than 'ready' in one key aspect:
    if the next token is a number we consider it pertinent
    and take it. howver in the ready state, a pertinent number
    must be preceded by a pertinent prelude.
    '''
    if TRC: print(funcname())
    if is_integer(token):
        return at_pertinent_score                
    if is_pertinent_prelude(token):
        return at_pertinent_prelude
    if is_subscore_prelude(token):
        return at_subscore_prelude(token)
    else:
        return at_just_entered

def at_ready(token):
    '''
    in the ready state, a pertinent number
    must be preceded by a pertinent prelude
    '''
    if TRC: print(funcname())
    if is_pertinent_prelude(token):
        return at_pertinent_prelude
    if is_subscore_prelude(token):
        return at_subscore_prelude(token)
    else:
        return at_ready

def at_skip_number(token):
    if TRC: print(funcname())
    if is_pertinent_prelude(token):
        return at_pertinent_prelude
    if is_subscore_prelude(token):
        return at_subscore_prelude
    else:
        return at_skip_number

def at_subscore_prelude(token):
    if TRC: print(funcname())
    if is_subscore_prelude(token):
        return at_subscore_prelude
    if is_integer(token): # subscore numeric value; discard.
        return at_ready
    else:
         return at_subscore_prelude

def at_pertinent_prelude(token):
    if TRC: print(funcname())
    if is_skip_word(token):
        return at_pertinent_prelude 
    if is_pertinent_prelude(token):
        return at_pertinent_prelude
    if is_subscore_prelude(token): # not sure if likely, but account for.
        return at_subscore_prelude
    if is_integer(token): # this is value of interest.
        if TRC: print('got it: ' + str(token))
        return at_pertinent_score  
    else:
        return at_pertinent_prelude

def at_pertinent_score(token):
    if TRC: print(funcname())
    raise Exception('End state has no transition.')

def at_unknown(token):
    if TRC: print(funcname())
    raise Exception('End state has no transition.')

def fsm(tokens):
    '''Returns token of interest; or None if can't find'''
    curr_state = at_just_entered
    for token in tokens:
        try:
            if TRC: print('token: ' + str(token))
            curr_state = curr_state(token)
            if curr_state == at_pertinent_score:
                return token
            if curr_state == at_unknown:
                return 'NOT FOUND'
            if TRC: print('afte transition, curr_state: ' + str(curr_state.__name__))
        except Exception as ex:
            if TRC: print('fsm got ex: ' + str(ex))
            return None
    return 'NOT FOUND'
    if TRC: print('err: ' + str(token))
    raise Exception('fsm: should never get here')

#------------------------------------------------------------------------------
# drivers

def find_score(text):
    '''
    1 find anchor (if multiple anchors, how to proceed?)
    2 just after anchor, tokenize the rest of the string.
    3 take the first 25 tokens.
    4 loop through tokens, feeding into "current_state"
        - need to decide if class will be needed for this. 
    5 if not found, return None
        if found, return as string
    '''
    result = None
    anchor_indices = get_anchor_indices(text, ANCHORS)
    if TRC: print('anchors indices: ' + str(anchor_indices) )
        
    for idx in anchor_indices:
        if TRC: print('start')
        tokens = into_word_tokens(text[idx:])  
        tokens = tokens[:30] # limit to 30 tokens.
        if TRC: print('len tokens: ' + str(len(tokens)))
        result = fsm(tokens)
        if is_integer(result):
            break
    return result if result else 'NOT FOUND'

def get_pertinent_sections(db_spec):
    qy = ("select empi, proc_date, proc_code, findings, impression"
          " from dm_cadc.ibd.tmp_endoscopy_sections"
          " where findings like '%ses-cd%'"
          " or findings like '%simple endo%'"
          " or impression like  '%ses-cd%'"
          " or impression like '%simple endo%'")
    rslt = db_qy(db_spec, qy)
    return rslt

def doall(db_spec=p06):    
    out = []
    dat = get_pertinent_sections(db_spec)
    for row in dat:
        score = find_score(row['impression'])
        if score == 'NOT FOUND':
            score = find_score(row['findings'])
        if TRC: print('Score was: ' + str(score))
        row['ses_cd'] = score
        del row['findings']
        del row['impression']
        out.append(row)
    t = DEST_TABLE
    make_tmp_table(db_spec) 
    db_insert_many(db_spec, t, out)
    return dat

