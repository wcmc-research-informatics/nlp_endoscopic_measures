from enum import Enum, auto
import traceback
from db3 import *
from ks3 import *
from common import *

p06 = None
try: p06 = slurpj("enclave/p06.json")
except: pass

# trace flag
TRC = False

DEST_TABLE = '[dm_cadc].[ibd].[tmp_endoscopy_mayo]'

#-----------------------------------------------------------------------------
# tunable parameters

# How many times we'll skip words before we decide we are in the weeds and
# there aren't any additional potential scores/elements to extract.
MAX_SKIPS = 19

# Additional characters where we want text on either side (when no spaces)
# split up as separate tokens (when tokenizing the report).
SPLITTERS = [':', '-', '=', '/']

# transitioners
STOP_WORDS = ['hb', 'harvey']
SF_WORDS = ['sf', 'stool'] 
RB_WORDS = ['rb', 'rectal', 'bleeding'] 
MA_WORDS = ['ma', 'endoscopy', 'endoscopic', 'mucosal']
MD_WORDS = ['md', 'physician']
TOTAL = 'total'

#-----------------------------------------------------------------------------

def is_valid_subscore(score):
    return (
        is_integer(score)
        and int(score) in (0, 1, 2, 3))

def is_valid_total_score(score):
    return (
        is_integer(score)
        and int(score) >= 0
        and int(score) <= 12)

def save_sf_score(cache, score):
    cache['sf'] = int(score)

def save_rb_score(cache, score):
    cache['rb'] = int(score)

def save_ma_score(cache, score):
    cache['ma'] = int(score)

def save_md_score(cache, score):
    cache['md'] = int(score)

def save_total_score(cache, score):
    cache['total'] = int(score)

def inc_skips(cache):
    if 'skips' in cache:
        cache['skips'] += 1
    else:
        cache['skips'] = 1

def reset_skips(cache):
    '''Should reset after every transition.'''
    cache['skips'] = 0

def can_skip_then_inc(cache):
    '''Increments skip count when called!!!'''
    if 'skips' in cache and cache['skips'] >= MAX_SKIPS:
        return False
    else:
        inc_skips(cache) 
        return True

#-----------------------------------------------------------------------------
# fsm

class EndState(Enum):
    TOO_MANY_SKIPS = auto()
    AT_STOP_WORD = auto() 
    AT_STANDALONE_SCORE = auto()

def is_transitioning_token(token):
    '''Note: numbers are always transitioners but we don't worry about
    that here'''
    return (
        token in STOP_WORDS
        or token in SF_WORDS
        or token in RB_WORDS
        or token in MA_WORDS
        or token in MD_WORDS
        or token == TOTAL)

def get_next_state(token):
    '''Only call when token is transitioner.'''
    if not is_transitioning_token(token):
        raise ValueError
    if token in STOP_WORDS:
        return EndState.AT_STOP_WORD
    elif token in SF_WORDS:
        return at_sf_prelude
    elif token in RB_WORDS:
        return at_rb_prelude
    elif token in MA_WORDS:
        return at_ma_prelude
    elif token in MD_WORDS:
        return at_md_prelude
    elif token == TOTAL:
        return at_total_prelude
    else:
        raise Exeption('should never get here')

def at_entry(token, cache):
    '''at_entry is unique b/c we assume a score right here is a pertinent score,
    in contrast to at_ready where we require a keyword to indicate as such 
    (to differentiate from subscores.)
    We never come back to at_entry after leaving.
    Returns tuple (state, cache).'''
    this = at_entry
    if is_valid_subscore(token):
        return (EndState.AT_STANDALONE_SCORE, cache)
    elif is_transitioning_token(token):
        reset_skips(cache)
        return (get_next_state(token), cache)
    else:
        if can_skip_then_inc(cache): return (this, cache)
        else: return (EndState.TOO_MANY_SKIPS, cache)

def at_ready(token, cache):
    '''For difference between at_ready and at_entry, see former's
    docstring.
    Returns tuple (state, cache).
    '''
    this = at_ready
    if is_transitioning_token(token):
        reset_skips(cache)
        return (get_next_state(token), cache)
    else:
        if can_skip_then_inc(cache): return (this, cache)
        else: return (EndState.TOO_MANY_SKIPS, cache)

def at_sf_prelude(token, cache):
    this = at_sf_prelude
    if is_transitioning_token(token):
        reset_skips(cache)
        return (get_next_state(token), cache)
    elif is_valid_subscore(token):
        reset_skips(cache)
        save_sf_score(cache, token)
        return (at_ready, cache)
    else:
        if can_skip_then_inc(cache): return (this, cache)
        else: return (EndState.TOO_MANY_SKIPS, cache)

def at_rb_prelude(token, cache):
    this = at_rb_prelude
    if is_transitioning_token(token):
        reset_skips(cache)
        return (get_next_state(token), cache)
    elif is_valid_subscore(token):
        reset_skips(cache)
        save_rb_score(cache, token)
        return (at_ready, cache)
    else:
        if can_skip_then_inc(cache): return (this, cache)
        else: return (EndState.TOO_MANY_SKIPS, cache)

def at_ma_prelude(token, cache):
    this = at_ma_prelude
    if is_transitioning_token(token):
        reset_skips(cache)
        return (get_next_state(token), cache)
    elif is_valid_subscore(token):
        reset_skips(cache)
        save_ma_score(cache, token)
        return (at_ready, cache)
    else:
        if can_skip_then_inc(cache): return (this, cache)
        else: return (EndState.TOO_MANY_SKIPS, cache)

def at_md_prelude(token, cache):
    this = at_md_prelude
    if is_transitioning_token(token):
        reset_skips(cache)
        return (get_next_state(token), cache)
    elif is_valid_subscore(token):
        reset_skips(cache)
        save_md_score(cache, token)
        return (at_ready, cache)
    else:
        if can_skip_then_inc(cache): return (this, cache)
        else: return (EndState.TOO_MANY_SKIPS, cache)

def at_total_prelude(token, cache):
    this = at_total_prelude
    if is_transitioning_token(token):
        reset_skips(cache)
        return (get_next_state(token), cache)
    elif is_valid_total_score(token):
        reset_skips(cache)
        save_total_score(cache, token)
        return (at_ready, cache)
    else:
        if can_skip_then_inc(cache): return (this, cache)
        else: return (EndState.TOO_MANY_SKIPS, cache)

def fsm(tokens):
    curr_state = at_entry
    cache = {}
    not_found = 'NOT FOUND'
    def f_resolve_score(cache):
        '''Need endoscopic subscore.'''
        if 'ma' in cache:
            return cache['ma']
        else:
            return not_found
    for t in tokens:
        if TRC: print('token: ' + t)
        try:
            curr_state, cache = curr_state(t, cache)
            if curr_state == EndState.TOO_MANY_SKIPS:
                return f_resolve_score(cache)
            elif curr_state == EndState.AT_STOP_WORD:
                return f_resolve_score(cache)
            elif curr_state == EndState.AT_STANDALONE_SCORE:
                return t
        except Exception as ex:
            if TRC: print('fsm got ex: ' + str(ex) + '\n' + traceback.format_exc())
            return 'ERROR'
    # If we get here, ran out of tokens but 'promising' tokens
    # were seen near end of report.
    return f_resolve_score(cache)

#-----------------------------------------------------------------------------
# drivers

MAYO = 'mayo'

def find_score(text):
    tokens = into_word_tokens_with_splitters(text, SPLITTERS)
    starts = indices_for(tokens, MAYO)
    captured_scores = []
    found_any = False
    for i in starts:
        if TRC: print('----------------')
        if TRC: print('start i: ' + str(i))
        toks = tokens[i:]
        result = fsm(toks)
        if is_integer(result):
            found_any = True
            captured_scores.append(result)
    if found_any:
        max_score = max(list(map(lambda x: int(x), captured_scores)))
        return max_score
    else:
        return 'NOT FOUND'

def db_get_pertinent_reports(db_spec=p06):
    qy = ("select empi, [Procedure Date] as proc_date, "
          " [Procedure Code] as proc_code, order_proc_id,"
          " notes as rpt"
          " from dm_cadc.ibd.endoscopy_unfinished"
          " where notes like '%mayo%' ")
    rslt = db_qy(db_spec, qy)
    return rslt

def do_all(db_spec=p06):
    out = []
    dat = db_get_pertinent_reports(db_spec)
    for row in dat:
        score = find_score(row['rpt'])
        if TRC: print('Score was: ' + str(score))
        row['mayo'] = score
        del row['rpt']
        out.append(row)
    t = DEST_TABLE
    db_trunc_table(db_spec, t)
    db_insert_many(db_spec, t, out)
    return dat

