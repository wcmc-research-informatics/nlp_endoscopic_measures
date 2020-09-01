import collections
import re
import string
import sys
from nltk.tokenize import word_tokenize
from db3 import *
from ks3 import *

p06 = None
try: p06 = slurpj("enclave/p06.json")
except: pass

def funcname():
    return sys._getframe(1).f_code.co_name

def is_integer(x):
    if x and x[len(x)-1] == '.':
        # Removing trailing period; causes int() to return false 
        # for an otherwise good integer.
        x = x[:len(x)-1]
    try:
        x = int(x)
        y = float(x)
        return x == y
    except:
        return False

def to_integer(x):
    if x and x[len(x)-1] == '.':
        # Removing trailing period; causes int() to return false 
        # for an otherwise good integer.
        x = x[:len(x)-1]
    return int(x)

def get_anchor_indices(s, anchors, found=None):
    '''Specifically, 'trailing' anchor indcies.
    For each anchor string, get index immediately after its end;
    return all these indices as a list of ints.
    NOTE: if anchor occurs mult times, will only find the first
    one.'''
    if not found:
        found = []
        # if-clause helps to avoid doing below more than once.
        s = s.lower()
        anchors = list(map(lambda x: x.lower(), anchors))
    if len(s) == 0 or len(anchors) == 0:
        return found
    anchor = anchors.pop(0)
    x = s.find(anchor)
    #print(anchor)
    #print(x)
    if x != -1:
        found.append(x + len(anchor))
    # Recursive call to process remaining anchor strings.
    return get_anchor_indices(s, anchors, found)

def into_word_tokens(s, to_lower=True):
    '''
    Tokenize s to list of words with punctuation removed.
    (And convert to lowercase when is_lower is True.)
    Returns list of strings.
    '''
    if not isinstance(s, str):
        raise TypeError('s needs to be a string')
    tokens = word_tokenize(s)
    if to_lower:
        tokens = map(lambda x: x.lower(), tokens)
    # For any token that ends in a period, remove the period.
    def chomp_period(t):
        if t[len(t)-1] == '.':
            return t[:len(t)-1]
        else:
            return t
    tokens = list(map(chomp_period, tokens))
    # Next, remove words consisting of 1-char punctuation, then return.
    return list(filter(lambda x: x not in string.punctuation,
                       tokens))

def into_word_tokens_with_splitters(s, splitters, to_lower=True):
    '''Calls into_word_tokens (above), and then goes through the 
    results and splits words again whenever they contain a char
    found in the splitters list.
    o splitters arg: should be list/seq of strings;
            any word that contains an element in splitters will
            be split into two separate words, discarding the
            string it was split on. e.g.,
                    splitters = [':', '=']
                then...
                    'DF:0'  -->  'DF', '0'
                    'score=1' --> 'score', '1'
            Note that words might get split multiple times,
            if they contain the item multiple times, or if
            they contain more than one of the elements found
            in splitters, e.g.,
                'foo-bar:1' --> 'foo', 'bar', '1'
    Returns list of strings.
    '''
    if not isinstance(splitters, collections.Sequence):
        raise TypeError
    tokens = into_word_tokens(s, to_lower)
    def replace_insert_mult(list_a, list_a_index, list_b):
        '''Replace item at list_a_index from list_a, with
        one or more items.'''
        list_a[list_a_index:list_a_index + 1] = list_b
    for sp in splitters:
        # Identify replacements first; after all found, we'll walk 
        # backwards from end of tokens to front. (Beginning to end
        # would modify indexing before we have a chance to replace
        # everything.)
        to_replace = []
        for i, t in enumerate(tokens):
            if sp in t:
                to_replace.append( (i, t.split(sp)) )
        for tup in reversed(to_replace):
            replace_insert_mult(tokens, tup[0], tup[1])
    return tokens

def indices_for(tokens, t_of_interest):
    return [i for i, t in enumerate(tokens) if t == t_of_interest] 

