import re

class Utils():
    negacao_aspas = '''(?=(?:[^"]*"[^"]*")*[^"]*$)'''
    negacao_colchetes = '''(?=(?:[^\[]*\[[^\]]*\])*[^\]]*$)''' 
    regexp = '''\s{}{}'''.format(negacao_aspas,negacao_colchetes)

    WHITESPACE_RE = re.compile(regexp)