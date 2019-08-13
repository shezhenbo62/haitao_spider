# coding:utf-8
from spider_test import test_123
import re
from fnmatch import fnmatch


with open('./test_123.py', 'r') as f:
    a = f.read()
    print(a)
    b = re.findall(r'ABC_\d+\s=\s[\'\"a-zA-Z0-9]+', a)
    c = re.findall(r'ABC_\d+', a)
    print(b)
    print(c)
print([i for i in test_123.__dir__() if fnmatch(i, 'ABC_[0-9]*')])
print([i for i in test_123.__dict__.keys() if i.startswith('ABC')])
print([test_123.__dict__.get(i) for i in test_123.__dict__.keys() if i.startswith('ABC')])