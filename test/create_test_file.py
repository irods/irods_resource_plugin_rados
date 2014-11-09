#!/usr/local/bin/python2.7
# encoding: utf-8
'''
structured_file -- create well defined test files

structured_file is a description

@author:     Matthias Grawinkel

@copyright:  2014 everbit. All rights reserved.

@contact:    m.grawinkel@everb.it
'''

import sys
import os

def main(argv=None):

    target_file = os.path.abspath(sys.argv[1])
    
    if os.path.exists(target_file):
        print("target file exists, aborting.")
        sys.exit(1)
    
    
    with open(target_file, 'wb') as tf:
        for i in range(15):
            buf = [chr(i)] * (1024 * 1024)
                       
            tf.write("".join(buf))
  
if __name__ == "__main__":
    sys.exit(main())
