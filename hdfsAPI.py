#!/usr/bin/env python
#coding=utf-8
from hdfs import *
import posixpath as psp

def list_files(path):
	client=Client("http://namenode:9870")
	fpaths = [
		psp.join(dpath,fname)
		for dpath,_,fnames in client.walk(path)
		for fname in fnames
	]
	return fpaths
def download_file(filepath,savepath):
	client=Client("http://namenode:9870")
	client.download(filepath,savepath,overwrite=True)


