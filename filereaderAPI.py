#!/usr/bin/env python
#coding=utf-8
import pdfplumber
import docx
def readpdf(path):
	pdf = pdfplumber.open(path)
	text=""
	for p in pdf.pages:
		text=text+p.extract_text()
	return text
def readtxt(path):
	f=open(path,"r",encoding='utf8')
	text=f.read()
	f.close()
	return text
def readdocx(path):
	f=docx.Document(path)
	text=""
	for para in f.paragraphs:
		text=text+para.text
	return text

