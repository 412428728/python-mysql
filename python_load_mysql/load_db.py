#!/usr/bin/python
# coding: UTF-8
## Author: xxhh
## Time: 2016-10-15 21:08

import MySQLdb
import re

## 操作输入文件
def begin(file_name):
	execdb_create('course')
	f = open(file_name,'r')
	result = list()
	index = 0
	for line in f.readlines():                          #依次读取每行  
		try:
			if index==0:
				line=line[3:]
				index+=1
			line = line.strip()                             #去掉每行头尾空白  
			if not len(line):       						#判断是否是空行 
				continue                                    #是的话，跳过不处理 
			result.append(line.decode('utf-8').encode('utf-8'))                             #保存
		except:
			print index
	for line in result:
		value_list = re.split('\s+',line)
		print value_list[0]
		if len(value_list)!=4:
			print 'not 4 arguments'
			continue
		a = value_list[0]
		b = value_list[1]
		c = value_list[2]
		#d = value_list[3][0:4]+'-'+value_list[3][4:6]+'-'+value_list[3][6:8]
		d = value_list[3]
		execdb_course(a,b,c,d)
	print 'insert all records'
	return



def execdb_create(table_name):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 如果数据表已经存在使用 execute() 方法删除表。
	cursor.execute("DROP TABLE IF EXISTS "+table_name)
	# 创建数据表SQL语句
	sql = "CREATE TABLE "+table_name+"(course_id int(10) NOT NULL,course_name varchar(20) not null,credits int(6) not null,course_type varchar(6) not null,PRIMARY KEY(course_id))default charset=utf8;"
	try:
		cursor.execute(sql)
		db.commit()
		print 'create table sql ok'
	except Exception, e:
		# Rollback in case there is any error
		db.rollback()
		print 'create table sql fails '+repr(e)
	db.close()
	return

## 操作student表
def execdb_student(stu_id,stu_name,sex,date,department_id):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 构造sql插入语句
	sql = "insert into student(stu_id,stu_name,sex,birth,department_id)values("+stu_id+",'"+stu_name+"','"+sex+"','"+date+"',"+department_id+");"
	# 使用execute方法执行SQL语句
	try:
		# 执行sql语句
		cursor.execute(sql)
		# 提交到数据库执行
		db.commit()
		print 'sql ok'
	except Exception, e:
		# Rollback in case there is any error
		db.rollback()
		print 'sql fails '+repr(e)
	# 关闭数据库连接
	db.close()
	return

## 操作course表
def execdb_course(course_id,course_name,credits,course_type):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 构造sql插入语句
	sql = "insert into course(course_id,course_name,credits,course_type)values("+course_id+",'"+course_name+"',"+credits+",'"+course_type+"');"
	# 使用execute方法执行SQL语句
	try:
		print sql
		# 执行sql语句
		cursor.execute(sql)
		# 提交到数据库执行
		db.commit()
		print 'sql ok'
	except Exception, e:
		# Rollback in case there is any error
		db.rollback()
		print 'sql fails '+repr(e)
	# 关闭数据库连接
	db.close()
	return


if __name__=='__main__':
	filename_list=[]
	filename_list.append('department.txt')
	filename_list.append('department.txt')
	
	filename = './course.txt'
	begin(filename)
