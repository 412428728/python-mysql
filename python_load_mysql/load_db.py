#!/usr/bin/python
# coding: UTF-8
## Author: xxhh
## Time: 2016-10-15 21:08

import MySQLdb
import re

## 操作输入文件
def begin(file_name, variables_list, index):
	table_name=re.search('\.\/(\w+)\.txt',file_name).group(1)
	if index==0:
		execdb_create_department(table_name)
	elif index==1:
		execdb_create_selectcourse(table_name)
	elif index==2:
		execdb_create_teach(table_name)
	elif index==3:
		execdb_create_teacher(table_name)
		
	f = open(file_name,'r')
	line_list = list() # 用来存储预处理后的行的行列表
	# 开始进行预处理，去掉空行以及每行前后的空白符，以及去掉BOM
	index_line = 0
	for line in f.readlines():                          #依次读取每行  
		try:
			if index_line==0:
				line=line[3:]
				index_line+=1
			line = line.strip()                             #去掉每行头尾空白  
			if not len(line):       						#判断是否是空行 
				continue                                    #是的话，跳过不处理 
			line_list.append(line.decode('utf-8').encode('utf-8'))                             #保存
		except:
			print index_line
	# 预处理完毕，开始构建sql语句需要的可变参数
	variables_length = len(variables_list[index]) # 表属性个数
	for line in line_list:
		value_list = re.split('\s+',line)
		print value_list[0]
		if len(value_list)!=variables_length:
			print 'Arguments number are not same!'
			continue		
		if index==0:
			execdb_department(table_name, variables_list[index], value_list)
		elif index==1:
			execdb_selectcourse(table_name, variables_list[index], value_list)
		elif index==2:
			execdb_teach(table_name, variables_list[index], value_list)
		elif index==3:
			execdb_teacher(table_name, variables_list[index], value_list)
		
	print 'insert all records'
	return

def execdb_create_department(table_name):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 如果数据表已经存在使用 execute() 方法删除表。
	cursor.execute("DROP TABLE IF EXISTS "+table_name)
	# 创建数据表SQL语句
	sql = "create table "+table_name+" (department_id int(10) NOT NULL,department_name varchar(10) NOT NULL,PRIMARY KEY(department_id))default charset=utf8;"
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

## 操作department表
def execdb_department(table_name, variables, value_list):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 构造sql插入语句
	variable_sql=''
	for variable in variables:
		variable = ','+variable
		variable_sql = variable_sql+variable
	variable_sql = variable_sql[1:]
	
	value_sql=value_list[0]+",'"+value_list[1]+"'"
	
	sql = "insert into "+table_name+"("+variable_sql+")values("+value_sql+");"
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

def execdb_create_selectcourse(table_name):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 如果数据表已经存在使用 execute() 方法删除表。
	cursor.execute("DROP TABLE IF EXISTS "+table_name)
	# 创建数据表SQL语句
	sql = "create table "+table_name+" (course_id int(10) NOT NULL,stu_id int(10) NOT NULL,tea_id int(10) NOT NULL,grade int(10) NOT NULL,PRIMARY KEY(course_id,stu_id,tea_id))default charset=utf8;"
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

## 操作department表
def execdb_selectcourse(table_name, variables, value_list):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 构造sql插入语句
	variable_sql=''
	for variable in variables:
		variable = ','+variable
		variable_sql = variable_sql+variable
	variable_sql = variable_sql[1:]
	
	value_sql=value_list[0]+","+value_list[1]+","+value_list[2]+","+value_list[3]
	
	sql = "insert into "+table_name+"("+variable_sql+")values("+value_sql+");"
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

def execdb_create_teach(table_name):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 如果数据表已经存在使用 execute() 方法删除表。
	cursor.execute("DROP TABLE IF EXISTS "+table_name)
	# 创建数据表SQL语句
	sql = "create table "+table_name+" (tea_id int(10) NOT NULL,course_id int(10) NOT NULL,PRIMARY KEY(tea_id, course_id))default charset=utf8;"
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

## 操作department表
def execdb_teach(table_name, variables, value_list):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 构造sql插入语句
	variable_sql=''
	for variable in variables:
		variable = ','+variable
		variable_sql = variable_sql+variable
	variable_sql = variable_sql[1:]
	
	value_sql=value_list[0]+","+value_list[1]
	
	sql = "insert into "+table_name+"("+variable_sql+")values("+value_sql+");"
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

def execdb_create_teacher(table_name):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 如果数据表已经存在使用 execute() 方法删除表。
	cursor.execute("DROP TABLE IF EXISTS "+table_name)
	# 创建数据表SQL语句
	sql = "create table "+table_name+" (tea_id int(10) NOT NULL,tea_name varchar(10) NOT NULL,title varchar(10) NOT NULL,department_id int(10) NOT NULL,PRIMARY KEY(tea_id))default charset=utf8;"
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

## 操作department表
def execdb_teacher(table_name, variables, value_list):
	# 设置字符集，连接数据库
	db = MySQLdb.connect(host='localhost', user='root',passwd='g19941011', db='database_homework_db', charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 构造sql插入语句
	variable_sql=''
	for variable in variables:
		variable = ','+variable
		variable_sql = variable_sql+variable
	variable_sql = variable_sql[1:]
	
	value_sql=value_list[0]+",'"+value_list[1]+"','"+value_list[2]+"',"+value_list[3]
	
	sql = "insert into "+table_name+"("+variable_sql+")values("+value_sql+");"
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




if __name__=='__main__':
	# set file name list 
	filename_list=[]
	filename_list.append('./department.txt')
	filename_list.append('./selectcourse.txt')
	filename_list.append('./teach.txt')
	filename_list.append('./teacher.txt')
	# set every table's variables' name list by above order
	variables_list=[]
	variables=['department_id','department_name']
	variables_list.append(variables)
	variables=['course_id','stu_id','tea_id','grade']
	variables_list.append(variables)
	variables=['tea_id','course_id']
	variables_list.append(variables)
	variables=['tea_id','tea_name','title','department_id']
	variables_list.append(variables)
	# begin 
	index = 0; # 告诉variables_list这是第几次循环
	for filename in filename_list:
		begin(filename, variables_list, index)
		index+=1
