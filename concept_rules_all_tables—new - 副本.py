# -*- coding: utf-8 -*-
"""
Created on Sat Oct  2 10:15:25 2021

@author: huxin
"""

import pyodbc
import time
import copy
import re
from operator import eq
from itertools import combinations

#特定类型如Actor的谓词和类型
#database_name='Garden_rule'
type_predicate='<http://www.w3.org/1999/02/22_rdf_syntax_ns#type>'
#type_object='<http://dbpedia.org/ontology/Sport>'#454	459	71408
type_object='<http://dbpedia.org/ontology/Garden>'#358	42	17396
#type_object='<http://dbpedia.org/ontology/Cave>'#525	206	38751

#type_predicate='<http://dbpedia.org/ontology/type>'
#type_object='<http://dbpedia.org/resource/River>'#223	254	30782




#**********************************获取Garden的实例并保存到instances表中******************************************
def obtain_instances():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #对DBpedia2016中的triple表进行查询，找到所有的实例并保存
    sql="select distinct [subject] into ["+database_name+"].[dbo].[instance_original] from [DBpedia2016].[dbo].[triples] where [predicate]='"+type_predicate+"' and [object]='"+type_object+"'"
    cursor.execute(sql)
    cnxn.commit()
    
    #在instance表中增加一列序号id
    sql="SELECT Row_Number() over ( order by getdate() ) as id , * into ["+database_name+"].[dbo].[instance] FROM ["+database_name+"].[dbo].[instance_original]"
    cursor.execute(sql)
    cnxn.commit()
    
    #删除没有序号的原始实例表
    sql="drop table ["+database_name+"].[dbo].[instance_original]"
    cursor.execute(sql)
    cnxn.commit()
    
    #关闭连接
    cnxn.close()
    
    
#**********************************获取Garden的实例相关的所有三元组并保存到triples表中******************************************
def obtain_triples():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #获得所有的三元组
    sql="select distinct [subject],[predicate],[object] into ["+database_name+"].[dbo].[triple] from [DBpedia2016].[dbo].[triples] where [subject] in (select [subject] from ["+database_name+"].[dbo].[instance])"
    cursor.execute(sql)
    cnxn.commit()
    
    #关闭连接
    cnxn.close()
    
    
#**********************************获取triple表中所有属性predicate，以备构建信息表时使用******************************************
def obtain_predicate():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #获取triple中所有的predicate，并保存到表predicate
    sql="select distinct [predicate] into ["+database_name+"].[dbo].[predicate_temp] from ["+database_name+"].[dbo].[triple]"
    cursor.execute(sql)
    cnxn.commit()
    
    #在predicate表中增加一列序号id
    sql="SELECT Row_Number() over ( order by getdate() ) as id , * into ["+database_name+"].[dbo].[predicate] FROM ["+database_name+"].[dbo].[predicate_temp]"
    cursor.execute(sql)
    cnxn.commit()
    sql="drop table ["+database_name+"].[dbo].[predicate_temp]"
    cursor.execute(sql)
    cnxn.commit()    

    #将triple中的predicate替换为id
    sql="select [id],[predicate] from ["+database_name+"].[dbo].[predicate]"
    cursor.execute(sql)
    results=cursor.fetchall()
    for result in results:
        sql="update ["+database_name+"].[dbo].[triple] set predicate='"+str(result[0])+"' where predicate='"+str(result[1])+"'" 
        cursor.execute(sql)
        cnxn.commit()
    
    #关闭连接
    cnxn.close()
    
    
def obtain_all_instance_types():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    sql="select * into ["+database_name+"].[dbo].[all_instance_types] from [DBpedia2016].[dbo].[triples] where [predicate]='<http://dbpedia.org/ontology/type>' or [predicate]='<http://dbpedia.org/property/type>' or [predicate]='<http://purl.org/dc/elements/1.1/type>' or [predicate]='<http://www.w3.org/1999/02/22_rdf_syntax_ns#type>'"
    cursor.execute(sql)
    cnxn.commit()
    
    #关闭连接
    cnxn.close()
    
    
#**********************************获取triple表中object的类型，以备构建信息表使用******************************************
def obtain_object_types():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #获取triple中所有的object，并保存到triple_object
    sql="select distinct [object] into ["+database_name+"].[dbo].[triple_object] from ["+database_name+"].[dbo].[triple]"
    cursor.execute(sql)
    cnxn.commit()
    
    #获取triple中所有的object的类型三元组，并保存到object_types:
    sql="select distinct [subject] as [object],[predicate] as [type],[object] as [object_type] into ["+database_name+"].[dbo].[object_types] from ["+database_name+"].[dbo].[all_instance_types] where [subject] in (select [object] from ["+database_name+"].[dbo].[triple_object])"
    #sql="select distinct [subject] as [object],[predicate] as [type],[object] as [object_type] into ["+database_name+"].[dbo].[object_types] from [DBpedia2016].[dbo].[triples] where [subject] in (select [object] from ["+database_name+"].[dbo].[triple_object]) and ([predicate]='<http://dbpedia.org/ontology/type>' or [predicate]='<http://dbpedia.org/property/type>' or [predicate]='<http://purl.org/dc/elements/1.1/type>' or [predicate]='<http://www.w3.org/1999/02/22_rdf_syntax_ns#type>')"
    cursor.execute(sql)    
    cnxn.commit()
    
    #关闭连接
    cnxn.close()
    
    
#**********************************获取正例数据******************************************
def obtain_positive_data():
    print("obtain_instance")
    obtain_instances()#创建表instance
    
    print("obtain_triples")
    obtain_triples()#创建表triple
    
    print("obtain_predicate")
    obtain_predicate()#创建表predicate/similar_degree
    
    print("obtain_all_instance_types")
    obtain_all_instance_types()#获得所有实例的类型
    
    print("obtain_object_types")
    obtain_object_types()#创建表triple_object/object_types
    
    
#**********************************#创建object_type的id******************************************
def create_object_type_id():
	#数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
	#获取三元组中非实例的object,并生成它的类型
    #object中带有类型的
    sql="select distinct [object] from ["+database_name+"].[dbo].[triple] where [object] not like '<%'"
    cursor.execute(sql)
    objects=cursor.fetchall()
    
    
    for objectx in objects:
        sql="insert into ["+database_name+"].[dbo].[object_types] ([object],[type],[object_type]) values "
        objectx=str(objectx[0])
        typex=re.findall(r"<(.+?)>",objectx)
        if typex!=[]:
            typex=typex[0]
        else:
            typex="string"
        sql+="('"+objectx+"','type','"+typex+"')"
        cursor.execute(sql)
        cnxn.commit()
        
    #获取所有的object_type放入表object_type_id
    sql="select distinct [object_type] into ["+database_name+"].[dbo].[object_type_id_temp] from ["+database_name+"].[dbo].[object_types]"
    cursor.execute(sql)
    cnxn.commit()

	#在object_type_id表中增加一列序号id
    sql="SELECT Row_Number() over ( order by getdate() ) as id , * into ["+database_name+"].[dbo].[object_type_id] FROM ["+database_name+"].[dbo].[object_type_id_temp]"
    cursor.execute(sql)
    cnxn.commit()
    sql="drop table ["+database_name+"].[dbo].[object_type_id_temp]"
    cursor.execute(sql)
    cnxn.commit()    

    #关闭连接
    cnxn.close()


#**********************************#创建object的id******************************************
def create_object_id():
	#数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
	#获取所有的object_type放入表object_type_id
    sql="select distinct [object]  into ["+database_name+"].[dbo].[object_id_temp] from ["+database_name+"].[dbo].[triple]"
    cursor.execute(sql)
    cnxn.commit()

	#在object_type_id表中增加一列序号id
    sql="SELECT Row_Number() over ( order by getdate() ) as id , * into ["+database_name+"].[dbo].[object_id] FROM ["+database_name+"].[dbo].[object_id_temp]"
    cursor.execute(sql)
    cnxn.commit()
    sql="drop table ["+database_name+"].[dbo].[object_id_temp]"
    cursor.execute(sql)
    cnxn.commit()    

    #关闭连接
    cnxn.close()
    
    
#**********************************#创建正向的四种信息表******************************************
def create_information_table():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #获取predicate中的值构建属性是否存在的信息表
    sql="select count([predicate]) from ["+database_name+"].[dbo].[predicate]"
    cursor.execute(sql)
    sql="create table ["+database_name+"].[dbo].information_table_yes_no(instance_no int,"
    predicate_number=cursor.fetchone()
    predicate_number=predicate_number[0]
    str_values=[]
    str_predicates=""
    for i in range(1,predicate_number+1):
        sql+=" p_"+str(i)+" int,"
        str_predicates+="p_"+str(i)+","
        str_values.append(0)
    sql=sql[0:-1]
    str_predicates=str_predicates[0:-1]
    sql+=")"
    cursor.execute(sql)
    cnxn.commit()
    
    #创建属性值个数信息表
    sql=str(sql)
    sql=sql.replace("information_table_yes_no","information_table_num")
    cursor.execute(sql)
    cnxn.commit()
    
    #创建属性值类型信息表
    sql=str(sql)
    sql=sql.replace("int","nvarchar(MAX)")
    sql=sql.replace("instance_no nvarchar(MAX)","instance_no int")
    sql=sql.replace("information_table_num","information_table_type")
    cursor.execute(sql)
    cnxn.commit()
    
    #创建属性值信息表
    sql=str(sql)
    sql=sql.replace("information_table_type","information_table_values")
    cursor.execute(sql)
    cnxn.commit()
    
    
    #关闭连接
    cnxn.close()
    
    return str_predicates,str_values,predicate_number
    
   
#**********************************#返回object的type******************************************
def insert_object_type_and_object(idx,instance,predicate_number,str_predicates):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #获取实例相关的predicate
    sql="select distinct [predicate] from ["+database_name+"].[dbo].[triple] where [subject]='"+str(instance)+"' order by [predicate]"
    cursor.execute(sql)
    predicates=cursor.fetchall()
    
    types_set=['0']*predicate_number
    object_set=['0']*predicate_number
    
    #对每个谓词分别获得object的类型的id
    for predicate in predicates:
        predicate=predicate[0]
        sql="select [id] from ["+database_name+"].[dbo].[object_type_id] where [object_type] in (select distinct [object_type] from ["+database_name+"].[dbo].[object_types] where [object] in (select distinct [object] from ["+database_name+"].[dbo].[triple] where [predicate]='"+str(predicate)+"' and [subject]='"+str(instance)+"'))"
        cursor.execute(sql)
        type_ids=cursor.fetchall()
        types=[]
        for type_id in type_ids:
            types.append(type_id[0])
        types=str(types)
        types_set[int(predicate)-1]=types[1:-1]
       
        
        sql="select distinct [id] from ["+database_name+"].[dbo].[object_id] where [object] in (select distinct [object] from ["+database_name+"].[dbo].[triple] where [predicate]='"+str(predicate)+"' and [subject]='"+str(instance)+"')"
        cursor.execute(sql)
        objects=cursor.fetchall()
        object_id=[]
        for objectx in objects:
            object_id.append(objectx[0])
        object_id=str(object_id)
        object_set[int(predicate)-1]=object_id[1:-1]
        
        
    types_set=str(types_set)
    types_set=types_set[1:-1]
    object_set=str(object_set)
    object_set=object_set[1:-1]
    
    #插入实例的object的类型id
    sql="insert into ["+database_name+"].[dbo].information_table_type(instance_no,"+str(str_predicates)+") values ("+str(idx)+","+str(types_set)+")"
    cursor.execute(sql)
    cnxn.commit()
    
    #插入实例的object的id
    sql="insert into ["+database_name+"].[dbo].information_table_values(instance_no,"+str(str_predicates)+") values ("+str(idx)+","+str(object_set)+")"
    cursor.execute(sql)
    cnxn.commit()
    
    #关闭连接
    cnxn.close()
    

#**********************************#向正向的四种信息表中插入元素******************************************
def insert_into_information_table(str_predicates,str_values,predicate_number):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()

    #获取instance中的id和subject
    sql="select [id],[subject] from ["+database_name+"].[dbo].[instance] order by [id]"
    cursor.execute(sql)
    ids_instances=cursor.fetchall()
    
    #向属性是否存在的信息表中插入数据，向熟悉值个数信息表中插入数据
    for id_instance in ids_instances:
        #获取属性和属性值
        idx=id_instance[0]
        instance=id_instance[1]
        sql="select [predicate],[object] from ["+database_name+"].[dbo].[triple] where [subject]='"+str(instance)+"'"
        cursor.execute(sql)
        result=cursor.fetchall()
    
        #判断谓词是否存在，以及谓词出现次数统计，并记录所有的object
        newvalues=copy.copy(str_values)
        newvalues_num=copy.copy(str_values)
        objects=[]
        for i in range(0,len(result)):
            x=int(result[i][0])-1
            newvalues[x]=1
            newvalues_num[x]=newvalues_num[x]+1
            objects.append(result[i][1])
        
        #插入yes_no信息表的值
        newvalues=str(newvalues)
        newvalues=newvalues[1:-1]
        sql="insert into ["+database_name+"].[dbo].information_table_yes_no(instance_no,"+str(str_predicates)+") values ("+str(idx)+","+str(newvalues)+")"
        cursor.execute(sql)
        cnxn.commit()
        
        #插入属性个数信息表的值
        newvalues_num=str(newvalues_num)
        newvalues_num=newvalues_num[1:-1]
        sql="insert into ["+database_name+"].[dbo].information_table_num(instance_no,"+str(str_predicates)+") values ("+str(idx)+","+str(newvalues_num)+")"
        cursor.execute(sql)
        cnxn.commit()
        
        #插入object类型#插入object值
        insert_object_type_and_object(idx,instance,predicate_number,str_predicates)
        
    #关闭连接
    cnxn.close()
    
    
 #**********************************创建信息表******************************************
def create_and_insert_information_table():
    create_object_type_id()#创建object_type的id
    create_object_id()#创建object的id
    str_predicates,str_values,predicate_number=create_information_table()#创建属性是否存在的信息表#创建属性值个数信息表#创建属性值类型信息表
    insert_into_information_table(str_predicates,str_values,predicate_number)   
    
    
#**********************************获取所有包含类型相关属性的三元组******************************************
def obtain_triple_by_predicate():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #通过谓词划分反例三元组
    sql="select ru.[id],data.[id] from ["+database_name+"].[dbo].[predicate] as ru, [DBpedia2016predicates].[dbo].[predicate] as data where ru.[predicate]=data.[predicate] order by ru.id"
    cursor.execute(sql)
    results=cursor.fetchall()
    
    for result in results:
        rule_id=result[0]
        print(rule_id)
        data_id=result[1]
        #对DBpedia201610中的表依次查询，找到所有的实例相关的三元组
        sql="select [subject],[object] into ["+database_name+"].[dbo].[negetive_predicate_"+str(rule_id)+"] from [DBpedia2016predicates].[dbo].[predicate_"+str(data_id)+"] where [subject] not in (select [subject] from ["+database_name+"].[dbo].[instance])"
        cursor.execute(sql)
        cnxn.commit()
        
        sql="select distinct [subject] into ["+database_name+"].[dbo].[negetive_instance_"+str(rule_id)+"] from ["+database_name+"].[dbo].[negetive_predicate_"+str(rule_id)+"]"
        cursor.execute(sql)
        cnxn.commit()


    #关闭连接
    cnxn.close()
    
def create_negetive_object_type_table():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    sql="select count(id) from ["+database_name+"].[dbo].[predicate]"
    cursor.execute(sql)
    predicate_num=cursor.fetchone()
    predicate_num=int(predicate_num[0])
    
    for i in range(1,predicate_num+1):
        print(i)
        sql="select distinct [object] into ["+database_name+"].[dbo].[negetive_object_"+str(i)+"] from ["+database_name+"].[dbo].[negetive_predicate_"+str(i)+"] where [object] like '<%>'"
        cursor.execute(sql)
        cnxn.commit()
    
        sql="select * into ["+database_name+"].[dbo].[negetive_object_type_"+str(i)+"] from ["+database_name+"].[dbo].[all_instance_types] where [subject] in (select [object] from ["+database_name+"].[dbo].[negetive_object_"+str(i)+"])"
        cursor.execute(sql)
        cnxn.commit()
    
        sql="drop table ["+database_name+"].[dbo].[negetive_object_"+str(i)+"]"
        cursor.execute(sql)
        cnxn.commit()
    #关闭连接
    cnxn.close()
    
    
#**********************************通过谓词获得反例数据******************************************
def obtian_negetive_data():
    obtain_triple_by_predicate()
    create_negetive_object_type_table()
    



#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#*********************************以上是数据获取，以下是信息表生成极大频繁模式
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************

#*********************************从信息表的属性值集和实例集获得原始的频繁集*********************************
def obtain_initial_fre_instance_and_attribute_value(possible_set,instance_set_by_each_attribute_value):
    instances_set=[]
    one_value_set=[]
    multiple_values_set=[]
    #对每个属性依次筛选频繁的
    i=0
    for attribute_id in possible_set:
        #获取每个属性对应的实例集和属性值集
        if len(instance_set_by_each_attribute_value[attribute_id])==1:
            one_value_set.append(i)
        else:
            multiple_values_set.append(i)
        i=i+1
        instances_set.append(instance_set_by_each_attribute_value[attribute_id])
                
    return instances_set,one_value_set,multiple_values_set

    
#**********************************求两个list集的交集******************************************
def obtain_intersection_set_for_multiple_vs_multiple(lista_sets,listb_sets):
    intersection_set=[]
    #lista_sets中每个lista都和setx求交集，并保存到listb_sets中
    for lista in lista_sets:
        for listb in listb_sets:
            setx=list(set(lista).intersection(set(listb)))
            #对listb_sets去除重复
            if intersection_set.count(setx)==0:
                intersection_set.append(setx)
    
    return intersection_set


#**********************************对于多值属性获得实例交集**********************************
def obtain_intersection_set_from_multiple_value(intersection_instance_set_for_one_value,instances_set,multiple_values_set,threshold_num):
    if intersection_instance_set_for_one_value==[]:
        sets=instances_set[multiple_values_set[0]]
        for i in multiple_values_set[1:]:
            sets=obtain_intersection_set_for_multiple_vs_multiple(instances_set[i],sets)
    else:
        sets=intersection_instance_set_for_one_value
        for i in multiple_values_set:
            sets=obtain_intersection_set_for_multiple_vs_multiple(instances_set[i],sets)
     
    intersection_set=[]
    for setx in sets:
        if len(setx)>threshold_num:
            intersection_set.append(setx)
    return intersection_set
        
    
    
def obtain_intersection_set_for_types_or_values(intersection_set,possible_set,information_table,threshold_num,instance_set_by_each_attribute_value):
    #intersection_set=[1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16...358]
    #possible_set=[2, 8, 14, 16, 17, 18, 22, 28, 31, 36, 37, 38, 40, 41]
    #threshold_num=cov*instance_count
    
    #从信息表的属性值集和实例集获得原始的频繁集
    instances_set,one_value_set,multiple_values_set=obtain_initial_fre_instance_and_attribute_value(possible_set,instance_set_by_each_attribute_value)
    
    #对于只有一个属性值（简称单值）的属性，求实例集的交集
    if one_value_set!=[]:
        intersection_instance_set_for_one_value=instances_set[one_value_set[0]]
        for i in one_value_set[1:]:
            intersection_instance_set_for_one_value=obtain_intersection_set_for_multiple_vs_multiple(instances_set[i],intersection_instance_set_for_one_value)
    
        #如果单值属性的实例个数不频繁，则直接返回空集
        if len(intersection_instance_set_for_one_value[0])<threshold_num:
            return []
    else:
        intersection_instance_set_for_one_value=[]
        
    intersection_set=obtain_intersection_set_from_multiple_value(intersection_instance_set_for_one_value,instances_set,multiple_values_set,threshold_num)
    
    return intersection_set


#**********************************求多个list的交集******************************************
def obtain_intersection_set(instance_set_by_attribute,possible_set):
    intersection_set=instance_set_by_attribute[possible_set[0]]
    #print(possible_set)
    for i in range(1,len(possible_set)):
        intersection_set=list(set(intersection_set).intersection(set(instance_set_by_attribute[possible_set[i]])))
    return intersection_set


#**********************************判断是否频繁******************************************
def is_frequent_and_return_instance_set(table_no,instance_set_by_attribute,possible_set,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    intersection_set=obtain_intersection_set(instance_set_by_attribute,possible_set)
    if len(intersection_set)<cov*instance_count:
        return []
    else:
        if table_no==3 or table_no==4:
            intersection_set=obtain_intersection_set_for_types_or_values(intersection_set,possible_set,information_table,cov*instance_count,instance_set_by_each_attribute_value)
        return intersection_set
            
    
#**********************************对于other_sub_set中的模式划分频繁或非频繁******************************************
def divide_fre_or_not(table_no, other_sub_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    fre_set=[]
    non_fre_set=[]
    
    #对于other_sub_set中的模式划分频繁或非频繁
    for setx in other_sub_set:
        x=is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,setx,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        if x!=[]:
            fre_set.append(setx)
            #print(len(fre_set))
        else:
            non_fre_set.append(setx)
            #print(len(non_fre_set))
    return fre_set,non_fre_set,

        
#**********************************从patterns中删除不包含difference_set的pattern，如果flag=0，则直接删除，如果flag=1，则需要先去重******************************************
def delete_difference_pattern(patterns,difference_set,flag):
    setx=set(difference_set)
    #如果flag=1，则需要先去重
    if flag==1:
        result=[patterns[0]]
        for i in range(0,len(patterns)-1):
            if not eq(patterns[i+1],patterns[i]):
                result.append(patterns[i+1])
        patterns=result
        
    #筛选并返回patterns中不包含difference_set的pattern
    result=[]
    for pattern in patterns:
        if setx.issubset(set(pattern)):
            result.append(pattern)
            
    return result
    

#**********************************求setx的所有低一阶的模式******************************************
def size_lower_pattern(setx):
    sety=[]
    for i in range(0,len(setx)):
        x=setx[i]
        setx.remove(x)
        sety.append(copy.copy(setx))
        setx.insert(i,x)
    return sety


#**********************************判断是否是子集关系******************************************
def not_exist(all_set,sub_set):
    sub_set=set(sub_set)
    length=len(sub_set)
    for i in range(int(len(all_set)-1),int(length-1),-1):
        sets=all_set[i]
        for setx in sets:
            if sub_set.issubset(set(setx)):
                return 0
    return 1


#**********************************在保证不重复的情况下，将模式集setsb中的模式加入模式集setsa**********************************
def add_sets_to_fre_sets(setsa,setsb):
    for setx in setsb:
        if not_exist(setsa,setx):
            setsa[len(setx)].append(setx)
    return setsa


#**********************************在保证不重复的情况下，将模式集setsb中的模式加入模式集setsa**********************************
def add_sets_to_non_fre_sets(setsa,setsb):
    for setx in setsb:
        if not_exist(setsa,setx):
            setsa[len(setx)].append(setx)
    return setsa


#**********************************判断集合sets是否为空，非空返回1，空则返回0**********************************
def non_empty(sets):
    for setx in sets:
        if len(setx)!=0:
            return 1
    return 0


#*********************************返回sets中的第一个最高阶模式***********************************
def first_set(sets):
    for i in range(int(len(sets)-1),-1,-1):
        setx=sets[i]
        if len(setx)!=0:
            return setx[0]
    return []

#*********************************setx中可能存在某些属性，这些属性在信息表中没有类型，需要把这些属性删除***********************************
def delete_empty_attribute(setx,instance_set_by_attribute):
    sety=[]
    for attribute in setx:
        if instance_set_by_attribute[attribute]!=[]:
            sety.append(attribute)
    return sety


#**********************************生成实例集，实例集以单个属性为划分，即每个属性分别有哪些实例******************************************
def obtain_instance_set_by_each_attribute(table_no,information_table,instance_count,predicate_number,cov):
    #初始化实例集，属性1有哪些实例，属性2有哪些实例
    instance_set_by_each_attribute=[]
    for i in range(0,predicate_number):
        instance_set_by_each_attribute.append([])
    
    #将属性值集转换为按每个属性划分的实例集
    if table_no==1 or table_no==2:#table1和table2是数值型
        for i in range(0,len(information_table)):
            instance_values=information_table[i]
            for j in range(0,len(instance_values)):
                if instance_values[j]!=0:#table1和table2是数值型
                    instance_set_by_each_attribute[j].append(i+1)
    else:#table3和table3是文本型
        for i in range(0,len(information_table)):
            instance_values=information_table[i]
            for j in range(0,len(instance_values)):
                if instance_values[j]!='0' and instance_values[j]!='':#table3和table3是文本型
                    instance_set_by_each_attribute[j].append(i+1)
    
    for i in range(0,len(instance_set_by_each_attribute)):
        if len(instance_set_by_each_attribute[i])<instance_count*cov:
            instance_set_by_each_attribute[i]=[]
    return instance_set_by_each_attribute
    #instance_set_by_each_attribute,list中每个元素都是一个list，每个子list代表拥有第i个属性的所有实例集
    

def obtain_instance_set_by_attribute_value(instance_count,predicate_number,information_table,cov):
    #instance_set=[1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16...358]
    #possible_set=[2, 8, 14, 16, 17, 18, 22, 28, 31, 36, 37, 38, 40, 41]
    #初始化实例集，属性1有哪些实例，属性2有哪些实例
    attribute_value_set_by_each_attribute=[]
    instance_set_by_each_attribute_value=[]
    for i in range(0,predicate_number):
        attribute_value_set_by_each_attribute.append([])
        instance_set_by_each_attribute_value.append([])
    
    #将属性值集转换为按每个属性划分的实例集
    for i in range(0,instance_count):
        instance_information=information_table[i]#保存一个实例的相关信息
        for j in range(0,predicate_number):
            values=instance_information[j]#保存每个属性值集的信息
            values=values.split(',')
            for value in values:#对属性值集中的每个属性值做操作
                if value=='0' or value=='':
                    continue
                #如果不存在属性值，则添加属性值
                if attribute_value_set_by_each_attribute[j].count(value)==0:
                    attribute_value_set_by_each_attribute[j].append(value)
                    instance_set_by_each_attribute_value[j].append([])
                #获得属性值的位置，并在对应位置插入实例编号
                location=attribute_value_set_by_each_attribute[j].index(value)
                instance_set_by_each_attribute_value[j][location].append(i+1)
    
    #只保留频繁的实例集和属性值集
    threshold=cov*instance_count
    fre_instances_sets=[]
    #fre_attribute_value_set=[]     
    for instances_sets,attribute_value_set in zip(instance_set_by_each_attribute_value,attribute_value_set_by_each_attribute):
        setx=[]
        #sety=[]
        #for instances,attribute_value in zip(instances_sets,attribute_value_set):
        for instances in instances_sets:
            if len(instances)>threshold:
                setx.append(instances)
                #sety.append(attribute_value)
        fre_instances_sets.append(setx)
        #fre_attribute_value_set.append(sety)
                

    return fre_instances_sets  


#**********************************从信息表中获得事务数据******************************************  
def obtain_transaction_data(table_no):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #生成位示图
    sql="select * from ["+database_name+"].[dbo].[information_table_yes_no] order by instance_no"
    cursor.execute(sql)
    location_graph=cursor.fetchall()
    
    #读取表中的数据
    if table_no==1:
        attribute_values_set=location_graph
        for i in range(0,len(location_graph)):
            location_graph[i]=location_graph[i][1:]
    else:
        attribute_values_set=[]
        for i in range(0,len(location_graph)):
            location_graph[i]=location_graph[i][1:]
            if table_no==2:
                sql="select * from ["+database_name+"].[dbo].[information_table_num] where instance_no="+str(i+1)
            elif table_no==3:
                sql="select * from ["+database_name+"].[dbo].[information_table_type] where instance_no="+str(i+1)
            elif table_no==4:
                sql="select * from ["+database_name+"].[dbo].[information_table_values] where instance_no="+str(i+1)
            else:
                print("the table_no is error")
            cursor.execute(sql)
            attribute_values=cursor.fetchone()
        
        
            attribute_values_set.append(attribute_values[1:])
        
    
    #关闭连接
    cnxn.close()
    
    return location_graph,attribute_values_set



#**********************************向下递归******************************************
def recursion_down(table_no, non_fre_pattern,difference_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    #获得non_fre_pattern的低一阶模式
    patterns=size_lower_pattern(non_fre_pattern)
    
    #从patterns中删除不包含difference_set的pattern
    sub_patterns_with_difference=delete_difference_pattern(patterns,difference_set,0)
    
    #对sub_patterns_with_difference划分频繁和非频繁的
    fre_set,non_fre_set=divide_fre_or_not(table_no, sub_patterns_with_difference,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
    
    fre_sets=fre_set
    
    #如果非频繁模式集不为空，则继续向下
    while len(non_fre_set)>0:
        
        #对于集合中的所有非频繁模式，获得所有低一阶的模式
        patterns=[]
        for non_fre_pattern in non_fre_set:
            patterns.extend(size_lower_pattern(non_fre_pattern))
        patterns.sort()
        
        #从patterns中删除不包含difference_set的pattern，参数1代表patterns中可能存在重复模式，需要先去重
        sub_patterns_with_difference=delete_difference_pattern(patterns,difference_set,1)
        
        print(str(len(sub_patterns_with_difference[0]))+"---"+str(len(sub_patterns_with_difference)))
        
        fre_set,non_fre_set=divide_fre_or_not(table_no, sub_patterns_with_difference,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        fre_sets.extend(fre_set)
        
    return fre_sets


def not_exist_in_fre_or_not_fre_set(fre_or_not_fre_sets, setsx):
    setsy=[]
    for setx in setsx:
        if not_exist(fre_or_not_fre_sets,setx)==1:
            setsy.append(setx)
    return setsy
    
def delete_repeat(patterns):
    patterns.sort()
    result=[patterns[0]]
    for i in range(0,len(patterns)-1):
        if not eq(patterns[i+1],patterns[i]):
            result.append(patterns[i+1])
    return result

#**********************************从信息表yes_no中生成规则******************************************
def obtain_patterns(pre_patterns,table_no, instance_set_by_attribute,instance_count,predicate_number,information_table,cov,instance_set_by_each_attribute_value):
    #predicate_values_set,list中每个元素都是一个list，每个子list代表拥有第i个属性的所有实例集
    #predicate_values_num,list中每个元素对应一个属性的实例个数
    #instance_count,总的实例个数
    #predicate_number，谓词个数
    #cov，规则的覆盖度
    
    
    fre_pattern_result=[]
    non_fre_pattern_set=[]
    
    
    if table_no==1:
        #删掉不可能形成规则的属性，
        possible_set=[]
        for i in range(0,predicate_number):
            if len(instance_set_by_attribute[i])<cov*instance_count:
                instance_set_by_attribute[i]=[]
            else:
                possible_set.append(i)
                
        #print("1---"+str(len(possible_set)))
        
        #生成多阶的频繁规则集和非频繁规则集
        n=len(possible_set)+1    
        for i in range(0,n):
            x=[]
            fre_pattern_result.append(x)
            y=[]
            non_fre_pattern_set.append(y)
        

        #将最原始的频繁或非频繁项集加入集合中
        x=is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,possible_set,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        if x!=[]:
            fre_pattern_result[len(possible_set)].append(possible_set)
        else:
            non_fre_pattern_set[len(possible_set)].append(possible_set)
    else:
        
        maximal_set=first_set(pre_patterns)
        if maximal_set!=[]:
            n=len(maximal_set)+1
            for i in range(0,n):
                x=[]
                fre_pattern_result.append(x)
                y=[]
                non_fre_pattern_set.append(y)
        
            #将先验模式放入other_sub_set
            other_sub_set=[]
            for sets in pre_patterns:
                if sets==[]:
                    continue
                for setx in sets:
                    setx=delete_empty_attribute(setx,instance_set_by_each_attribute_value)
                    if setx!=[]:
                        other_sub_set.append(setx)
            #对other_sub_set中的先验模式划分频繁或非频繁
            fre_set,non_fre_set=divide_fre_or_not(table_no,other_sub_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
            if fre_set!=[]:
                fre_pattern_result=add_sets_to_fre_sets(fre_pattern_result,fre_set)
            if non_fre_set!=[]:
                non_fre_pattern_set=add_sets_to_non_fre_sets(non_fre_pattern_set,non_fre_set)

    #不频繁集非空
    num=1
    while non_empty(non_fre_pattern_set):
        
    
        #*****************************************************************************************
        other_sub_set=[]
        
       
        
        maximal_set=first_set(non_fre_pattern_set)
        size=len(maximal_set)
        setsx=non_fre_pattern_set[len(maximal_set)]
        x=int(500000/size)
        for setx in setsx[:x]:
            other_sub_set.extend(size_lower_pattern(setx))

        non_fre_pattern_set[len(maximal_set)]=setsx[x:]
        
        #print(str(len(other_sub_set[0]))+"---"+str(len(other_sub_set)))
        
        other_sub_set=delete_repeat(other_sub_set)
        
        #print(str(len(other_sub_set[0]))+"---"+str(len(other_sub_set)))
        
        
        other_sub_set=not_exist_in_fre_or_not_fre_set(fre_pattern_result,other_sub_set)
        #other_sub_set=not_exist_in_fre_or_not_fre_set(non_fre_pattern_set,other_sub_set)
        '''
        if other_sub_set!=[]:
            #print(str(len(other_sub_set[0]))+"---"+str(len(other_sub_set)))
            print(str(len(other_sub_set[0]))+"---"+str(len(non_fre_pattern_set[len(maximal_set)])))
        '''
        num=num+len(other_sub_set)
        
        fre_set,non_fre_set=divide_fre_or_not(table_no, other_sub_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        
        #如果都是频繁的，则直接加入频繁集
        if fre_set!=[]:
            fre_pattern_result[len(fre_set[0])].extend(fre_set)
        if non_fre_set!=[]:
            non_fre_pattern_set[len(non_fre_set[0])].extend(non_fre_set)
        
        '''
        
        #***********************************以上，以下，二选一******************************************************
        
        other_sub_set=[]
        difference_set=[]
        
        #第一个不频繁集作为极大集被处理
        maximal_set=first_set(non_fre_pattern_set)
       # print(str(len(maximal_set))+"---"+str(len(non_fre_pattern_set[len(maximal_set)])))
        non_fre_pattern_set[len(maximal_set)].remove(maximal_set)
        
        
        #递归获得低一阶的子模式，并划分是否频繁
        #recursion_obtain_sub_set(len(maximal_set)-1,[],maximal_set,other_sub_set,difference_set)
        other_sub_set=size_lower_pattern(maximal_set)
        
        other_sub_set=not_exist_in_fre_or_not_fre_set(fre_pattern_result,other_sub_set)
        other_sub_set=not_exist_in_fre_or_not_fre_set(non_fre_pattern_set,other_sub_set)
        
        num=num+len(other_sub_set)
    
        
        fre_set,non_fre_set=divide_fre_or_not(table_no, other_sub_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        
    
        #如果都是频繁的，则直接加入频繁集
        if fre_set!=[] and non_fre_set==[]:
            fre_pattern_result=add_sets_to_fre_sets(fre_pattern_result,fre_set)
        
        #如果都是不频繁的，则直接加入非频繁集
        elif fre_set==[] and non_fre_set!=[]:
            non_fre_pattern_set=add_sets_to_non_fre_sets(non_fre_pattern_set,non_fre_set)
        
        #如果部分频繁，部分不频繁，则从频繁到不频繁的中间阶数作为子集阶数
        elif fre_set!=[] and non_fre_set!=[]:
            difference_set=list(set(non_fre_set[0]).difference(set(fre_set[0])))
            
            #将频繁模式加入rule_set中
            fre_pattern_result=add_sets_to_fre_sets(fre_pattern_result,fre_set)
            
            #对maximal_set向下递归，获得频繁模式
            fre_set=recursion_down(table_no, maximal_set,difference_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
            
            #将所有的频繁集放入频繁集中
            if fre_set!=[]:
                fre_pattern_result=add_sets_to_fre_sets(fre_pattern_result,fre_set)
        '''
        #*****************************************************************************************
    #print(str(table_no)+":"+str(num))            
    return fre_pattern_result


#**********************************从信息表yes_no中生成规则******************************************
def obtain_patterns_without_pre_patterns(table_no, instance_set_by_attribute,instance_count,predicate_number,information_table,cov,instance_set_by_each_attribute_value):
    #predicate_values_set,list中每个元素都是一个list，每个子list代表拥有第i个属性的所有实例集
    #predicate_values_num,list中每个元素对应一个属性的实例个数
    #instance_count,总的实例个数
    #predicate_number，谓词个数
    #cov，规则的覆盖度
    
    
    fre_pattern_result=[]
    non_fre_pattern_set=[]
    
    

    #删掉不可能形成规则的属性，
    possible_set=[]
    for i in range(0,predicate_number):
        if instance_set_by_each_attribute_value[i]!=[]:
            possible_set.append(i)
        else:
            instance_set_by_attribute[i]=[]

                
    #print("1---"+str(len(possible_set)))
        
    #生成多阶的频繁规则集和非频繁规则集
    n=len(possible_set)+1    
    for i in range(0,n):
        x=[]
        fre_pattern_result.append(x)
        y=[]
        non_fre_pattern_set.append(y)
        

    #将最原始的频繁或非频繁项集加入集合中
    x=is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,possible_set,information_table,cov,instance_count,instance_set_by_each_attribute_value)
    if x!=[]:
        fre_pattern_result[len(possible_set)].append(possible_set)
    else:
        non_fre_pattern_set[len(possible_set)].append(possible_set)
    
    #不频繁集非空
    num=1
    while non_empty(non_fre_pattern_set):
        
    
        #*****************************************************************************************
        other_sub_set=[]
        
       
        
        maximal_set=first_set(non_fre_pattern_set)
        size=len(maximal_set)
        setsx=non_fre_pattern_set[len(maximal_set)]
        x=int(500000/size)
        for setx in setsx[:x]:
            other_sub_set.extend(size_lower_pattern(setx))

        non_fre_pattern_set[len(maximal_set)]=setsx[x:]
        
        #print(str(len(other_sub_set[0]))+"---"+str(len(other_sub_set)))
        
        other_sub_set=delete_repeat(other_sub_set)
        
        #print(str(len(other_sub_set[0]))+"---"+str(len(other_sub_set)))
        
        
        other_sub_set=not_exist_in_fre_or_not_fre_set(fre_pattern_result,other_sub_set)
        #other_sub_set=not_exist_in_fre_or_not_fre_set(non_fre_pattern_set,other_sub_set)

        num=num+len(other_sub_set)
        
        fre_set,non_fre_set=divide_fre_or_not(table_no, other_sub_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        
        #如果都是频繁的，则直接加入频繁集
        if fre_set!=[]:
            fre_pattern_result[len(fre_set[0])].extend(fre_set)
        if non_fre_set!=[]:
            non_fre_pattern_set[len(non_fre_set[0])].extend(non_fre_set)
        

    #print(str(table_no)+":"+str(num))            
    return fre_pattern_result


#**********************************从信息表yes_no中生成规则******************************************
def obtain_patterns_apriori(pre_patterns,table_no, instance_set_by_attribute,instance_count,predicate_number,information_table,cov,instance_set_by_each_attribute_value):
    #predicate_values_set,list中每个元素都是一个list，每个子list代表拥有第i个属性的所有实例集
    #predicate_values_num,list中每个元素对应一个属性的实例个数
    #instance_count,总的实例个数
    #predicate_number，谓词个数
    #cov，规则的覆盖度
    
    #生成可能的规则
    rule_set=[]
    
    
    #删掉不可能形成规则的属性，
    fre_set=[]
    numbers=[]
    rule_set=[]
    rule_set.append([])
    for i in range(0,predicate_number):
        if len(instance_set_by_attribute[i])<cov*instance_count:
            instance_set_by_attribute[i]=[]
        else:
            fre_set.append([i])
            numbers.append(i)
            rule_set.append([])









    #多生成一组，因为下表从0开始不好处理，所以多生成之后可以从1开始    
    #rule_set[1]=fre_set
    delete_rule()
    write_rule(1,fre_set)
    #add_to_fre_rule_set(rule_set,fre_set)
    
    x=is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,numbers,information_table,cov,instance_count,instance_set_by_each_attribute_value)
    if x!=[]:
        rule_set[len(numbers)].append(numbers)
    else:
        mid_down_up(numbers,1,len(fre_set),1,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
    '''
    for i in range(len(numbers),0,-1):
        setsx=read_rule(i)
        #rule_set[i]=[]
        rule_set=add_sets_to_fre_sets(rule_set,setsx)
    '''  
    return rule_set



def verify_pattern_is_subset(sets, pattern):
    temp_sub_set=set(pattern)
    for super_set in sets:
        if temp_sub_set.issubset(super_set):
            return 1
    return 0
           
    
'''
def verify_non_fre_set_which_no_subset_of_fre(old_fre_set,pattern):
    flag=0
    temp_sub_set=set(pattern)
    for super_set in old_fre_set:
        if temp_sub_set.issubset(super_set):
            flag=1
            break
    if flag==0:
        return pattern
    else:
        return []

def verify_non_fre_set_which_subset_of_non_fre(non_fre_set,pattern):
    flag=0
    temp_sub_set=set(pattern)
    for super_set in non_fre_set:
        if temp_sub_set.issubset(super_set):
            flag=1
            break
    if flag==1:
        return pattern
    else:
        return []
'''

def obtain_low_size_pattern(numbers,k,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    fre_set=[]
    comb=combinations(numbers,k)
    for sub_pattern in comb:
        pattern=list(sub_pattern)
        if verify_higher_can_set_which_no_subset_of_fre(rule_set,pattern)==1:
            x=is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,pattern,information_table,cov,instance_count,instance_set_by_each_attribute_value)
            if x!=[]:
                fre_set.append(pattern)
            
    
    return fre_set
    
    
    '''
    lower_set=[]
    for patternx in non_fre_set:
        lower_set.extend(size_lower_pattern(patternx))
    lower_set=delete_repeat(lower_set)
    lower_set=verify_non_fre_set(old_fre_set,lower_set)
    if len(lower_set[0])>k:
        lower_set=obtain_low_size_pattern(old_fre_set,lower_set,k)
    
    return lower_set
    '''
    
def verify_higher_can_set_which_no_subset_of_fre(rule_set,setx):
    k=len(setx)
    for i in range(len(rule_set)-1,k,-1):
        sets=rule_set[i]
        if verify_pattern_is_subset(sets,setx)==1:
            return 0
    return 1
    
    
def combine_can_set_and_return_fre_set(can_set,can_size_lower1,size_higher,lower_fre_set,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    former_set=set(can_set[0][0:can_size_lower1])
    latter=[]
    for setx in can_set:
        latter.extend(setx[-1:])
    
    comb = combinations(latter, size_higher-can_size_lower1)
    
    higher_fre_set=[]
    for latter_one in comb:
        setx=list(former_set.union(set(latter_one)))
        setx.sort()
        if verify_apriori_higher(setx,lower_fre_set) and verify_higher_can_set_which_no_subset_of_fre(rule_set,setx)==1 and is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,setx,information_table,cov,instance_count,instance_set_by_each_attribute_value)!=[]:
            higher_fre_set.append(setx)
    
    return higher_fre_set
    
    
def obtain_high_size_pattern(lower_fre_set,numbers,mid,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    can_size_lower1=len(lower_fre_set[0])-1
    size_higher=mid
    higher_set=[]

    can_set=[]
    setx=lower_fre_set[0]
    can_set.append(setx)
    
    for i in range(1,len(lower_fre_set)):
        if eq(setx[0:can_size_lower1],lower_fre_set[i][0:can_size_lower1]):
            can_set.append(lower_fre_set[i])  
        else:
            if len(can_set)>=size_higher-can_size_lower1:
                higher_set.extend(combine_can_set_and_return_fre_set(can_set,can_size_lower1,size_higher,lower_fre_set,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value))
            can_set=[]
            setx=lower_fre_set[i]
            can_set.append(setx)
    if len(can_set)>=size_higher-can_size_lower1:
        higher_set.extend(combine_can_set_and_return_fre_set(can_set,can_size_lower1,size_higher,lower_fre_set,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value))
    
    return higher_set


def verify_apriori_higher(setx,fre_set):
    k=len(fre_set[0])
    sub_setx=combinations(setx,k)
    for subset in sub_setx:
        if fre_set.count(list(subset))==0:
            return False
   
    return True


'''
def obtain_high_size_pattern(old_fre_set,numbers,mid,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    k=len(old_fre_set[0])
    fre_set=[]
    non_fre_set=[]
    comb = combinations(numbers, mid)
    for super_pattern in comb:
        if k==1 or verify_super_pattern(old_fre_set,super_pattern,k):
            pattern=list(super_pattern)
            x=is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,pattern,information_table,cov,instance_count,instance_set_by_each_attribute_value)
            if x!=[]:
                fre_set.append(pattern)
            else:
                non_fre_set.append(pattern)
            #print(len(new_set))
           
            
    
    return fre_set,non_fre_set
'''
def verify_super_pattern(old_fre_set,super_pattern,k):
    comb=combinations(super_pattern,k)
    for sub_pattern in comb:
        sub_pattern=list(sub_pattern)
        if old_fre_set.count(sub_pattern)==0:
            return 0
    return 1
    
def str_to_pattern(strx):
    strx=strx[1:-1]
    strx=strx.split(',')
    pattern=[]
    for x in strx:
        x=int(x)
        pattern.append(x)
    return pattern

def read_rule(size):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    rule_set=[]
    sql="select pattern from ["+database_name+"].[dbo].size_pattern where size="+str(size)
    cursor.execute(sql)
    result=cursor.fetchall()
    
    for strx in result:
        pattern=str_to_pattern(strx[0])
        rule_set.append(pattern)
    
    #关闭连接
    cnxn.close()
    
    rule_set.sort()
    
    return rule_set
    
def write_rule(size,rule_set):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #保存k阶规则
    for pattern in rule_set:
        sql="insert into ["+database_name+"].[dbo].size_pattern (size, pattern) values("+str(size)+",'"+str(pattern)+"')"
        cursor.execute(sql)
        cnxn.commit()
       
    #关闭连接
    cnxn.close()
    
    
def delete_rule():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #保存k阶规则
    sql="delete from ["+database_name+"].[dbo].size_pattern"
    cursor.execute(sql)
    cnxn.commit()
       
    #关闭连接
    cnxn.close()
    
def get_mid(low,high):
    return int(low+(high-low)/2)

def add_to_fre_rule_set(rule_set,fre_set):
    k=len(fre_set[0])
    '''
    for i in range(len(rule_set)-1,k,-1):
        sets=rule_set[i]
        for setx in fre_set:
            if not_exist(sets,setx)==0:#如果存在，则删除
                fre_set.remove(setx)
    ''' 
    for i in range(1,k):
        sets=rule_set[i]
        new_set=[]
        for setx in sets:
            if verify_pattern_is_subset(fre_set,setx)==0:#如果不存在，则加入新集合
                new_set.append(setx)
                
        rule_set[i]=new_set
    
    rule_set[k]=fre_set
    
    return rule_set
        

def mid_down_up(numbers,low,high,down_up,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value):
    
    mid=get_mid(low,high)
    
    print("--low--"+str(low)+"--high--"+str(high)+"--mid--"+str(mid))
    
    #获得mid阶的模式集
    if down_up==1:#由低阶到高阶
        old_fre_set=read_rule(low)
        fre_set=obtain_high_size_pattern(old_fre_set,numbers,mid,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        #验证mid阶的模式集,即高阶模式的子集是否都存在
        #size_k_patterns_set=retain_pattern_with_sub_or_super_pattern_exist(old_fre_set,[],size_k_patterns_set,0)
    else:
        #old_fre_set=read_rule(high)
        fre_set=obtain_low_size_pattern(numbers,mid,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
        #size_k_patterns_set=retain_pattern_with_sub_or_super_pattern_exist(old_fre_set,non_fre_set,size_k_patterns_set,1)
    
    #划分频繁集和非频繁集
    #fre_set,non_fre_set=divide_fre_or_not(table_no, size_k_patterns_set,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)    
    
    #print("--all--"+str(len(size_k_patterns_set))+"--fre--"+str(len(fre_set))+"--non--"+str(len(non_fre_set)))
    print("--mid--"+str(mid)+"--fre--"+str(len(fre_set)))
    
    #rule_set[mid]=fre_set
    if fre_set!=[]:
        write_rule(mid,fre_set)
        rule_set=add_to_fre_rule_set(rule_set,fre_set)
    
    if fre_set!=[] and high-mid>1:
        mid_down_up(numbers,mid,high,1,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
    if mid-low>1:
        mid_down_up(numbers,low,mid,0,rule_set,table_no,instance_set_by_attribute,information_table,cov,instance_count,instance_set_by_each_attribute_value)
       




'''
#**********************************从信息表yes_no中生成规则******************************************
def obtain_patterns_apriori(pre_patterns,table_no, instance_set_by_attribute,instance_count,predicate_number,information_table,cov,instance_set_by_each_attribute_value):
    #predicate_values_set,list中每个元素都是一个list，每个子list代表拥有第i个属性的所有实例集
    #predicate_values_num,list中每个元素对应一个属性的实例个数
    #instance_count,总的实例个数
    #predicate_number，谓词个数
    #cov，规则的覆盖度
    
    #生成可能的规则
    rule_set=[]
    
    #删掉不可能形成规则的属性，
    fre_set=[]
    for i in range(0,predicate_number):
        if len(instance_set_by_attribute[i])<cov*instance_count:
            instance_set_by_attribute[i]=[]
        else:
            fre_set.append([i])


    #多生成一组，因为下表从0开始不好处理，所以多生成之后可以从1开始
    rule_set.append([])
    fre_set.sort()
    rule_set.append(fre_set)
    
    global pattern_number
    
    pattern_number=len(rule_set[1])
    print("1---"+str(pattern_number))
    
    pre_patterns=read_table_yes_no_result(1)
    apriori(rule_set,1,pre_patterns)
    
 
    return rule_set


def apriori(rule_set,lower_k,pre_patterns):
    global pattern_number
    pattern_number=0
    lower_fre_set=rule_set[lower_k]
    higher_fre_set=[]
    for i in range(0,len(lower_fre_set)-1):
        setx=lower_fre_set[i]
        for j in range(i+1,len(lower_fre_set)):
            sety=lower_fre_set[j]
            if eq(setx[0:-1],sety[0:-1]):
                sety=list((set(setx)).union(set(sety)))
                sety.sort()
                if verify(sety,lower_fre_set,pre_patterns):
                    higher_fre_set.append(sety)
            else:
                break
    print(str(lower_k+1)+"---"+str(pattern_number))
    if higher_fre_set!=[]:
        higher_fre_set.sort()
        rule_set.append(higher_fre_set)
        #print(lower_k)
        apriori(rule_set,lower_k+1,pre_patterns)


def verify(setx,fre_set,pre_patterns):
    global pattern_number
    sub_setx=size_lower_pattern(setx)
    for subset in sub_setx:
        if fre_set.count(subset)==0:
            return False
    if is_frequent(setx,pre_patterns)==1:
        return True
    else:
        return False
    
    
def is_frequent(possible_set,pre_patterns):
    global pattern_number
    pattern_number=pattern_number+1
    if not_exist(pre_patterns,possible_set):
        return 0
    else:
        return 1
    
    '''
#**********************************从信息表yes_no中生成规则******************************************
def produce_patterns_from_table(pre_patterns, table_no, cov,x):
    #attribute_values_set,list中每个元素都是一个list，每个子list代表拥有第i个属性的所有实例集
    #attribute_values_num,list中每个元素对应一个属性的实例个数
    #instance_count,总的实例个数
    #predicate_number，谓词个数
    #cov，规则的覆盖度
    
    #获得整个信息表（排除实例编号）
    location_graph,information_table=obtain_transaction_data(table_no)
    
    #获得实例个数和谓词个数
    instance_count=len(information_table)
    predicate_number=len(information_table[0])
    
    #从信息表转换为实例集，其中实例集以单个属性为划分，即每个属性分别有哪些实例
    instance_set_by_each_attribute=obtain_instance_set_by_each_attribute(table_no,information_table,instance_count,predicate_number,cov)#从信息表yes_no中生成谓词的值（实例）集
    
    if table_no!=1:
        #从信息表中获得属性值和实例之间的划分，即每个属性值对应哪些实例
        instance_set_by_each_attribute_value=obtain_instance_set_by_attribute_value(instance_count,predicate_number,information_table,cov)
    else:
        instance_set_by_each_attribute_value=[]
        
    #依靠实例集生成规则
    if x==0:
        patterns=obtain_patterns(pre_patterns,table_no, instance_set_by_each_attribute,instance_count,predicate_number,information_table,cov,instance_set_by_each_attribute_value)
    if x==1:
        patterns=obtain_patterns_apriori(pre_patterns,table_no, instance_set_by_each_attribute,instance_count,predicate_number,information_table,cov,instance_set_by_each_attribute_value)
    if x==2:
        patterns=obtain_patterns_without_pre_patterns(table_no, instance_set_by_each_attribute,instance_count,predicate_number,information_table,cov,instance_set_by_each_attribute_value)
    
    
    return patterns
    



#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#*********************************以上是信息表生成频繁模式，以下是反例数据生成频繁模式
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************
#********************************************************************************************************************************************************************************************************************

'''
#**********************************从反例数据中获得yes_no模式的共同实例**********************************
def obtain_common_subject_of_negetive_data_table_1(pattern,min_num,max_num):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    time_start = time.time()
    
    sql="select [subject] from ["+database_name+"].[dbo].[negetive_instance_"+str(pattern[0]+1)+"]"
    
    for i in range(1,len(pattern)):
        sql=sql+" intersect select [subject] from ["+database_name+"].[dbo].[negetive_instance_"+str(pattern[i]+1)+"]"
    
    cursor.execute(sql)
    result=cursor.fetchall()
    common_instance=len(result)
    
     

    time_end = time.time()
    print('--- %fs' % (int(time_end - time_start)))
    
    if common_instance<min_num:
            #关闭连接
        cnxn.close()
        return 1
    
    #关闭连接
    cnxn.close()
    
    #如果循环过程中未返回，则说明交集实例个数满足阈值，所以最后返回1
    if common_instance>max_num:
        return -1#不可信，且不用验证子集
    else:
        return 0#不可信，还需要验证子集


#**********************************从反例数据中获得yes_no模式的共同实例**********************************
def obtain_common_subject_of_negetive_data_table_1(pattern,min_num,max_num):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
        
    sql="select count(t"+str(pattern[0]+1)+".[subject]) from ["+database_name+"].[dbo].[negetive_instance_"+str(pattern[0]+1)+"] as t"+str(pattern[0]+1)
    str_on=" where "
    for i in range(1,len(pattern)):
        str_table_as=", ["+database_name+"].[dbo].[negetive_instance_"+str(pattern[i]+1)+"] as t"+str(pattern[i]+1)
        sql=sql+str_table_as
        str_on=str_on+"t"+str(pattern[i-1]+1)+".[subject]=t"+str(pattern[i]+1)+".[subject] and "
        
    sql=sql+str_on[:-5]
    
    cursor.execute(sql)
    result=cursor.fetchone()
    common_instance=result[0]    
    
    if common_instance<min_num:
        #关闭连接
        cnxn.close()
        return 1
    
    #关闭连接
    cnxn.close()
    
    #如果循环过程中未返回，则说明交集实例个数满足阈值，所以最后返回1
    if common_instance>max_num:
        return -1#不可信，且不用验证子集
    else:
        return 0#不可信，还需要验证子集
        
'''        




    
    
#**********************************从特征集中获得属性值id和对应的属性值或类型**********************************
def obtain_attribute_value_and_its_id(characters,table_no):
    #获得所有的值，并存放于attribute_values
    attribute_value_ids=[]
    for x in characters:
        for y in x:
            for value in y:
                attribute_value_ids.append(int(value))
    attribute_value_ids=list(set(attribute_value_ids))
    
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    
    #获得特征集中id对应的object_type或object
    special_types=[]
    attribute_values=[]
    for idx in attribute_value_ids:
        #利用id获得object或object_type
        if table_no==3:
            sql="select object_type from ["+database_name+"].[dbo].[object_type_id] where id="+str(idx)
        if table_no==4:
            sql="select object from ["+database_name+"].[dbo].[object_id] where id="+str(idx)
        cursor.execute(sql)
        result=cursor.fetchone()
        attribute_values.append(result[0])
        
        #如果是表3，需要判断是否是特殊类型
        if table_no==3:
            if result[0]=="string":
                special_types.append("string")
            else:
                sql="select top 1 [object] from ["+database_name+"].[dbo].[object_types] where [object] like '%"+str(result[0])+"%'"
                cursor.execute(sql)
                objectx=cursor.fetchone()
                if objectx!=None:
                    special_types.append(result[0])
        
    #关闭连接
    cnxn.close()
    return attribute_value_ids,attribute_values,special_types


def obtain_subjects_of_types_or_values_from_negative_data_by_ids(table_no,type_or_value,database_name,pattern,pattern_no,special_types,cursor):
    #如果是特殊类型，如string、float等
    if table_no==3:
        #string类型，找到不包含>的即可
        if type_or_value=="string":
            sql="select distinct [subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[pattern_no]+1)+"] where [object] not like '%>'"
                #属于特殊类型，找到object包含类型的即可
        elif special_types.count(type_or_value)!=0:
            sql="select distinct [subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[pattern_no]+1)+"] where [object] like '%<"+str(type_or_value)+">'"
                #属于普通的类型，需要在KGC.dbo.all_instance_types中找到对应的类型，然后返回subject
        else:
            sql="select distinct table_predicate.[subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[pattern_no]+1)+"] as table_predicate, ["+database_name+"].[dbo].[negetive_object_type_"+str(pattern[pattern_no]+1)+"] as table_types where table_predicate.[object]=table_types.[subject] and table_types.[object]='"+str(type_or_value)+"'"
            #对于value表，直接返回值相等的subject
    if table_no==4:
        sql="select distinct [subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[pattern_no]+1)+"] where [object]='"+str(type_or_value)+"'"
            
    cursor.execute(sql)
    result=cursor.fetchall()
    
    #获得当前的实例集
    instancesx=set()
    for raw in result:
        instancesx.add(raw[0])
        
    return instancesx


def obtain_subject_from_common_character(characters,table_no,pattern,attribute_value_ids,attribute_values,special_types,min_num):
    #获得特征集的公共部分，按列处理
    non_common_character_no=[]
    common_character=[]
    for j in range(0,len(pattern)):
        flag=0
        #如果每一个特征集对应的属性值相同，则flag=0
        characters[0][j].sort()
        for i in range(1,len(characters)):
            characters[i][j].sort()
            if characters[i][j]!=characters[i-1][j]:
                flag=1
                break
        #如果属性值相同，则插入属性值，否则插入空集,并将j保存到non_common_character_no
        if flag==0:
            common_character.append(characters[i-1][j])
        else:
            common_character.append([])
            non_common_character_no.append(j)
    
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    pattern_no=-1
    for sets in common_character:
        pattern_no=pattern_no+1
        for idx in sets:
            #获得idx对应的type或者value
            type_or_value=attribute_values[attribute_value_ids.index(int(idx))]
            
            #将每个实例依次放入common_instance集合中
            if pattern_no==0:
                common_instance=obtain_subjects_of_types_or_values_from_negative_data_by_ids(table_no,type_or_value,database_name,pattern,pattern_no,special_types,cursor)
            else:
                #获得当前的实例集
                instancesx=obtain_subjects_of_types_or_values_from_negative_data_by_ids(table_no,type_or_value,database_name,pattern,pattern_no,special_types,cursor)
                
                #将当前获得实例集与之前的结果求交集
                common_instance=common_instance.intersection(instancesx)
                if len(common_instance)<min_num:
                    #关闭连接
                    cnxn.close()
                    return 1,[],[]

                
    #关闭连接
    cnxn.close()
    return 0,common_instance,non_common_character_no
    

def obtain_subject_from_non_common_character(characters,table_no,pattern,attribute_value_ids,attribute_values,special_types,common_instances,non_common_character_no,min_num,max_num):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    flag=0
    new_characters=[]
    new_common_instance=[]
    for sets in characters:
        temp_common_instance=common_instances
        pattern_no=-1
        for no in non_common_character_no:
            setx=sets[no]
            pattern_no=no
            for idx in setx:
                #获得idx对应的type或者value
                type_or_value=attribute_values[attribute_value_ids.index(int(idx))]
            
                #获得当前的实例集
                instancesx=obtain_subjects_of_types_or_values_from_negative_data_by_ids(table_no,type_or_value,database_name,pattern,pattern_no,special_types,cursor)
                
                #将当前获得实例集与之前的结果求交集
                temp_common_instance=temp_common_instance.intersection(instancesx)
        
        #如果交集的个数小于阈值，则直接返回0，不再继续求交集
        if len(temp_common_instance)<min_num:
            new_characters.append(sets)
        else:
            flag=1
            new_common_instance.append(temp_common_instance)
        
    #关闭连接
    cnxn.close()
    
    verify_subset=0
    for x in new_common_instance:
        if len(x)<max_num:
            verify_subset=1
    
    #如果新的特征集为空，则说明所有的公共实例个数大于min_num,则是不可信规则
    if new_characters==[]:
        if verify_subset==1:
            return 0,[]#子集需要验证
        else:
            return -1,[]#子集不需要验证
    
    #如果flag==1，说明特征集减少了，则返回新的特征集
    elif flag==1:
        return 1,new_characters
    else:
        return 1,[]
    


#**********************************从反例数据中获得type模式或values模式的共同实例**********************************
def obtain_common_subject_of_negetive_data_table_34(characters,table_no,pattern,min_num,max_num):
    
    #获得特征集中所有的id和数据库中对应的值（object_type或object）,以及手工获得的特殊类型，如string，http://www.w3.org/2001/XMLSchema#float（来源于"_118.11976"^^<http://www.w3.org/2001/XMLSchema#float>）
    attribute_value_ids,attribute_values,special_types=obtain_attribute_value_and_its_id(characters,table_no)
    
    flag,common_instances,non_common_character_no=obtain_subject_from_common_character(characters,table_no,pattern,attribute_value_ids,attribute_values,special_types,min_num)
    if flag==1:
        return 1,[]
    
    return obtain_subject_from_non_common_character(characters,table_no,pattern,attribute_value_ids,attribute_values,special_types,common_instances,non_common_character_no,min_num,max_num)
    
    

#**********************************从正例信息表num中获得模式的属性特征，即模式中每个属性值个数应该大于等于多少**********************************
def obtain_common_characters_from_information_table_num(pattern,common_instances,information_table):
    #初始化特征集
    character=[]
    #按列（属性编号）处理
    for j in pattern:
        column=j
        row=common_instances[0]-1
        #每一列的第一行的值作为初始最小值
        min_value=int(information_table[row][column])
        #如果9初始最小值为1，则无需继续对比，直接放入特征集
        if min_value!=1:
            #如果初始最小值不为1，则一行一行地对比，找到最小值
            for i in common_instances[1:]:
                row=i-1
                value=int(information_table[row][column])
                if min_value>value:
                    min_value=value
                    #如果找到的当前最小值为1，则停止继续比较
                    if min_value==1:
                        break   
        #将每一列的最小值保存到特征集
        character.append(min_value)

    return character
    
    
#**********************************从正例信息表type或value中获得模式的属性特征，即模式中每个属性值包含哪些类型或属性值**********************************
def obtain_common_characters_from_information_table_type_or_value(pattern,common_instances,information_table):
    #common_instance包含多组实例集，每组实例集的值不同
    #先求所有实例集的交集
    intersection_set=set(common_instances[0])
    for i in range(1,len(common_instances)):
        intersection_set=intersection_set.intersection(set(common_instances[i]))
    intersection_set=list(intersection_set)
    
    #对characters初始化
    common_characters=[]
    for i in range(0,len(pattern)):
        common_characters.append([])
    
    #先对交集求character，且按列处理
    for j in pattern:
        column=j
        row=intersection_set[0]-1
        character_x=set(information_table[row][column].split(', '))
        for i in intersection_set[1:]:
            row=i-1
            character_x=character_x.intersection(set(information_table[row][column].split(', ')))
        #将每一列的特征值保存到特征集
        common_characters[pattern.index(j)]=list(character_x)
        
    #对差集分别求character，并构建新的list
    result_characters=[]
    for instances in common_instances:
        #每一个差集对应一个特征序列
        common_and_different_characters=copy.copy(common_characters)
        difference_set=set(instances).difference(set(intersection_set))
        #用公共特征集与差集中的每个实例求交集，得到差集对应的特征序列
        for i in difference_set:
            row=i-1
            for j in pattern:
                column=j
                common_and_different_characters[pattern.index(j)]=list(set(common_and_different_characters[pattern.index(j)]).intersection(set(information_table[row][column].split(', '))))
        
        #获得一个差集对应的特征序列后，保存到最终的结果特征集中
        result_characters.append(common_and_different_characters)

    return result_characters



    
#**********************************依次从正例数据中获得公共实例集和公共特征集**********************************
def obtain_common_instances_and_characters_from_positive_data(table_no, pattern, cov):
    #获得information_table
    location_graph,information_table=obtain_transaction_data(table_no)
    
    #获得实例个数和谓词个数
    instance_count=len(information_table)
    predicate_number=len(information_table[0])
    
    #从信息表转换为实例集，其中实例集以单个属性为划分，即每个属性分别有哪些实例
    instance_set_by_attribute=obtain_instance_set_by_each_attribute(table_no,information_table,instance_count,predicate_number,cov)
        
    #对于信息表3和4，需要生成每个属性值对应的实例集，信息表1和2不需要
    if table_no==1 or table_no==2:
        instance_set_by_each_attribute_value=[]
    else:
        instance_set_by_each_attribute_value=obtain_instance_set_by_attribute_value(instance_count,predicate_number,information_table,cov)
        
    #利用正例模式生成过程中的函数，生成正例模式的公共实例
    common_instances=is_frequent_and_return_instance_set(table_no, instance_set_by_attribute,pattern,information_table,cov,instance_count,instance_set_by_each_attribute_value)
    
    #从正例数据中生成模式的公共特征
    if table_no==1:
        common_characters=[]
    elif table_no==2:
        common_characters=obtain_common_characters_from_information_table_num(pattern,common_instances,information_table)
    else:
        common_characters=obtain_common_characters_from_information_table_type_or_value(pattern,common_instances,information_table)
    
    #返回正例数据中模式的公共实例和公共特征集
    return common_instances,common_characters
  

def add_pattern_and_characters_to_fre_result(fre_set,characters_set,pattern,common_characters_in_positive):
    size=len(pattern)
    fre_set[size].append(pattern)
    characters_set[size].append(common_characters_in_positive)
    return fre_set,characters_set


def obtain_instance_count_in_positive():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #通过谓词划分反例三元组
    sql="select count(id) from ["+database_name+"].[dbo].[instance]"
    cursor.execute(sql)
    result=cursor.fetchone()
    
    result=int(result[0])

    #关闭连接
    cnxn.close()
    return result


def read_predicate_no_set():
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #通过谓词划分反例三元组
    sql="SELECT [id] FROM ["+database_name+"].[dbo].[predicate] order by id"
    cursor.execute(sql)
    result=cursor.fetchall()
    
    predicate_no_set=[]
    
    for i in range(0,len(result)):
        predicate_no_set.append(int(result[i][0]))

    #关闭连接
    cnxn.close()
    return predicate_no_set


def obtain_all_pre_patterns_and_predicate_nos(pre_patterns):
    all_size_can_rule_set=copy.deepcopy(pre_patterns)
    
    #获得模式的最大阶数
    max_k=len(first_set(pre_patterns))
    
    #删掉最大阶数之后的空集
    all_size_can_rule_set=all_size_can_rule_set[0:max_k+1]
    
    #求子模式
    for i in range(max_k,0,-1):
        #一次对一个阶数的所有模式求子模式
        size_k_patterns=all_size_can_rule_set[i]
        size_lower_pattern_sets=[]
        for pattern in size_k_patterns:
            size_lower_pattern_sets.extend(size_lower_pattern(pattern))
        all_size_can_rule_set[i-1].extend(size_lower_pattern_sets)
        
        #去除子模式中的重复
        pattern_set=all_size_can_rule_set[i-1]
        all_size_can_rule_set[i-1]=[]
        pattern_set.sort()
        for pattern in pattern_set:
            if pattern not in all_size_can_rule_set[i-1]:
                all_size_can_rule_set[i-1].append(pattern)
    '''
    pattern_predicate_nos=[]
    pattern_set=all_size_can_rule_set[1]
    for pattern in pattern_set:
        pattern_predicate_nos.extend(pattern)
    
    return all_size_can_rule_set,pattern_predicate_nos
    '''
    pattern_num=0
    for i in range(0,len(all_size_can_rule_set)):
        pattern_num=pattern_num+len(all_size_can_rule_set[i])
    
    return all_size_can_rule_set,pattern_num



def obtain_all_negative_instances_table_1(pattern_predicate_nos):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    all_negative_instances=[]
    #从第二个实例开始，依次获得实例集，并与第一个实例集求交集
    for no in pattern_predicate_nos:
        sql="select [subject] from ["+database_name+"].[dbo].[negetive_instance_"+str(no)+"]"
        cursor.execute(sql)
        result=cursor.fetchall()
        instancesx=set()
        for raw in result:
            instancesx.add(raw[0])
        all_negative_instances.append(instancesx)
    
    #关闭连接
    cnxn.close()
    return all_negative_instances


#**********************************从反例数据中获得num模式的共同实例**********************************
def obtain_common_subject_of_negetive_data_table_2(characters,table_no,pattern,min_num,max_num):
    #数据库连接
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-U7HDLOK;DATABASE=master;UID=sa;PWD=123456')
    cursor = cnxn.cursor()
    
    #如果属性值个数为1，则直接返回所有的实例，如果不为1，则需要返回属性值个数大于等于给定值的实例
    if characters[0]==1:
        sql="select [subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[0]+1)+"]"
    else:
        sql="select [subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[0]+1)+"] group by subject having count(subject)>="+str(characters[0])
    cursor.execute(sql)
    result=cursor.fetchall()
    #将所有实例放入common_instance作为初始集
    common_instance=set()
    for raw in result:
        common_instance.add(raw[0])
    
    #从第二个属性集开始，依次返回瞒住属性值个数要求的实例，并与之前的初始集求交集
    for i in range(1,len(pattern)):
        if characters[0]==1:
            sql="select [subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[i]+1)+"]"
        else:
            sql="select [subject] from ["+database_name+"].[dbo].[negetive_predicate_"+str(pattern[i]+1)+"] group by subject having count(subject)>="+str(characters[i])
        cursor.execute(sql)
        result=cursor.fetchall()
        #获得当前的实例集
        instancesx=set()
        for raw in result:
            instancesx.add(raw[0])
        
        #将当前获得实例集与之前的结果求交集
        common_instance=common_instance.intersection(instancesx)
        #如果交集的个数小于阈值，则直接返回0，不再继续求交集
        if len(common_instance)<min_num:
            #关闭连接
            cnxn.close()
            return 1
    
    #关闭连接
    cnxn.close()
    
    #如果循环过程中未返回，则说明交集实例个数满足阈值，所以最后返回1
    if len(common_instance)>max_num:
        return -1#不可信，且不用验证子集
    else:
        return 0#不可信，还需要验证子集




def obtain_common_negative_instance(pattern,all_negative_instances):
    common_instance=all_negative_instances[pattern[0]]
    for i in range(1,len(pattern)):
        common_instance=common_instance.intersection(all_negative_instances[pattern[i]])
    return len(common_instance)

def delete_all_sub_pattern(all_pre_patterns,pattern):
    supper_set=set(pattern)
    delete_num=0
    for i in range(0,len(pattern)):
        size_k_pattern=all_pre_patterns[i]
        new_set=[]
        for patternx in size_k_pattern:
            if supper_set.issuperset(set(patternx))==False:
                new_set.append(patternx)
        delete_num=delete_num+len(size_k_pattern)-len(new_set)
        all_pre_patterns[i]=new_set

    return all_pre_patterns,delete_num
    
    

#**********************************从反例数据中获得yes_no模式的共同实例**********************************
def obtain_credible_decision_rule_for_table_1(all_pre_patterns,predicate_no_set,cov,ac,instance_total_in_positive):
    
    #构建多阶频繁空集，用于存放频繁模式
    fre_set=[]
    characters_set=[]
    for i in range(0,len(first_set(all_pre_patterns))+1):
        fre_set.append([])
        characters_set.append([])
    
    #从数据库中获得所有反例的instances
    #对于不同的信息表，分别采用不同的方式进行处理
    all_negative_instances=obtain_all_negative_instances_table_1(predicate_no_set)
    
    '''
    max_num=(1-ac)*instance_total_in_positive/ac
    
    all_delete_num=0
    for i in range(len(all_pre_patterns)-1,0,-1):
        if all_pre_patterns[i]!=[]:
            size_k_patterns=all_pre_patterns[i]
            for pattern in size_k_patterns:
                #通过求交集得到公共的反例实例
                common_negative_instance_num=obtain_common_negative_instance(pattern,all_negative_instances)
                if common_negative_instance_num>max_num:
                    all_pre_patterns,delete_num=delete_all_sub_pattern(all_pre_patterns,pattern)
                    all_delete_num=all_delete_num+delete_num
                else:
                    #生成模式的正例实例和正例特征集
                    common_instances_in_positive_data,common_characters_in_positive=obtain_common_instances_and_characters_from_positive_data(1, pattern, cov)
                    min_num=len(common_instances_in_positive_data)*(1-ac)/ac
                    if common_negative_instance_num<min_num:
                        fre_set,characters_set=add_pattern_and_characters_to_fre_result(fre_set,characters_set,pattern,common_characters_in_positive)
    
    
    #max_num=(1-ac)*instance_total_in_positive/ac
    
    '''
    all_delete_num=0
    for i in range(len(all_pre_patterns)-1,0,-1):
        if all_pre_patterns[i]!=[]:
            size_k_patterns=all_pre_patterns[i]
            for pattern in size_k_patterns:
                #通过求交集得到公共的反例实例
                common_negative_instance_num=obtain_common_negative_instance(pattern,all_negative_instances)
                #if common_negative_instance_num>max_num:
                #    all_pre_patterns=delete_all_sub_pattern(all_pre_patterns,pattern)
                #else:
                    #生成模式的正例实例和正例特征集
                common_instances_in_positive_data,common_characters_in_positive=obtain_common_instances_and_characters_from_positive_data(1, pattern, cov)
                min_num=len(common_instances_in_positive_data)*(1-ac)/ac
                if common_negative_instance_num<min_num:
                    fre_set,characters_set=add_pattern_and_characters_to_fre_result(fre_set,characters_set,pattern,common_characters_in_positive)
    
    return fre_set,characters_set,all_delete_num
   
    

#**********************************用反例数据验证正例模式，如果模式是频繁的，继续下行，如果不频繁，则停止**********************************
def produce_pattern_from_negetive_information_table(cov,ac):
    
    instance_total_in_positive=obtain_instance_count_in_positive()
    
    for table_no in range(1,2):
        
        #读取正面数据所获得的频繁模式
        pre_patterns=read_table_yes_no_result(table_no,cov)
        
        #读取谓词的id号
        predicate_no_set=read_predicate_no_set()
        
        #将pre_pattern分解为所有阶数的模式，并获得属性编号集
        all_pre_patterns,all_pattern_num=obtain_all_pre_patterns_and_predicate_nos(pre_patterns)
        
        #对于不同的信息表，分别采用不同的方式进行处理
        if table_no==1:
            fre_set,characters_set,delete_pattern_num=obtain_credible_decision_rule_for_table_1(all_pre_patterns,predicate_no_set,cov,ac,instance_total_in_positive)
            #print("all:"+str(all_pattern_num)+"delete:"+str(delete_pattern_num))
        else:
            fre_set,characters_set=obtain_credible_decision_rule_for_table_234(table_no,pre_patterns,instance_total_in_positive,predicate_no_set,cov,ac)
            
        save_patterns_and_characters(fre_set,characters_set,table_no,cov,ac)


#**********************************依次用反例数据验证模式是否是可信的**********************************
def verifying_pattern_by_negative_data(characters,table_no,ac, pattern,predicate_no_set,instance_count_in_positive_data,instance_total_in_positive):
    
    #将0到20的属性编号转换为数据库中的1-200的属性编号
    new_pattern=[]
    for i in range(0,len(pattern)):
        new_pattern.append(predicate_no_set[pattern[i]]-1)
    
    #求得一个反例中最小的实例个数阈值min_conf
    min_num=(1-ac)*instance_count_in_positive_data/ac
    max_num=(1-ac)*instance_total_in_positive/ac
    
    #对于不同的信息表，分别采用不同的方式进行处理
    if table_no==2:
        return obtain_common_subject_of_negetive_data_table_2(characters,table_no,new_pattern,min_num,max_num),[]
    if table_no==3 or table_no==4:
        return obtain_common_subject_of_negetive_data_table_34(characters,table_no,new_pattern,min_num,max_num)


#**********************************用反例数据验证正例模式，如果模式是频繁的，继续下行，如果不频繁，则停止**********************************
def obtain_credible_decision_rule_for_table_234(table_no,pre_patterns,instance_total_in_positive,predicate_no_set,cov,ac):        
    #构建多阶频繁空集，用于存放频繁模式
    fre_set=[]
    characters_set=[]
    for i in range(0,len(first_set(pre_patterns))+1):
        fre_set.append([])
        characters_set.append([])
            
    #只要非空，就继续执行，
    #高阶不频繁，则无需再验证低阶**********************************************************************
    while non_empty(pre_patterns):
        #获得第一个模式
        pattern=first_set(pre_patterns)
        pre_patterns[len(pattern)].remove(pattern)
            
        if len(pattern)==1:
            break
            
        #生成模式的正例实例和正例特征集
        common_instances_in_positive,common_characters_in_positive=obtain_common_instances_and_characters_from_positive_data(table_no, pattern, cov)
            
        #用反例验证模式是否频繁，
        fre_or_not,new_character_sets=verifying_pattern_by_negative_data(common_characters_in_positive,table_no,ac, pattern,predicate_no_set,len(common_instances_in_positive),instance_total_in_positive)
        #fre_or_not，如果等于-1，不可信，且无需向下验证，
        #fre_or_not，如果等于0，不可信，需要向下验证，
        #fre_or_not，如果等于1，可信且需要向下验证
            
        #如果高阶的不频繁，则无需向下验证
        if fre_or_not==-1:
            continue
            
        if fre_or_not==1:
        #可信的
            if new_character_sets==[]:
                #将频繁模式和值集加入频繁结果集
                fre_set,characters_set=add_pattern_and_characters_to_fre_result(fre_set,characters_set,pattern,common_characters_in_positive)
            else:
                #将频繁模式和值集加入频繁结果集
                fre_set,characters_set=add_pattern_and_characters_to_fre_result(fre_set,characters_set,pattern,new_character_sets)
            
        #获得低一阶的所有模式
        size_lower_pattern_sets=size_lower_pattern(pattern)
        
        #将低一阶的模式加入频繁模式验证序列中
        pre_patterns=add_sets_to_fre_sets(pre_patterns,size_lower_pattern_sets)
            
    return fre_set,characters_set


def save_patterns_and_characters(patterns,characters,table_no,cov,ac):
    filename=str(database_name)+"_rules_table"+str(table_no)+"_cov"+str(cov)+"_ac"+str(ac)+".txt"
    f=open(filename,'w')
    f.write(str(patterns))

    filename=str(database_name)+"_characters_table"+str(table_no)+"_cov"+str(cov)+"_ac"+str(ac)+".txt"
    f=open(filename,'w')
    f.write(str(characters))
    f.close()


#*********************************将文件中读取的字符串转换为list******************************
def str_result_to_list_result(str_x):
    #[[], [], [], [], [[2, 3, 4], [4, 5, 6]], [], [[1, 2, 4, 5]]]
    result=[]
    #删除最外层括号[], [], [], [], [[2, 3, 4], [4, 5, 6]], [], [[1, 2, 4, 5]]
    str_x=str_x[1:-2]#-1是\n,-2是最右边的括号
    #分裂为多个模式
    str_x=str_x.split("], [")
    for strx in str_x:
        temp=[]
        result.append([])
        if strx.find(',')!=-1:
            #[[2, 3, 4]  or  [4, 5, 6]]  or  [[1, 2, 4, 5]]
            strx=strx.replace("[","")
            strx=strx.replace("]","")
            strx=strx.split(", ")#strx分裂之后为纯数字
            
            for x in strx:
                temp.append(int(x))
        if temp!=[]:
            result[len(temp)].append(temp)
    return result


#*********************************读取文件中的字符串并转换为list返回******************************
def read_table_yes_no_result(table_no,cov):
    f = open(str(database_name)+"_patterns_from_table"+str(table_no)+"_cov_"+str(cov)+".txt", "r")
    str_patterns = f.readline()
    f.close()
    
    patterns=str_result_to_list_result(str_patterns)
    
    return patterns

    
def save_patterns(patterns,table_no,cov):
    filename=str(database_name)+"_patterns_from_table"+str(table_no)+"_cov_"+str(cov)+".txt"
    f=open(filename,'w')
    f.write(str(patterns))
    f.close()



#**********************************生成频繁模式和规则******************************************
def produce_pattern_from_information_table(cov):
    for table_no in range(1,2):
        #print("*********cov***********")
        #print(cov)
        time_start = time.time()
        #对于yes_no表，初始模式为空，然后生成频繁模式
        patterns=[]
        if table_no==1:
            
            #print("****** 0 ******")
            #patterns=produce_patterns_from_table(patterns,table_no, cov,0)
            #save_patterns(patterns,table_no)
            #print("****** 1 ******")
            patterns=produce_patterns_from_table(patterns,table_no, cov,0)
            save_patterns(patterns,table_no,cov)
        '''
        #对于number表，直接读取yes_no表的模式结果即可
        if table_no==2:
            patterns=read_table_yes_no_result(1,cov)
        '''
        #对于type表，读取yes_no表的模式结果，
        if table_no==3:
            patterns=read_table_yes_no_result(1,cov)
            patterns=produce_patterns_from_table(patterns,table_no, cov,0)
            save_patterns(patterns,table_no,cov)
        if table_no==4:
            patterns=read_table_yes_no_result(3,cov)
            patterns=produce_patterns_from_table(patterns,table_no, cov,0)
            save_patterns(patterns,table_no,cov)
        
        time_end = time.time()
        if table_no==1:
            print(str(database_name)+"---cov:"+str(cov)+'--- %fs' % (int(time_end - time_start)))


#pattern_number=0
#cov=0.9
cov=0.8

#obtain_positive_data()#获取实例、三元组、谓词、谓词类型数据
#create_and_insert_information_table()#创建信息表
#obtian_negetive_data()#通过谓词获得反例数据
for ac in range(10,100,10):
    ac=ac*1.0/100
    for i in range(1,5):
        if i==1: 
            database_name='Reduce_River_rule'
        elif i==2:
            database_name='Reduce_Cave_rule'
        elif i==3:
            database_name='Reduce_Sport_rule'
        else:
            database_name='Reduce_Garden_rule'
        #print(database_name)
        time_start = time.time()
        #produce_pattern_from_information_table(cov)#生成频繁模式和规则

        produce_pattern_from_negetive_information_table(cov,ac)
        time_end = time.time()
        print(str(database_name)+"-cov:"+str(cov)+"-ac:"+str(ac)+'--- %fs' % (int(time_end - time_start)))