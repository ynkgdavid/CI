import os
import time
import pymysql
import json
import time

key_list = []

def get_dict_allkeys(dict_a):

        if isinstance(dict_a, dict):
            for x in range(len(dict_a)):
                temp_key = list(dict_a.keys())[x]
                temp_value = dict_a[temp_key]
                if temp_key == "alarm name":
                    key_list.append(temp_value)
                # key_list.append(temp_key)
                get_dict_allkeys(temp_value)
        elif isinstance(dict_a, list):
            for k in dict_a:
                if isinstance(k, dict):
                    for x in range(len(k)):
                        temp_key = list(k.keys())[x]
                        temp_value = k[temp_key]
                        if temp_key == "alarm name":
                            key_list.append(temp_value)
                        # key_list.append(temp_key)
                        get_dict_allkeys(temp_value)
        return key_list

connection = pymysql.connect(host='10.127.96.46',
                             user='monitor',
                             password='redhat001',
                             db='monitor',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

try:

  upsql1="update prodalarm set reset=0,target=0,alarmTime=SYSDATE()"
  insql = "insert into prodalarm (alarmName,alarmTime,target,reset)  values(%s,SYSDATE(),1,1) on  DUPLICATE key update target=1,reset=1,alarmTime=SYSDATE()"
  upsql2="update prodalarm set reset=0,target=0,alarmTime=SYSDATE() where alarmName not in "

  #loop every 5 minutes
  while 1:
    #reset key_list
    key_list=[]
    #get staging alarm list
    p=os.popen("sshpass -p 'xAhmncuufCR8WlCZCMqLja7D' ssh mapr@10.127.33.139 maprcli alarm list -json")
    x=p.read()
    
    
    #if had alarm list
    if len(x)!=0:
       #load json format
       alarmjson = json.loads(x)
       #get alarm name
       get_dict_allkeys(alarmjson)
       #print(key_list)

       for i in range(len(key_list)):
          #if had the name,update the record.
          with connection.cursor() as cursor:
            print("["+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"] "+insql+key_list[i])
            cursor.execute(insql,key_list[i])
            connection.commit()
       
       #update other record unincluded these names
       alarmNameString = str(key_list).replace("[","(").replace("]",")").replace("u","")
       with connection.cursor() as cursor:
         upsql2 = upsql2 + alarmNameString
         print("["+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"]"+ upsql2)
         cursor.execute(upsql2)
         connection.commit()
         upsql2="update prodalarm set reset=0,target=0,alarmTime=SYSDATE() where alarmName not in "
       
       time.sleep(300)
       continue

    #if alarm list was null , then pause 5 minutes 
    if len(x) == 0:
      #update alarm record flag
      with connection.cursor() as cursor:
        print("["+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"]"+ upsql1)
        cursor.execute(upsql1)
        connection.commit()

      time.sleep(300)
      continue
    

finally:
  print("["+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"] connection is closed")
  connection.close()

  
