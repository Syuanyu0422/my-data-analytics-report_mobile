import csv
import re
import pandas as pd
import numpy as np
import pymysql
import matplotlib.pyplot as plt
 
# 連線到mysql資料庫
def connect2mysql():   
    host='localhost'
    #db=input('請輸入要連線的資料庫名稱(若尚未建立請輸入0來建立):')
    db='report'                 
    user='root'
    password='Sql860422'
    port=3306
    #因為數值帶進來PYTHON會帶著demical，所以作轉換
    conv=pymysql.converters.conversions
    conv[246]= float
    if db=='0':        
        conn= pymysql.connect(host=host,user=user,password=password,port=port)
        cursor = conn.cursor()
        print('已連接MYSQL，請建立資料庫')
        db=input('請輸入想建立的資料庫名稱')
        sql="CREATE DATABASE IF NOT EXISTS {}".format(db)
        cursor.execute(sql)
        print('已建立好資料庫，正在重新連接')        
    conn= pymysql.connect(host=host,db=db,user=user,password=password,charset='utf8',use_unicode=True,conv=conv)    
    print('已連接資料庫',db)        
    return conn

# 將DataFrame資料表寫入CSV
def df2csv(file):
    #了解存入資料的大概內容
    print('顯示此檔案的前五筆資料為:')
    print(file.head())
    #檔名直接用變數名稱做存檔
    fn=[name for name in globals() if globals()[name] is file]
    fn=fn[0]+'.csv'
    file.to_csv(fn.format(file),index=False,encoding='UTF-8')
    print('已存入',fn)

# 將資料表匯入到mysql    
def csv2sql(fn):   
    conn=connect2mysql()
    cur=conn.cursor()
    # 建立資料表
    file=pd.read_csv(fn+'.csv')
    col_str=''
    for col in file.columns:
        dtype=file[col].dtype
        col_str+='`'+col+'` '
        # 設定匯入資料表的各欄位資料型態
        if 'int' in str(dtype):
            col_str+='INT, '
        elif 'float' in str(dtype):
            col_str+='FLOAT, '
        else:
            col_str+='TEXT, '
    sql="CREATE TABLE IF NOT EXISTS {} ({})"
    # 把最後一個欄位後面的,刪除
    col_str=col_str.rstrip(', ')    
    sql=sql.format(fn,col_str)
    cur.execute(sql) 
    
    # 將資料寫入資料表
    with open(fn+'.csv','r',encoding='UTF-8') as file:
        file=csv.reader(file)
        file=list(file)
        for row in file:
            if file.index(row)==0:
                continue
            sql="INSERT INTO {} VALUES {}"
            row=tuple(row)
            sql=sql.format(fn,row)
            cur.execute(sql)
    conn.commit()
    print('已建立資料表',fn)
    conn.close()

# pd.read_sql 可以取代
# def select_from_sql():
#     result=cur.fetchall()
#     cols=[]
#     for i in range(len(cur.description)):
#         cols.append(cur.description[i][0])
#     data=pd.DataFrame(result,columns=cols)
#     return data

#%%
# 資料清理，並寫入資料庫

#第一個檔案
cell_site=pd.read_csv('行動通信業務基地臺統計數(區分縣市業者).csv')   
#前5個數字為年月，不需要日的部分
p1=r'\d\d\d\d\d'
year=[]
month=[]
for i in range(len(cell_site)):      
    cell_site.iloc[i,1]=re.findall(p1,str(cell_site.iloc[i,1]))[0]
    year.append(cell_site.iloc[i,1][:3])
    month.append(cell_site.iloc[i,1][3:])
    #業者名稱後面有很多空白，將空白清除
    cell_site.iloc[i,2]=cell_site.iloc[i,2].rstrip()
    #將業者名稱與其他表格都統一化
    if '台灣之星' in cell_site.iloc[i,2]:
        cell_site.iloc[i,2]='台灣之星'
    else:
        cell_site.iloc[i,2]=cell_site.iloc[i,2].rstrip('股份有限公司')
#插入年、月兩個欄位
cell_site.insert(0,'年',year)
cell_site.insert(1,'月',month)
#將統計期欄位刪掉
cell_site.drop('統計期',axis=1,inplace=True)

#第二個檔案
data_used=pd.read_csv('行動寬頻用戶每月平均數據用量 .csv',thousands=",")
# 經過觀察，業者名稱欄位中空值皆為五間電信的總計，因此將空值改為"總計"
data_used['業者名稱'].fillna('總計',inplace=True)
year=[]
month=[]
# 107年12月的數據用量已有5間電信的數據，又有總計，會影響分析結果，因此將總計刪除
data_used=data_used.drop(axis=0,index=240)
for i in range(len(data_used)):
    # 在分析上面需要將數據使用量的年、月分開
    date=data_used.iloc[i,0].split('/')
    year.append(date[0])
    month.append(date[1])    
    #將業者名稱與其他表格都統一化
    if '台灣之星' in data_used.iloc[i,1]:
        data_used.iloc[i,1]='台灣之星'
    else:
        data_used.iloc[i,1]=data_used.iloc[i,1].rstrip('股份有限公司')
#插入年跟月兩個欄位，並把多餘的年月欄位刪除
data_used.insert(1,'年',year)
data_used.insert(2,'月',month)
data_used.drop('年月',axis=1,inplace=True)
#使用這個資料表再做出一個"客戶數量"的資料表
#各電信使用者數量不需要用到網路傳輸量，故把兩個欄位刪除傳給一個新的"客戶數量"資料表
user_count=data_used.drop('數據傳輸量（GBytes）',axis=1).drop('平均每一用戶數據傳輸量（GBytes）',axis=1)


#第三個檔案
call_time=pd.read_csv(r'行動通信用戶平均每月通話時間.csv',thousands=",")
year=[]
month=[]        
for i in range(len(call_time)):
    #將年份跟月份分為兩個欄位
    date=call_time.iloc[i,1]
    date=date.split('/')
    year.append(date[0])
    month.append(date[1])
    call_time.iloc[i,0]=call_time.iloc[i,0].strip()
    #在網路種類中，行動寬頻等同於4G含5G，將名稱統一
    if call_time.iloc[i,0]=='行動寬頻':
        call_time.iloc[i,0]='4G(含5G)'
    #台灣之星的業者名稱出現不同，也需要統一
    if '台灣之星' in call_time.iloc[i,2]:
        call_time.iloc[i,2]='台灣之星'
    else:
        call_time.iloc[i,2]=call_time.iloc[i,2].rstrip('股份有限公司')
#先將年月的欄位刪除
call_time.drop('年月',axis=1,inplace=True)
#插入年、月兩個欄位
call_time.insert(1,'年',year)
call_time.insert(2,'月',month)


# 第四個檔案為pdf檔
import pdfplumber
pdf= pdfplumber.open(r'附錄五：110定點量測各行政區4G上網速率量測結果.pdf')
# 先用第1頁做開頭，並取得欄位名稱
table=pdf.pages[0].extract_tables()
speed=pd.DataFrame(table[0])
for i in range(1,15):
    table=pdf.pages[i].extract_tables()
    # 第2~15頁就不用欄位名稱，所以從第三列資料開始輸入
    data=pd.DataFrame(table[0][2:])
    speed=pd.concat([speed,data],axis=0)   
# 填入欄位名稱
speed.iloc[1,0]='縣市'
speed.iloc[1,1]='鄉鎮市區' 
# 將檔案拆分為「下載速度」跟「上載速度」
download = speed.drop(range(7,12),axis=1)
upload = speed.drop(range(2,7),axis=1)
# 調整欄位名稱及刪除不需要的列
download.columns=download.iloc[1]
download=download.iloc[2:,:].reset_index(drop=True)
upload.columns=upload.iloc[1]
upload=upload.iloc[2:,:].reset_index(drop=True)
# 反樞紐分析，參考語法:https://medium.com/%E6%95%B8%E6%93%9A%E4%B8%8D%E6%AD%A2-not-only-data/%E5%B8%B6%E4%BD%A0%E5%BF%AB%E9%80%9F%E7%90%86%E8%A7%A3-pandas-melt-%E5%A6%82%E4%BD%95%E4%BD%BF%E7%94%A8-443976e00f2
# 把下載上傳合成一張表就好
net_speed = pd.melt(download, id_vars=download.columns[0:2], value_vars=download.columns[2:7], var_name='業者名稱', value_name='4G下載速率(Mbps)')
upload = pd.melt(upload, id_vars=upload.columns[0:2], value_vars=upload.columns[2:7], var_name='業者名稱', value_name='4G上傳速率(Mbps)')
net_speed.insert(4,'4G上傳速率(Mbps)',upload['4G上傳速率(Mbps)'])


'''
整理出5個資料表:
    cell_site 基地台每年數量(區分業者、縣市)
    data_used 行動數據使用量(區分業者)
    call_time 通話時間(區分業者)
    user_count 客戶數(區分業者)
    net_speed 4G網路速度(區分業者、縣市、鄉鎮)
'''

all_file=np.array([cell_site,data_used,call_time,user_count,net_speed])

# 將整理好了資料存到CSV之後，連到資料庫寫入
for file in all_file:
    df2csv(file)
    fn=[name for name in globals() if globals()[name] is file]
    csv2sql(fn[0])

#%% 先前設定
# 圖表設定中文
plt.rcParams['font.family']='Microsoft YaHei'
# 先設定好各電信label、color
mobile=['中華電信','台灣大哥大','遠傳電信','台灣之星','亞太電信']
color=['#5A96C4','#EBA46A','#EB6A69','#C69DD1','#68DB7C']
# 設定畫布底色
face_c='#BFBFBF'
# 設定格線顏色
grid_c='#E2E2E2'
#連接資料庫
conn=connect2mysql()
cur=conn.cursor()

#%% 用戶的網路使用量&通話使用量

# 近幾年平均網路用量
sql="""SELECT 年,月,SUM(數據傳輸量（GBytes）)/SUM(用戶數) 
FROM data_used GROUP BY 年, 月 ORDER BY 年,月 ;"""
avg_data=pd.read_sql(sql,con=conn)
avg_data.columns=['年','月','平均數據用量']
x=np.arange(len(avg_data['月']))
plt.figure(figsize=(7,4),facecolor=face_c)
plt.plot(x,avg_data['平均數據用量'])
#ticks以每年作為一個區隔，不然太擠了
ticks=[i for i in range(0,len(avg_data['年']),12)]
labels=[str(avg_data['年'][i]) for i in ticks]
plt.xticks(ticks=ticks,labels=labels)
plt.grid(ls='--',c=grid_c)
plt.ylabel('平均數據用量(GBytes)')
plt.title('國人4G行動數據用量趨勢')
plt.show()

# 近幾年4G用戶的平均通話分鐘數 
sql=""" SELECT 年,月,SUM(通話分鐘數（分）)/SUM(用戶數) 
FROM call_time  WHERE `種類` LIKE '4G%'
GROUP BY 年, 月 ORDER BY 年,月 ; """
avg_4Gcall=pd.read_sql(sql,con=conn)
avg_4Gcall.columns=['年','月','平均通話量']
x=np.arange(len(avg_4Gcall['月']))
plt.figure(figsize=(7,4),facecolor=face_c)
plt.plot(x,avg_4Gcall['平均通話量'])
#ticks以每年作為一個區隔
ticks=[i for i in range(0,len(avg_4Gcall['年']),12)]
labels=[str(avg_4Gcall['年'][i]) for i in ticks]
plt.xticks(ticks=ticks,labels=labels)
plt.grid(ls='--',c=grid_c)
plt.xlabel('(年)',loc='right')
plt.ylabel('分\n鐘',rotation=0,loc='bottom') #把ylabel改成直的，放最底下
plt.title('4G用戶通話分鐘數')
plt.show()

# 跟4G尚未開台之前的通話量做比較，通話量減少的趨勢越來越明顯
sql=""" SELECT 年,月,SUM(通話分鐘數（分）)/SUM(用戶數) 
FROM call_time
GROUP BY 年, 月 ORDER BY 年,月 ; """
avg_4Gcall=pd.read_sql(sql,con=conn)
avg_4Gcall.columns=['年','月','平均通話量']
x=np.arange(len(avg_4Gcall['月']))
plt.figure(figsize=(7,4),facecolor=face_c)
plt.plot(x,avg_4Gcall['平均通話量'])
#ticks以每年作為一個區隔
ticks=[i for i in range(0,len(avg_4Gcall['年']),12)]
labels=[str(avg_4Gcall['年'][i]) for i in ticks]
plt.xticks(ticks=ticks,labels=labels)
plt.grid(ls='--',c=grid_c)
plt.ylim(0,150)
plt.xlabel('(年)',loc='right')
plt.ylabel('(分)',rotation=0,labelpad=15,loc='bottom') #把ylabel改成直的，放最底下
plt.title('100年~111年用戶通話分鐘數(含4G開台前)')
plt.show()

#%% 基地台
# 近幾年來，全台4G基地台總數量的趨勢(103~111年)
sql=""" SELECT 年,月,SUM(基地臺) FROM cell_site 
WHERE 類別='4G' AND (月=6 OR 月=12) 
GROUP BY 年 ,月 ORDER BY 年 ,月 
"""
site4G=pd.read_sql(sql, con=conn)
x=np.arange(len(site4G['月']))
plt.figure(figsize=(11,5),facecolor=(face_c))
plt.plot(x,site4G['SUM(基地臺)'],marker='.')
labels=[str(site4G['年'][i])+'/'+str(site4G['月'][i]) for i in range(len(site4G['月']))]
plt.xticks(ticks=x,labels=labels)
plt.grid(ls='--',c=grid_c)
plt.ylabel('基地台總數')
plt.title('4G基地台總數量(103~111年)')
plt.show()

# 近兩年業者積極蓋5G基地台，4G基地台越來越少了嗎? 還是都市受影響較大，鄉下沒什麼影響?
# 這邊也可以用POWER BI製作圖表，用交叉篩選器選縣市
#city=input('請輸入要查詢「近幾年4G基地台總數趨勢」的縣市:')
city='金門縣'
sql="""SELECT `年`,`月`,SUM(`基地臺`) FROM `report`.`cell_site` 
WHERE `類別`= '4G' AND `縣市`='{}' 
GROUP BY `年`,`月`,`縣市` ORDER BY `年`,`月` ;"""
sql=sql.format(city)
site_city=pd.read_sql(sql, con=conn)
x=range(len(site_city))
plt.figure(figsize=(9,5),facecolor=(face_c))
plt.plot(x,site_city['SUM(`基地臺`)'])
ticks=range(0,len(site_city),6)
labels=[str(site_city.iloc[i,0])+'/'+str(site_city.iloc[i,1]) for i in ticks]
plt.xticks(ticks=ticks,labels=labels)
plt.title('103~111年'+city+'的4G基地台總數變化')
plt.show()
# 其實4G基地台減少跟建設5G沒有關係，只是因為最新電信法的調整
# 參考: https://tel3c.tw/blog/post/33728


# 近幾年來，"各電信"4G基地台總數量的趨勢(103~111年)
sql="""SELECT 年,月,業者名稱,SUM(`基地臺`) FROM cell_site 
WHERE 年>103 AND (月=6 OR 月=12) 
GROUP BY 年 ,月,業者名稱 ORDER BY 年 ,月"""
site_year=pd.read_sql(sql, con=conn)
plt.figure(figsize=(10,5),facecolor=(face_c))
df_list=[]
for c,m in zip(color,mobile):
    df=site_year.loc[site_year['業者名稱'] == m ]
    df_list.append(df)
    x=np.arange(len(df['月']))
    plt.plot(x,df['SUM(`基地臺`)'],color=c,label=m)
    # X軸標籤日期
    labels=[str(df.iloc[i,0])+'/'+str(df.iloc[i,1]) for i in range(len(df['月']))]
    plt.xticks(ticks=x,labels=labels)
plt.xlabel('(年)',loc='right')
plt.legend(loc=2)
plt.title('近幾年，各電信的4G基地台總數量')
plt.show()

# 各電信目前在台灣五大城市的基地台數量
sql="""SELECT `縣市`, `業者名稱`, AVG(`基地臺`) 
FROM `report`.`cell_site` 
WHERE `年`=111 AND `月`=11
AND (`縣市`='新北市' OR `縣市`= '臺北市' OR `縣市`= '臺中市' OR `縣市`= '臺南市' OR `縣市`= '高雄市') 
GROUP BY `業者名稱` , `縣市` ;"""
now_site=pd.read_sql(sql, con=conn)
# 設定五條bar的X軸
x=[1,2,3,4,5]
x2=[(i-0.2) for i in x]
x1=[(i-0.1) for i in x]
x3=[(i+0.1) for i in x]
x4=[(i+0.2) for i in x]
x_list=[x2,x1,x,x3,x4]
plt.figure(figsize=(8,5.2),facecolor=face_c)
for i in range(5):
    df= now_site.loc[now_site['業者名稱'] ==mobile[i]]
    plt.bar(x_list[i],df['AVG(`基地臺`)'],color=color[i],width=0.08,label=mobile[i])
plt.legend()
plt.xticks(ticks=x,labels=['新北市','臺北市','臺中市','臺南市','高雄市'])
plt.ylabel('基地台數量')
plt.title('111年各電信在五大都市的基地台數量')
plt.show()


#%% 用戶數
# 111年11月，最新各電信用戶數比例
sql="SELECT * FROM user_count WHERE 年=111 AND 月=11 ; "
user= pd.read_sql(sql,con=conn)
plt.figure(figsize=(10,9),facecolor=(face_c))
#要將圓餅圖內的字體變大
#參考:https://blog.csdn.net/chenpe32cp/article/details/87865625
patches,l_text,p_text=plt.pie(user['用戶數'],labels=user['業者名稱'],
                              colors=color,shadow=True,autopct='%1.1f%%',radius=0.9)
for t in l_text:
    #把label的字藏起來，留圖例就好
    t.set_color(face_c)
for t in p_text:
    t.set_size(22)
    t.set_color('w')
plt.title('各電信用戶數占比',fontsize=20)
plt.legend(loc=2,fontsize=13)
plt.show()

#%% 基地台VS用戶數

# 比較基地台建設速度與用戶數量變化速度
# 取兩個資料都有的日期區間107/1~111/11
# 設定雙Y軸
fig, ax1 = plt.subplots()
sql="""SELECT `年`,`月`,SUM(`用戶數`) FROM `report`.`user_count` 
WHERE 年>106 GROUP BY `年`,`月` ORDER BY `年`,`月` ; """
sum_user_year=pd.read_sql(sql, con=conn)
# 數字過大變成科學記號，把它改為一般數字
sum_user_year['SUM(`用戶數`)']=sum_user_year['SUM(`用戶數`)'].astype('int64')
x=range(len(sum_user_year))
plt.bar(x,sum_user_year['SUM(`用戶數`)'],color='#CDD8E5',label='用戶數量')
plt.ylabel('用\n戶\n數',rotation=0,labelpad=(10))
ax2 = ax1.twinx()
sql="""SELECT `年`,`月`,SUM(`基地臺`) FROM `report`.`cell_site` 
WHERE 年>106 AND 類別='4G' GROUP BY `年`,`月` ORDER BY `年`,`月` ;"""
sum_site_year=pd.read_sql(sql, con=conn)
sum_site_year['SUM(`基地臺`)']=sum_site_year['SUM(`基地臺`)'].astype('int64')
x=range(len(sum_site_year))
plt.plot(x,sum_site_year['SUM(`基地臺`)'],label='基地台數量')
ticks=[i for i in range(0,len(sum_site_year),6)]
labels=[str(sum_site_year['年'][i])+'/'+str(sum_site_year['月'][i]) for i in ticks]
plt.xticks(ticks=ticks,labels=labels)
plt.ylabel('基\n地\n台\n數\n量',rotation=0)
plt.title('107~111年全台基地台數量與用戶數量')
plt.legend(loc=2)
plt.show()

# 比較用戶數量變化 與 基地台跟用戶數的比例(基地台/用戶數)
# 取兩個資料都有的日期區間107/1~111/11
fig, ax1 = plt.subplots()
sql="""SELECT `年`,`月`,SUM(`用戶數`) FROM `report`.`user_count` 
WHERE 年>106 GROUP BY `年`,`月` ORDER BY `年`,`月` ; """
sum_user_year=pd.read_sql(sql, con=conn)
# 數字過大變成科學記號，把它改為一般數字
sum_user_year['SUM(`用戶數`)']=sum_user_year['SUM(`用戶數`)'].astype('int64')
x=range(len(sum_user_year))
plt.bar(x,sum_user_year['SUM(`用戶數`)'],color='#CDD8E5',label='用戶數量')
plt.ylabel('用\n戶\n數',rotation=0,labelpad=(10))
plt.ylim(min(sum_user_year['SUM(`用戶數`)']),(max(sum_user_year['SUM(`用戶數`)']))*1.01)
plt.legend(loc=2)
ax2 = ax1.twinx()
sql="""SELECT `年`,`月`,SUM(`基地臺`) FROM `report`.`cell_site` 
WHERE 年>106 AND 類別='4G' GROUP BY `年`,`月` ORDER BY `年`,`月` ;"""
sum_site_year=pd.read_sql(sql, con=conn)
sum_site_year['SUM(`基地臺`)']=sum_site_year['SUM(`基地臺`)'].astype('int64')
x=range(len(sum_site_year))
plt.plot(x,sum_site_year['SUM(`基地臺`)']/sum_user_year['SUM(`用戶數`)'],label='基地台與用戶數比')
ticks=[i for i in range(0,len(sum_site_year),6)]
labels=[str(sum_site_year['年'][i])+'/'+str(sum_site_year['月'][i]) for i in ticks]
plt.xticks(ticks=ticks,labels=labels)
plt.title('107~111年全台用戶數量與基地台、用戶數比例')
plt.legend(loc=2)
plt.show()


#%% 110年4G網速 (比較基地台，取110年12月)
# 網速用python做運算、基地台用SQL語法做運算

# 全台各電信4G上載/下載平均網速
sql="SELECT * FROM net_speed ;"
tw_speed = pd.read_sql(sql,con=conn)
# 設定兩條bar的x軸
x=[1,2,3,4,5]
x1=[(i-0.2) for i in x]
# 設定下載/上傳速度的y軸
y=[]
y1=[]
# 分為5間業者，算出每間業者平均網速
for m in mobile:
    df=tw_speed[tw_speed['業者名稱']==m]
    y.append(np.mean(df['4G下載速率(Mbps)']))
    y1.append(np.mean(df['4G上傳速率(Mbps)']))

# 設定雙Y軸
fig, ax1 = plt.subplots()
# 用zorder改圖層位置，將格線放在最底層
plt.grid(ls='--',c=grid_c,zorder=0)
# 繪製長條圖
plt.bar(x,y,color='#7AB8CC',width=0.2,label='下載速度',zorder=2)
plt.bar(x1,y1,color='#FFA07A',width=0.2,label='上傳速度',zorder=2)
plt.legend()
# xticks取在兩者的x中間，才會置中
mid_x=[(i-0.1) for i in x]
plt.xticks(ticks=mid_x,labels=mobile)
plt.ylabel('4G速率(Mbps)')
# 設定第二條Y軸
ax2=ax1.twinx()
# 全台各電信基地台(取與網速資料同期_110年12月)
sql="""SELECT `業者名稱` , SUM(`基地臺`) FROM cell_site 
WHERE 年=110 AND  月=12 
GROUP BY `業者名稱` ;"""
site_110=pd.read_sql(sql, con=conn)
# 重新做排序
site_110.insert(2,'index',[0,1,3,4,2])
site_110.set_index(site_110['index'],inplace=True)
site_110.sort_index(inplace=True)
# 設定折線圖的X軸
mid_x=[(i-0.1) for i in x]
plt.plot(mid_x,site_110.iloc[:,1],marker='.',color='#8a6e66')
plt.ylabel('基地台數量')
plt.title('全台各電信4G網速與基地台數量比較')
plt.show()

conn.close()
# conn=connect2mysql()
# cur=conn.cursor()
# sql='DROP DATABASE report'
# cur.execute(sql)
