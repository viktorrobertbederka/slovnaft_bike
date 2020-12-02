from math import sin, cos, sqrt, atan2, radians
from sqlalchemy import create_engine, Table, Column, MetaData, update, text
from sqlalchemy.dialects.oracle import VARCHAR2
from tqdm import tqdm

def getRoutesData(table_name: str) -> list:
    """
    Getting unique route IDs list
    """
    con = create_engine("oracle://username:password@ip:port/service_name")
    que1 = con.execute("""select routeid, gpslat, gpslon from {}
                          where gpslat != '0'
                          and gpslon != '0'
                          order by routeid, detectdt asc""".format(table_name))
    data_list = [dict(i) for i in que1]

    proces_1 = []
    for i in data_list:
        temp = []
        for key, value in i.items():
            temp.append(value)
        proces_1.append(temp)
            
    proces_2 = set()
    for i in proces_1:
        proces_2.add(i[0])
    
    proces_3 = []
    for p2 in proces_2:
        temp = []
        for p1 in proces_1:
            if p2 == p1[0]:
                temp.append([p1[1], p1[2]])
        proces_3.append(temp)
        
    return proces_3, proces_2

def getDistance(*coors: list) -> int: 
    """
    Calculating of cumulative distance based on sequence of coordinates
    return: int(cumulative distance)
    """
    l = []
    for coordinates in coors:
        for index, i in enumerate(coordinates):
            if index < len(coordinates) - 1:
                l.append([i, coordinates[index + 1]])
            elif index == len(coordinates) - 1:
                pass
    cumulative_distance = 0

    for i in l:       
        R = 6373.0
        lat1 = radians(float(i[0][0]))
        lon1 = radians(float(i[0][1]))
        lat2 = radians(float(i[1][0]))
        lon2 = radians(float(i[1][1]))
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        distance = (R * c) * 1000
        cumulative_distance += distance
    
    return cumulative_distance
          
def putDistancesTogether(table_name: str) -> list:
    data, routes = getRoutesData(table_name)
    print(len(data), len(routes))
    result = []
    for d, r in zip(data, routes):
        dist = getDistance(d)
        result.append([r, dist])
    
    return result

def pushData(table_name):
    con = create_engine("oracle://username:password@ip:port/service_name")
    data = putDistancesTogether(table_name)
    con.execute("alter table {} add cumulative_distance varchar2(256)".format(table_name))
    for i in tqdm(data):
        con.execute("""update {} 
                       set cumulative_distance = {}
                       where ROUTEID = '{}'""".format(table_name, i[1], i[0]))
                       
def updateSlovnaftTableDistance(table_name):
    pushData(table_name)
    print("Done")
    
    
      
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

        