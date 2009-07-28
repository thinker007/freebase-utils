import datetime
import logging

from freebase.api import HTTPMetawebSession, MetawebError

def initLogging(name):
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.WARN) # dial down freebase.api's chatty root logging
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    return log
log = initLogging('fbStats')

def fbRead(session, query):
#    log.debug([  "  Read query = ", query])
#    try:
    response = session.mqlread(query)
#    except:
#        # TODO - Is the retryable?  Wait and retry
#        # if not retryable throw exception
#        log.error([ "**Freebase query MQL failed : ", query])
#        return []
    
#    log.debug([ "    Response = ",response])    
    return response

def fbReadAtTime(session, query, time=None):
#    log.debug([  "  Read query = ", query])
#    try:
    response = session.mqlreadmulti(query, time)
#    except:
#        # TODO - Is the retryable?  Wait and retry
#        # if not retryable throw exception
#        log.error([ "**Freebase query MQL failed : ", query])
#        return []
    
#    log.debug([ "    Response = ",response])    
    return response

def main():
#    usersByMonth()
    lastWriteByUser()
    
def usersByMonth():
    # Establish session
    session = HTTPMetawebSession('www.freebase.com')
    #    session.login()
    
    query = [{
              "return" : "count",
              "type" : "/type/type"
    },
     {'type':'/freebase/type_profile',
     '/freebase/type_profile/instance_count': None,
     '/freebase/type_profile/property_count': None,
      'id' : '/common/topic'
     },
      {
       "return" : "count",
       "type" : "/freebase/user_activity",
       "u:user" : {
          "/type/user/usergroup|=" : ["/freebase/mwstaff",
                                      "/freebase/bots"],
          "optional" : "forbidden"
        }
       },
      {
       "return" : "count",
       "type" : "/freebase/user_activity",
       "primitives_written>" : 100,
       "u:user" : {
          "/type/user/usergroup|=" : ["/freebase/mwstaff",
                                      "/freebase/bots"],
          "optional" : "forbidden"
        }
       },
      {
       "return" : "count",
       "type" : "/freebase/user_activity",
       "primitives_written>" : 1000,
       "u:user" : {
          "/type/user/usergroup|=" : ["/freebase/mwstaff",
                                      "/freebase/bots"],
          "optional" : "forbidden"
        }
       },
                    {
       "return" : "count",
       "type" : "/freebase/user_activity",
       "u:user" : {
          "/type/user/usergroup" : "/freebase/mwstaff",
        }
       },
             {
       "return" : "count",
       "type" : "/freebase/user_activity",
       "u:user" : {
          "/type/user/usergroup" : "/freebase/bots",
        }
       },
            {'type' : '/type/user',
      'return': 'count'},

    #     [{'limit' : 20,
#      'type':'/freebase/type_profile',
#     '/freebase/type_profile/property_count': None,
#     '/freebase/type_profile/instance_count': None,
#     'sort' : '-/freebase/type_profile/instance_count', 
#     'i:/freebase/type_profile/instance_count>': 1000,
#     'name' : None
#     }]
     ]
    

    
#    query.q0.query['user'] = '/user/tfmorris'
    
    startDate = datetime.date(2007, 2, 1)
    endDate =  datetime.date.today()
    date = endDate
    print 'Date', 'Types', 'Topics', 'Facts', 'NonStaffUsers', 'Users>100writes', 'Users>1000writes', 'Staff', 'Bots', 'Users'
    while date >= startDate:
        results = fbReadAtTime(session, query, date.isoformat())
        print date, results[0], results[1]['/freebase/type_profile/instance_count'], results[1]['/freebase/type_profile/property_count'],results[2], results[3], results[4], results[5], results[6], results[7]
        if date.day > 1:
            date = date.replace(day=1)
        else:
            month = date.month - 1
            year = date.year
            if month < 1:
                month = 12
                year -= 1
            date = datetime.date(year, month, 1)
        

def lastWriteByUser():
    # Establish session
    session = HTTPMetawebSession('www.freebase.com')
    #    session.login()

    # mqlread and mqlreaditer take different forms - ugh no [] for readiter
    usersQuery = {"limit" : 10,
                   "type" : "/type/user",
                   "id" : None,
                   "/type/object/timestamp" : None,
                   "usergroup":[{"id": None}]
                   }
    
    lastWriteQuery = [{
                       "limit" : 1,
                       "/type/object/timestamp" : None,
              "sort": "-/type/object/timestamp",
              "type" : "/type/object",
              "/type/object/creator" : "/user/tfmorris",
              "type": [],
              "id" : None,
              "name": None
              }]
    
    userStatsQuery = {"primitives_live" : None,
                       "primitives_written" : None,
                       "topics_live" : None,
                       "type" : "/freebase/user_activity",
                       "types_live" : None,
                       "user" : "/user/tfmorris"
                       }
    
    #users = fbRead(session, [{"return": "count" , "type" : "/type/user"}])
    # print 'Total users = ', users[0], 

    userCount = 0
    cursor = True
    now = datetime.datetime.now()
    print 'Date: ', now
    print 'User', 'Staff', 'Bot', 'Created', 'Days active', 'Days since last write', 'Date of Last Write', 'Last Write Id' , 'Last Write Topic', 'Types Live', 'Topics Live', 'Primitives Written', 'Primitives Live'
    users = session.mqlreaditer(usersQuery)
    histActive = {}
    histElapsed = {}
    for user in users:
        userCount += 1
        lastWriteQuery[0]['/type/object/creator'] = user['id']
        results = fbRead(session, lastWriteQuery)

        created = datetime.datetime.strptime(user['/type/object/timestamp'][:10],
                                                '%Y-%m-%d')
        lwTime = ''
        elapsed = 9999
        active = -1
        lwId = ''
        if len(results)>0:
            lwTime = datetime.datetime.strptime(results[0]['/type/object/timestamp'][:19],
                                                '%Y-%m-%dT%H:%M:%S')
            elapsed = int(max(None,0,(now - lwTime).days))
            active = int(max(None,0,(lwTime - created).days))        

        userStatsQuery['user'] = user['id']
        userStats = fbRead(session, userStatsQuery)
        types_live = 0
        topics_live = 0
        primitives_written = 0
        primitives_live = 0
        if userStats:
            types_live = userStats.types_live if userStats.types_live else 0
            topics_live = userStats.topics_live if userStats.topics_live else 0
            primitives_written = userStats.primitives_written if userStats.primitives_written else 0
            primitives_live = userStats.primitives_live if userStats.primitives_live else 0

        staff = 1 if {'id':'/freebase/mwstaff'} in user.usergroup or {'id': '/en/metaweb_staff'} in user.usergroup else 0
        bot = 1 if {'id':'/freebase/bots'} in user.usergroup else 0
        
        print user['id'], staff, bot, user['/type/object/timestamp'][:10], active, elapsed, lwTime, lwId, types_live, topics_live, primitives_written, primitives_live


if __name__ == '__main__':
     main() 


{
  "q0" : {
    "as_of_time" : "2008-01-01",
    "query" : [
      {
        "return" : "estimate-count",
        "type" : "/common/topic"
      }
    ]
  }
}

topUserQuery = {
        "limit" : 100,
        "primitives_live" : None,
        "primitives_written" : None,
        "primitives_written>" : 50,
        "sort" : "-primitives_written",
        "topics_live" : None,
        "type" : "/freebase/user_activity",
        "types_live" : None,
        "user" : {
                  "/type/user/usergroup|=" : ["/freebase/mwstaff",
                                      "/freebase/bots"],
                  "optional" : "forbidden"}
      }

topUserNoStaffQuery = {
        "limit" : 100,
        "primitives_live" : None,
        "primitives_written" : None,
        "primitives_written>" : 50,
        "sort" : "-primitives_written",
        "topics_live" : None,
        "type" : "/freebase/user_activity",
        "types_live" : None,
        "u:user" : {
                    #  "/boot/all_group" for inclusion? exclude empty list
          "/type/user/usergroup|=" : ["/freebase/mwstaff",
                                      "/freebase/bots"],
          "id" : None,
          "optional" : "forbidden"
        },
        "user" : None
      }

def queryUserStats(session, user, endTtime, duration):
    if duration == None:
        duration = timedelta(1)
        
    if endTime == None:
        t2 = datetime.now()
    else:
        t2 = endTime
        
    t1 = t2 - duration;
    
    queries = [{
     "creator" : user,
     "return" : "count"  
    },
    {
     "creator" : user,
     "return" : "count",
     "type" : "/type/link"
    }]
    
    result1 = fbRead(session, queries, t1)
    result2 = fbRead(session, queries, t2)