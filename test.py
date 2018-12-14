import datetime
import json

import pystreamredis
#import gevent
import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
import redis
#import credis
#from credis.geventpool import ResourcePool
conn = redis.StrictRedis()
#conn = ResourcePool(32, credis.Connection, host="localhost")

displayinterval = 50000
fetchinterval = 10000
counter = 0
intervalStart = datetime.datetime.now().timestamp()

streamname = "stream:tag:GEN:PR_COOL_WATER_12BAR_PUMP_PRESS_SCALED"
#streamname = "stream:tag:GEN:PR_COOL_WATER_TANK_LEVEL_SCALED"
filename = streamname.split(":")[-1] + ".csv"

with open(filename,"w") as fp:
    #for x in pystreamredis.Streams(conn,{"stream:tag:GEN:PR_WARM_WATER_TANK_TEMP_SCALED":0,"stream:tag:GEN:PR_BOILER_CONDENSATE_PRESS_SCALED":0}, count=fetchinterval):
    for x in pystreamredis.Streams(conn,[streamname], group="ryan",stop_on_timeout=False):
      try:
        counter = counter + 1
        #xx = json.loads(x[2][1])
        if counter % displayinterval == 0:
          newIntervalStart = datetime.datetime.now().timestamp()
          print(f"{displayinterval} records in {(newIntervalStart-intervalStart)} seconds. ({displayinterval/(newIntervalStart-intervalStart)} records/s)")
          #   print(f"Record:{x}")
          intervalStart = newIntervalStart
          #print(f"{xx[0]},{xx[1][0]}")
      except Exception as e:
          print(e)
        #    print(x)
