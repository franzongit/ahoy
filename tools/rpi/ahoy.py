"""
First attempt at providing basic 'master' ('DTU') functionality
for Hoymiles micro inverters.
Based in particular on demostrated first contact by 'of22'.
"""
import sys
import argparse
import time
import struct
import crcmod
import json
from datetime import datetime
from RF24 import RF24, RF24_PA_LOW, RF24_PA_MAX, RF24_250KBPS
import paho.mqtt.client
from configparser import ConfigParser

parser = argparse.ArgumentParser(description='monitor homiles')
parser.add_argument('-c', dest='configName', action='store', default='ahoy.conf',help='config file with settings')
parser.add_argument('-m', dest='mqttMode', action='store', default='1',help='mqtt mode, default 1')
parser.add_argument('-d', dest='debugMode', action='store', default='0',help='debug output, default 0')
parser.add_argument('-i', dest='pollingInterval', action='store', default='10',help='per inverter polling inverval, default 60')
parser.add_argument('-f', dest='file', action='store', default='',help='file output, default none')
parser.add_argument('-e', dest='endtime', action='store', default='',help='endtime in HH24:MI, default none')
args = vars(parser.parse_args())

mqttMode=int(args['mqttMode'])>0
print("mqttMode",mqttMode)

debugMode=int(args['debugMode'])>0
print("debugMode",debugMode)

minRefreshSeconds=int(args['pollingInterval'])
print("inverter polling interval in seconds:",minRefreshSeconds)

configName=args['configName']
print("using config from",configName)

cfg = ConfigParser()
cfg.read(configName)
mqtt_host = cfg.get('mqtt', 'host', fallback='192.168.1.1')
mqtt_port = cfg.getint('mqtt', 'port', fallback=1883)
mqtt_user = cfg.get('mqtt', 'user', fallback='')
mqtt_password = cfg.get('mqtt', 'password', fallback='')

fileName=args['file']
outFile=None
if fileName!="":
    print("using output file:",fileName)
    outFile=open(fileName,"a")

endTime=args['endtime']
if endTime=="":
    endTime="ZZ:ZZ"
else:
    print("will terminate at:",endTime)

radio = RF24(22, 0, 1000000)
if mqttMode:
    mqtt_client = paho.mqtt.client.Client()
    mqtt_client.username_pw_set(mqtt_user, mqtt_password)
    mqtt_client.connect(mqtt_host, mqtt_port)
    mqtt_client.loop_start()

# Master Address ('DTU')
dtu_ser = cfg.get('dtu', 'serial', fallback='99978563412')  # identical to fc22's

# inverter serial numbers
inv_ser = cfg.get('inverter', 'serial', fallback='444473104619')  # my inverter

l_inv_ser=inv_ser.strip().split(",") 

#all inverters
#...

f_crc_m = crcmod.predefined.mkPredefinedCrcFun('modbus')
f_crc8 = crcmod.mkCrcFun(0x101, initCrc=0, xorOut=0)

def ser_to_type(s):
    radioKey=s[4:]
    if s.startswith("1161"):                # Thomas B (tnombody), peter l, lukasp
        return ("HM-1200",radioKey,4)       # HM-1500
    elif s.startswith("1121"):              #
        return ("HM-300",radioKey,2)        # HM-350, HM-400   marcel, franz, (mpolak 350+700), avr-herbi
    elif s.startswith("1141"):              #petersilie, Martin P. (mpolak77), carsten B, golf2010, jan-jonas s, lpb
        return ("HM-600",radioKey,2)        # HM-700, HM-800
    elif s.startswith("1060"):              #
        return ("MI-1000",radioKey,2)       #
    elif s.startswith("1061"):              #
        return ("MI-1200",radioKey,4)       # MI-1500        
    elif s.startswith("1020"):              #
        return ("MI-250",radioKey,2)        #
    elif s.startswith("1021"):              #
        return ("MI-300",radioKey,2)        #        
    elif s.startswith("1040"):              #
        return ("MI-500",radioKey,2)        #    
    else:
        return ("",radioKey,1)  

def ser_to_hm_addr(s):
    """
    Calculate the 4 bytes that the HM devices use in their internal messages to 
    address each other.
    """
    bcd = int(str(s)[-8:], base=16)
    return struct.pack('>L', bcd)


def ser_to_esb_addr(s):
    """
    Convert a Hoymiles inverter/DTU serial number into its
    corresponding NRF24 'enhanced shockburst' address byte sequence (5 bytes).

    The NRF library expects these in LSB to MSB order, even though the transceiver
    itself will then output them in MSB-to-LSB order over the air.
    
    The inverters use a BCD representation of the last 8
    digits of their serial number, in reverse byte order, 
    followed by \x01.
    """
    air_order = ser_to_hm_addr(s)[::-1] + b'\x01'
    return air_order[::-1]

def compose_0x80_msg(dst_ser_no=72220200, src_ser_no=72220200, ts=None, messageType=b'\x80', subtype=b'\x0b', refetch=None):
    """
    Create a valid 0x80 request with the given parameters, and containing the 
    current system time.
    """

    if not ts:
        ts = 0x623C8ECF  # identical to fc22's for testing  # doc: 1644758171

    # "framing"
    p = b''
    p = p + b'\x15'
    p = p + ser_to_hm_addr(dst_ser_no)
    p = p + ser_to_hm_addr(src_ser_no)
    if refetch is not None:
        p=p+refetch        
        crc8 = f_crc8(p)
        p = p + struct.pack('B', crc8)   
        return p
    
    p = p + messageType

    # encapsulated payload
    pp = subtype + b'\x00'
    pp = pp + struct.pack('>L', ts)  # big-endian: msb at low address
    #pp = pp + b'\x00' * 8    # of22 adds a \x05 at position 19

    pp = pp + b'\x00\x00\x00\x05\x00\x00\x00\x00'

    # CRC_M
    crc_m = f_crc_m(pp)

    p = p + pp
    p = p + struct.pack('>H', crc_m)

    crc8 = f_crc8(p)
    p = p + struct.pack('B', crc8)   
    return p


def print_addr(a):
    print(f"ser# {a} ", end='')
    print(f" -> HM  {' '.join([f'{x:02x}' for x in ser_to_hm_addr(a)])}", end='')
    print(f" -> ESB {' '.join([f'{x:02x}' for x in ser_to_esb_addr(a)])}")

# time of last transmission - to calculcate response time
t_last_tx = 0

def on_receive(p=None, ctr=None, ch_rx=None, ch_tx=None, time_rx=datetime.now(), latency=None):
    """
    Callback: get's invoked whenever a Noridc ESB packet has been received.
    :param p: Payload of the received packet.
    """

    d = {}
    dd = {}
    
    t_now_ns = time.monotonic_ns()
    ts = datetime.utcnow()
    ts_unixtime = ts.timestamp()
    size=len(p)
    d['ts_unixtime'] = ts_unixtime
    d['isodate'] = ts.isoformat()
    d['rawdata'] = " ".join([f"{b:02x}" for b in p])
    d['trans_id'] = ctr

    dt = time_rx.strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"{dt} Received {size} b  {ch_tx:2d} to channel {ch_rx:2d} after tx {latency:10d} ns: " +
        " ".join([f"{b:02x}" for b in p]))
    # check crc8
    crc8 = f_crc8(p[:-1])
    d['crc8_valid'] = True if crc8==p[-1] else False
    
    if debugMode:
        ws='H'*int((size-1-10)/2)
        pw=struct.unpack('>B'+ws, p[9:size-1])
        print(f"{dt} Recwords {size} b  {ch_tx:2d} to channel {ch_rx:2d} after tx {latency:10d} ns: " + "      "+f"crc: {int(crc8==p[-1])}            "+
            " ".join([f"{b:05d}" for b in pw]))
    

    # interpret content
    mid = p[0]
    d['mid'] = mid
    d['response_time_ns'] = t_now_ns-t_last_tx
    d['ch_rx'] = ch_rx
    d['ch_tx'] = ch_tx
    d['src'] = 'src_unkn'
    d['name'] = 'name_unkn'
 
    if mid == 0x95:
        src, dst, cmd = struct.unpack('>LLB', p[1:10])
        inv_id=f'{src:08x}'
        if inv_id in m_last_tx:
            d['response_time_ns'] = t_now_ns-m_last_tx[inv_id]
        d['src'] = f'{src:08x}'
        d['dst'] = f'{dst:08x}'
        d['cmd'] = cmd
        if debugMode:
            print(f'MSG src={d["src"]}, dst={d["dst"]}, cmd={d["cmd"]}:')

        if inv_id not in mType:
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"UNKWOWN source",inv_id)
            return
        invType=mType[inv_id]
        d['fullsrc']=mFullSer[inv_id]
        
        if invType=="HM-1200": # based on lumapu  https://www.mikrocontroller.net/topic/525778?page=single#7038756
            d["infos"]=[dd]
            
            if cmd==1:
                dd['name'] = 'emeter-dc'  # guess voltages are dc like with hm-600 and total power is long 
                uk1, u1, i1, i2, p1, p2, ptotal1, uk8 = struct.unpack('>HHHHHHLH', p[10:28])
                   
                dd['1/voltage'] = u1/10
                dd['1/current'] = i1/100
                dd['2/current'] = i2/10
                dd['1/power'] = p1/10
                dd['2/power'] = p2/10
                dd['1/totalenergy'] = ptotal1
                
                dd['_uk1'] = uk1
                dd['_uk8'] = uk8
                _hm1200_ptotal2hb=uk8*65536

            elif cmd==2:
                dd['name'] = 'emeter-dc'  # guess voltages are dc like with hm-600 
                ptotal2, pday1, pday2, u2, i3, i4, p3, uk8 = struct.unpack('>LHHHHHHH', p[10:28])
                    
                dd['2/voltage'] = u2/10
                dd['3/current'] = i3/100
                dd['4/current'] = i4/10
                _hm1200_i4=i4/10
                if i4==0:
                    dd['4/voltage'] = 0
                    dd['4/power'] = 0
                elif _hm1200_p4!=0:
                    dd['4/voltage'] = _hm1200_p4/_hm1200_i4
                    
                dd['3/power'] = p3/10   
                dd['3/voltage'] = (p3/10)/(i3/100)  # hack where is it
 
                dd['2/totalenergy'] = ptotal2+_hm1200_ptotal2hb
                dd['1/todaysenergy'] = pday1
                dd['2/todaysenergy'] = pday2
                                
                dd['_uk8'] = uk8
        
            elif cmd==3:
                dd['name'] = 'emeter-dc'  # guess voltages are dc like with hm-600 
                p4, ptotal3, ptotal4, pday3, pday4, u, uk7 = struct.unpack('>HLLHHHH', p[10:28])
                
                dd['4/power'] = p4/10    # where is voltage channel 2+3, 3 cannot be calculated with single message
                _hm1200_p4=p4/10
                if p4==0:
                    dd['4/voltage'] = 0
                    dd['4/current'] = 0
                elif _hm1200_i4!=0:
                    dd['4/voltage'] = _hm1200_p4/_hm1200_i4
                                
                dd['3/totalenergy'] = ptotal3
                dd['4/totalenergy'] = ptotal4
                dd['4/todaysenergy'] = pday3
                dd['4/todaysenergy'] = pday4
                
                dd={}
                d["infos"].append(dd)
                dd['name'] = 'emeter' 
                dd['0/voltageAC'] = u/10                

                dd['_uk7'] = uk7
              
            elif cmd==132: #0x84
                freq, p, uk3, i, pctload, t = struct.unpack('>HHHHHH', p[10:22])
                dd['name'] = 'emeter'    
                dd['0/frequency'] = freq/100
                dd['0/powerAC'] = p/10
                dd['0/currentAC'] = i/100
                dd['0/voltageAC'] = (p/10)/(i/100)
                dd['0/pctload'] = pctload/10
                dd['0/temperature'] = t/10
                
                dd['_uk3'] = uk3
                
        
        if invType=="HM-600":       # original petersilie ahoy.py with renaming
            d["infos"]=[dd]
            if cmd==1:
                dd['name'] = 'emeter-dc'
                uk1, u1, i1, p1, u2, i2, p2, uk8 = struct.unpack(
                    '>HHHHHHHH', p[10:26])
                dd['1/voltage'] = u1/10
                dd['1/current'] = i1/100
                dd['1/power'] = p1/10
                dd['2/voltage'] = u2/10
                dd['2/current'] = i2/100
                dd['2/power'] = p2/10
                p=p1+p2
                
                dd={}
                d["infos"].append(dd)
                dd['name'] = 'emeter'    
                dd['0/powerAC'] = p
                dd['_uk1'] = uk1
                dd['_uk8'] = uk8
                
                _hm600_ptotal1hb=uk8*65536

            elif cmd==2:
                dd['name'] = 'emeter'
                ptotal1, ptotal2, pday1, pday2, u, f, p = struct.unpack(
                    '>HLHHHHH', p[10:26])
                dd['0/voltageAC'] = u/10
                dd['0/frequency'] = f/100
                dd['0/powerAC'] = p/10    
                dd['0/currentAC'] = i/100
                
                dd={}
                d["infos"].append(dd)
                dd['name'] = 'emeter-dc'  
                dd['1/totalenergy'] = ptotal1+_hm600_ptotal1hb                # just 16 bit?, is info in 2/uknown8 ?
                dd['2/totalenergy'] = ptotal2                
                dd['1/todaysenergy'] = pday1
                dd['2/todaysenergy'] = pday2                

            elif cmd==3:  # 0x03
                """
                On HM600 Response to
                    0x80 0x03 (garbled data)
                """
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack(
                    '>HHHHHHHH', p[10:26])
                    
                dd['name'] = 'error3'
                if debugMode:
                    print(f'uk1={uk1}, ', end='')
                    print(f'uk2={uk2}, ', end='')
                    print(f'uk3={uk3}, ', end='')
                    print(f'uk4={uk4}, ', end='')
                    print(f'uk5={uk5}, ', end='')
                    print(f'uk6={uk6}, ', end='')
                    print(f'uk7={uk7}, ', end='')
                    print(f'uk8={uk8}')

                dd['_uk1'] = uk1
                dd['_uk2'] = uk2
                dd['_uk3'] = uk3
                dd['_uk4'] = uk4
                dd['_uk5'] = uk5
                dd['_uk6'] = uk6
                dd['_uk7'] = uk7
                dd['_uk8'] = uk8

            elif cmd==4:  # 0x04
                """
                On HM600 Response to
                    0x80 0x03 (garbled data)
                """
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack(
                    '>HHHHHHHH', p[10:26])
                    
                dd['name'] = 'error4'
                if debugMode:
                    print(f'uk1={uk1}, ', end='')
                    print(f'uk2={uk2}, ', end='')
                    print(f'uk3={uk3}, ', end='')
                    print(f'uk4={uk4}, ', end='')
                    print(f'uk5={uk5}, ', end='')
                    print(f'uk6={uk6}, ', end='')
                    print(f'uk7={uk7}, ', end='')
                    print(f'uk8={uk8}')

                dd['_uk1'] = uk1
                dd['_uk2'] = uk2
                dd['_uk3'] = uk3
                dd['_uk4'] = uk4
                dd['_uk5'] = uk5
                dd['_uk6'] = uk6
                dd['_uk7'] = uk7
                dd['_uk8'] = uk8

            elif cmd==5:  # 0x05
                """
                On HM600 Response to
                    0x80 0x03 (garbled data)
                """
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack(
                    '>HHHHHHHH', p[10:26])
                    
                dd['name'] = 'error5'
                if debugMode:
                    print(f'uk1={uk1}, ', end='')
                    print(f'uk2={uk2}, ', end='')
                    print(f'uk3={uk3}, ', end='')
                    print(f'uk4={uk4}, ', end='')
                    print(f'uk5={uk5}, ', end='')
                    print(f'uk6={uk6}, ', end='')
                    print(f'uk7={uk7}, ', end='')
                    print(f'uk8={uk8}')

                dd['_uk1'] = uk1
                dd['_uk2'] = uk2
                dd['_uk3'] = uk3
                dd['_uk4'] = uk4
                dd['_uk5'] = uk5
                dd['_uk6'] = uk6
                dd['_uk7'] = uk7
                dd['_uk8'] = uk8

            elif cmd==6:  # 0x06
                """
                On HM600 Response to
                    0x80 0x03 (garbled data)
                """
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack(
                    '>HHHHHHHH', p[10:26])
                    
                dd['name'] = 'error7'
                if debugMode:
                    print(f'uk1={uk1}, ', end='')
                    print(f'uk2={uk2}, ', end='')
                    print(f'uk3={uk3}, ', end='')
                    print(f'uk4={uk4}, ', end='')
                    print(f'uk5={uk5}, ', end='')
                    print(f'uk6={uk6}, ', end='')
                    print(f'uk7={uk7}, ', end='')
                    print(f'uk8={uk8}')

                dd['_uk1'] = uk1
                dd['_uk2'] = uk2
                dd['_uk3'] = uk3
                dd['_uk4'] = uk4
                dd['_uk5'] = uk5
                dd['_uk6'] = uk6
                dd['_uk7'] = uk7
                dd['_uk8'] = uk8

            elif cmd==7:  # 0x07
                """
                On HM600 Response to
                    0x80 0x03 (garbled data)
                """
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack(
                    '>HHHHHHHH', p[10:26])
                    
                dd['name'] = 'error8'
                if debugMode:
                    print(f'uk1={uk1}, ', end='')
                    print(f'uk2={uk2}, ', end='')
                    print(f'uk3={uk3}, ', end='')
                    print(f'uk4={uk4}, ', end='')
                    print(f'uk5={uk5}, ', end='')
                    print(f'uk6={uk6}, ', end='')
                    print(f'uk7={uk7}, ', end='')
                    print(f'uk8={uk8}')

                dd['_uk1'] = uk1
                dd['_uk2'] = uk2
                dd['_uk3'] = uk3
                dd['_uk4'] = uk4
                dd['_uk5'] = uk5
                dd['_uk6'] = uk6
                dd['_uk7'] = uk7
                dd['_uk8'] = uk8

            elif cmd==129:
                dd['name'] = 'error129'

            elif cmd==131:  # 0x83
                dd['name'] = 'emeter'
                uk1, i, uk3, t, uk5, uk6 = struct.unpack('>HHHHHH', p[10:22])
                dd['0/currentAC'] = i/100
                dd['0/temperature'] = t/10
                dd['_uk1'] = uk1
                dd['_uk3'] = uk3
                dd['_uk5'] = uk5
                dd['_uk6'] = uk6

            elif cmd==132:  # 0x84
                dd['name'] = 'uk0x84'
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack(
                    '>HHHHHHHH', p[10:26])
                
                dd['name'] = 'error132'
                if debugMode:
                    print(f'uk1={uk1}, ', end='')
                    print(f'uk2={uk2}, ', end='')
                    print(f'uk3={uk3}, ', end='')
                    print(f'uk4={uk4}, ', end='')
                    print(f'uk5={uk5}, ', end='')
                    print(f'uk6={uk6}, ', end='')
                    print(f'uk7={uk7}, ', end='')
                    print(f'uk8={uk8}')            
                    
                dd['_uk1'] = uk1
                dd['_uk2'] = uk2
                dd['_uk3'] = uk3
                dd['_uk4'] = uk4
                dd['_uk5'] = uk5
                dd['_uk6'] = uk6
                dd['_uk7'] = uk7
                dd['_uk9'] = uk8

            else:
                print(f'unknown cmd {cmd}')
                        
                
        elif invType=="HM-300":
            d["infos"]=[dd]
            if cmd==1:                  
                uk0, u1, i1, p1, ptotal, pday, u  = struct.unpack('>HHHHLHH', p[10:26])
                dd['name'] = 'emeter-dc'    
                dd['1/voltage'] = u1/10
                dd['1/current'] = i1/100
                dd['1/power'] = p1/10
                dd['1/totalenergy'] = ptotal
                dd['1/todaysenergy'] = pday
                
                dd={}
                d["infos"].append(dd)
                dd['name'] = 'emeter'    
                dd['0/voltageAC'] = u/10             
            
            elif cmd==2:  # 0x02
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack('>HHHHHHHH', p[10:26])
                                    
                dd['_uk1'] = uk1          
                dd['_uk2'] = uk2    
                dd['_uk3'] = uk3    
                dd['_uk4'] = uk4    
                dd['_uk5'] = uk5    
                dd['_uk6'] = uk6    
                dd['_uk7'] = uk7    
                dd['_uk9'] = uk8    
            
            elif cmd==3:  # 0x03
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack('>HHHHHHHH', p[10:26])
                                    
                dd['_uk1'] = uk1          
                dd['_uk2'] = uk2    
                dd['_uk3'] = uk3    
                dd['_uk4'] = uk4    
                dd['_uk5'] = uk5    
                dd['_uk6'] = uk6    
                dd['_uk7'] = uk7    
                dd['_uk9'] = uk8 
            
            elif cmd==4:  # 0x04
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack('>HHHHHHHH', p[10:26])
                                    
                dd['_uk1'] = uk1          
                dd['_uk2'] = uk2    
                dd['_uk3'] = uk3    
                dd['_uk4'] = uk4    
                dd['_uk5'] = uk5    
                dd['_uk6'] = uk6    
                dd['_uk7'] = uk7    
                dd['_uk9'] = uk8 
            
            elif cmd==5:  # 0x05
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack('>HHHHHHHH', p[10:26])
                                    
                dd['_uk1'] = uk1          
                dd['_uk2'] = uk2    
                dd['_uk3'] = uk3    
                dd['_uk4'] = uk4    
                dd['_uk5'] = uk5    
                dd['_uk6'] = uk6    
                dd['_uk7'] = uk7    
                dd['_uk9'] = uk8 
            
            elif cmd==6:  # 0x06
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack('>HHHHHHHH', p[10:26])
                                    
                dd['_uk1'] = uk1          
                dd['_uk2'] = uk2    
                dd['_uk3'] = uk3    
                dd['_uk4'] = uk4    
                dd['_uk5'] = uk5    
                dd['_uk6'] = uk6    
                dd['_uk7'] = uk7    
                dd['_uk9'] = uk8 
            
            elif cmd==7:  # 0x07
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack('>HHHHHHHH', p[10:26])
                                    
                dd['_uk1'] = uk1          
                dd['_uk2'] = uk2    
                dd['_uk3'] = uk3    
                dd['_uk4'] = uk4    
                dd['_uk5'] = uk5    
                dd['_uk6'] = uk6    
                dd['_uk7'] = uk7    
                dd['_uk9'] = uk8 
            
            elif cmd==129:  # 0x81
                uk1, uk2, uk3, uk4, uk5, uk6, uk7, uk8 = struct.unpack('>HHHHHHHH', p[10:26])
                    
                dd['_uk1'] = uk1          
                dd['_uk2'] = uk2    
                dd['_uk3'] = uk3    
                dd['_uk4'] = uk4    
                dd['_uk5'] = uk5    
                dd['_uk6'] = uk6    
                dd['_uk7'] = uk7    
                dd['_uk9'] = uk8    
            
            elif cmd==130:  # 0x82
                freq, p, uk0, i, uk1, t,  uk2, uk3  = struct.unpack('>HHHHHHHH', p[10:26])
                                   
                dd['name'] = 'emeter'    
                dd['0/frequency'] = freq/100
                dd['0/powerAC'] = p/10
                dd['0/currentAC'] = i/100
                dd['0/temperature'] = t/10
                       
            else:
                print(f'unknown cmd {cmd}')
                        
            
            
    else:
        print(f'unknown frame id {p[0]}')

    # output to stdout
    if debugMode and d:
        print(json.dumps(d))

    # output to MQTT
    if d and d['crc8_valid'] and mqttMode:
        j = json.dumps(d)        
        for info in d["infos"]:            
            if "name" in info:
                if "emeter" in info["name"]:
                    mqtt_client.publish(f"ahoy/{d['fullsrc']}/{info['name']}", "")
                    for key in sorted(info.keys()):
                        if key!="name" and not key.startswith("_"):
                            if debugMode:
                                print("publishing",f'ahoy/{d["fullsrc"]}/{info["name"]}/{key}', info[key])
                            mqtt_client.publish(f'ahoy/{d["fullsrc"]}/{info["name"]}/{key}', info[key])

    if outFile is not None and d and d['crc8_valid']:
        return d
    

def main_loop():
    """
    Keep receiving on channel 3. Every once in a while, transmit a request
    to one of our inverters on channel 40.
    """

    global t_last_tx
    global m_last_tx
    global mType
    global mFullSer
    global mPackets

    m_last_tx={}
    mType={}
    mFullSer={}
    mPackets={}

    global _hm1200_i4
    _hm1200_i4=0
    global _hm1200_p4
    _hm1200_p4=0
    global _hm1200_ptotal2hb
    _hm1200_ptotal2hb=0
    global _hm600_ptotal1hb
    _hm600_ptotal1hb=0

    mLastInv={}
    for inv_ser in l_inv_ser:
        (type,radioKey,nrPakets)=ser_to_type(inv_ser)
        mType[radioKey]=type
        mFullSer[radioKey]=inv_ser
        mPackets[radioKey]=nrPakets
        print_addr(inv_ser)
        mLastInv[inv_ser]=time.monotonic_ns()-1e9*minRefreshSeconds*2
    print_addr(dtu_ser)

    ctr = 1
    last_tx_message = ''

    ts = int(time.time())  # see what happens if we always send one and the same (constant) time!
    
    rx_channels = [3,23,61,75,83]
    #rx_channels = [3,75]  # in case of tx channel 40 this leads to basically no packet loss for me
    rx_channel_id = 0
    rx_channel = rx_channels[rx_channel_id]    
    rx_channel_ack = None
    rx_error = 0

    tx_channels = [40]
    #tx_channels = [3,23,61,75,40]
    tx_channel_id = 0
    tx_channel = tx_channels[tx_channel_id]
    
    secsBetweenInverters=max(3,(15.0/len(l_inv_ser)))
    
    radio.setChannel(rx_channel)
    radio.enableDynamicPayloads()
    radio.setAutoAck(True)
    radio.setRetries(15, 2)
    radio.setPALevel(RF24_PA_LOW)
    #radio.setPALevel(RF24_PA_MAX)
    radio.setDataRate(RF24_250KBPS)
    radio.openReadingPipe(1,ser_to_esb_addr(dtu_ser))

    iInv=-1
    while True:
        iInv=iInv+1
        iInvCheck=iInv % len(l_inv_ser)
        inv_ser=l_inv_ser[iInv % len(l_inv_ser)]
        nowNs=time.monotonic_ns()
        maxAge=0  
        while mLastInv[inv_ser]>nowNs-1e9*minRefreshSeconds:
            maxAge=max(maxAge,nowNs-mLastInv[inv_ser])
            iInv=iInv+1
            inv_ser=l_inv_ser[iInv % len(l_inv_ser)]
            if iInv % len(l_inv_ser)==iInvCheck:
                toWait=(1e9*minRefreshSeconds-maxAge)/1e9
                #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"waiting with radio silence",toWait)
                timeLeft=toWait
                while timeLeft>0:
                    if timeLeft>30:
                        #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"sleeping 30")
                        time.sleep(30)                
                        timeLeft-=30
                    else:
                        #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"sleeping",timeLeft)
                        time.sleep(timeLeft)
                        timeLeft=0
                    HHMM=datetime.now().strftime("%H:%M")
                    if HHMM==endTime:
                        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"shutting down")
                        radio.powerDown()
                        sys.exit()
                break
        mLastInv[inv_ser]=time.monotonic_ns()
        
        radio.flush_rx()
        radio.flush_tx()
        m_buf = {}
        # Sweep receive start channel
        if not rx_channel_ack:
            rx_channel_id = ctr % len(rx_channels)
            rx_channel = rx_channels[rx_channel_id]

        tx_channel_id = tx_channel_id + 1
        if tx_channel_id >= len(tx_channels):
            tx_channel_id = 0
        tx_channel = tx_channels[tx_channel_id]

        # Transmit
        ts = int(time.time())
        payload = compose_0x80_msg(src_ser_no=dtu_ser, dst_ser_no=inv_ser, ts=ts)
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        radio.stopListening()  # put radio in TX mode
        radio.setChannel(tx_channel)
        radio.openWritingPipe(ser_to_esb_addr(inv_ser))
        t_tx_start = time.monotonic_ns()
        tx_status = radio.write(payload)  # will always yield 'True' because auto-ack is disabled
        t_last_tx = t_tx_end = time.monotonic_ns()
        m_last_tx[inv_ser[4:]]=t_last_tx
        if debugMode:
            print (dt,f"Sent:      {ctr:6d}:   channel {tx_channel:2d} to: {inv_ser}")
        radio.setChannel(rx_channel)
        radio.startListening()

        last_tx_message = f"{dt} Transmit {ctr:8d}:   channel {tx_channel:2d} len={len(payload):3d} ack={str(tx_status):7s}   | " + \
            " ".join([f"{b:02x}" for b in payload]) + f" to: {inv_ser}" + "\n"
        ctr = ctr + 1

        # Receive loop
        t_end = time.monotonic_ns()+1e9*1
        receivedPackets=[]
        receiveTries=0
        fakeMissing1=True
        receivingChannels=[]
        receivingOrder=[]
        while time.monotonic_ns() < t_end:
            if len(receivedPackets)>0 and receivedPackets[-1]>0x81 and receiveTries==0:
                for ii in range(1,receivedPackets[-1]-0x80):
                    if receivedPackets[ii-1]!=ii:
                        payload = compose_0x80_msg(src_ser_no=dtu_ser, dst_ser_no=inv_ser, refetch=struct.pack('B', ii+0x80) )
                        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"missing",ii, "sending ",len(payload),"byes on",tx_channel,"is"," ".join([f"{b:02x}" for b in payload]))
                        radio.stopListening()  # put radio in TX mode
                        radio.setChannel(tx_channel)
                        radio.openWritingPipe(ser_to_esb_addr(inv_ser))
                        tx_status = radio.write(payload)
                        m_last_tx[inv_ser[4:]]=t_last_tx=time.monotonic_ns()  # later packets could appear earlier..
                        radio.setChannel(rx_channel)
                        radio.startListening()
                        receiveTries=10
                        break                    
            receiveTries=max(0,receiveTries-1)
            
            if len(receivedPackets)>1 and len(receivedPackets)==receivedPackets[-1]-0x80:
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"all packets received count",len(receivedPackets))
                break
            
            has_payload, pipe_number = radio.available_pipe()
            if has_payload:
                # Data in nRF24 buffer, read it
                rx_error = 0
                rx_channel_ack = rx_channel                
                #t_end = time.monotonic_ns()+6e7
                
                size = radio.getDynamicPayloadSize()
                payload = radio.read(size)
                
                #if size>10 and payload[0]==0x95 and payload[9]==1 and fakeMissing1:
                #    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),"faking receive error")
                #    fakeMissing1=False
                #    continue
               
                # Only print last transmittet message if we got any response
                print(last_tx_message, end='')
                last_tx_message = ''
                if size>=10 and payload[0]==0x95:
                    cmd=payload[9]
                    receivingChannels.append(rx_channel)
                    receivingOrder.append(cmd)
                    m_buf[cmd]={
                        'p': payload,
                        'ch_rx': rx_channel, 'ch_tx': tx_channel,
                        'time_rx': datetime.now(), 'latency': time.monotonic_ns()-t_last_tx}
                    receivedPackets=sorted(m_buf.keys())
                    #print ("got: ",payload[9],"is:"," ".join([f"{b:02x}" for b in payload]))
                    #print (receivedPackets)
                
            else:
                # No data in nRF rx buffer, search and wait
                # Channel lock in (not currently used)
                rx_error = rx_error + 1
                if rx_error > 0:
                    rx_channel_ack = None
                # Channel hopping
                if not rx_channel_ack:
                    rx_channel_id = rx_channel_id + 1
                    if rx_channel_id >= len(rx_channels):
                        rx_channel_id = 0
                    #import random
                    #random.shuffle(rx_channels)
                    #random.shuffle(tx_channels)
                    rx_channel = rx_channels[rx_channel_id]
                    radio.stopListening()
                    radio.setChannel(rx_channel)
                    radio.startListening()
                time.sleep(0.005)

        # Process receive buffer outside time critical receive loop
        mFileInfo={}
        for cmd in sorted(m_buf.keys()):
            param=m_buf[cmd]
            d=on_receive(**param)
            mFileInfo[cmd]=d

        
        if len(m_buf)==mPackets[inv_ser[4:]]:
            header = f"{dt} "+inv_ser+f" channel: {tx_channel:2d} rx: "+",".join([f"{b:02d}" for b in receivingChannels])+" order: "+",".join([f"{b:02x}" for b in receivingOrder])
            lTiming=[]
            mDC={}
            mAC={}
            for cmd in sorted(mFileInfo.keys()):
                d=mFileInfo[cmd]
                responseTime=d['response_time_ns']
                lTiming.append(f"{responseTime:010d}")
                for info in d["infos"]: 
                    if "name" in info:
                        if "emeter" in info["name"]:
                            if "dc" in info["name"]:
                                for key in sorted(info.keys()):
                                    if key!="name" and not key.startswith("_"):
                                        mDC[key]=info[key]
                            else:
                                for key in sorted(info.keys()):
                                    if key!="name" and not key.startswith("_"):
                                        newKey=key.replace("0/","")
                                        mAC[newKey]=info[key]
                        
                    
            header=header+" ts: "+",".join(lTiming)+" "
            
            infoAC=""
            for key in mAC:
                infoAC=infoAC+key+": "+str(mAC[key])+" "
            infoDC=""    
            for key in mDC:
                infoDC=infoDC+key+": "+str(mDC[key])+" "    
                
            message=header+infoAC+infoDC.strip()
            print(message)
            if outFile is not None:
                print(message,file=outFile,flush=True)
                    

        if len(m_buf)==0:
            mLastInv[inv_ser]=time.monotonic_ns()-1e9*minRefreshSeconds/2.0

        # Flush console
        print(flush=True, end='')




if __name__ == "__main__":

    if not radio.begin():
        raise RuntimeError("radio hardware is not responding")

    radio.setPALevel(RF24_PA_LOW)  # RF24_PA_MAX is default

    # radio.printDetails();  # (smaller) function that prints raw register values
    # radio.printPrettyDetails();  # (larger) function that prints human readable data

    try:
        main_loop()

    except KeyboardInterrupt:
        print(" Keyboard Interrupt detected. Exiting...")
        radio.powerDown()
        sys.exit()

