import requests
import time
import logging
import json
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime


# froniusdatastream.py
# This will connect to the Fronius inverter and SmartMeter and will
# log data to an InfluxDB database.
# Make sure you leave the "" around the connection details variables!
# It assumes you have an InfluxDB running with a bucket named "SiteBucket"
# and one named "MeterBucket". You can of cource create whatever buckets you
# like, but just make sure you change the relevant variables below.

#Connection details
hostname = "inverter_IP_address"
Influx_url = "http://influxdb_IP_address:8086"
Influx_token = "influxDB_API_Key"
Influx_org = "influxDB_Organisation"
Influx_site_bucket = "SiteBucket"
Influx_meter_bucket = "MeterBucket"


def getData(hostname,dataRequest):
    """
    All Request's come via this function.  It builds the url from args
    hostname and dataRequest.  It is advised to have a fronius hostname
    entry in /etc/hosts.  There is no authentication required, it is assumed
    you are on a local, private network.
    """
    try:
        url = "http://" + hostname + dataRequest
        r = requests.get(url,timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.Timeout:
        print("Request: {} failed ".format(url))
    except requests.exceptions.RequestException as e:
        print("Request failed with {}".format(e))

    exit()

def GetPowerFlowRealtimeData():
    """
    This request provides detailed information about the local energy grid.
    The values replied represent the current state. Because of data has multiple
    asynchrone origins it is a matter of facts that the sum of all
    powers (grid, load and generate) will differ from zero.
    """
    dataRq = '/solar_api/v1/GetPowerFlowRealtimeData.fcgi'
    return getData(hostname,dataRq)

def GetMetersRealtimeData():
    """
    This request provides detailed information about the local energy grid from the meter.
    The values replied represent the current state. Because of data has multiple
    asynchrone origins it is a matter of facts that the sum of all
    powers (grid, load and generate) will differ from zero.
    """
    dataRq = '/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System'
    return getData(hostname,dataRq)



def PowerFlowRealtimeData(jPFRD):
# Collect the Inverter and Site Data(convert to single row)
    Inverters = dict()
    Site = dict()
# There could be more than 1 Inverter
    for i in jPFRD['Body']['Data']['Inverters']:
        Inverters['DeviceId'] = i
        Inverters['DT'] = jPFRD['Body']['Data']['Inverters'][i]['DT']
        Inverters['P'] = jPFRD['Body']['Data']['Inverters'][i]['P']
        Site['Timestamp'] = jPFRD['Head']['Timestamp']
        Site['Version'] = jPFRD['Body']['Data']['Version']
        Site['E_Day'] = jPFRD['Body']['Data']['Site']['E_Day']
        Site['E_Total'] = jPFRD['Body']['Data']['Site']['E_Total']
        Site['E_Year'] = jPFRD['Body']['Data']['Site']['E_Year']
        Site['Meter_Location'] = jPFRD['Body']['Data']['Site']['Meter_Location']
        Site['Mode'] = jPFRD['Body']['Data']['Site']['Mode']
        Site['P_Akku'] = jPFRD['Body']['Data']['Site']['P_Akku']
# TODO: Make Site(P_Akku) not 'None' 
        Site['P_Grid'] = jPFRD['Body']['Data']['Site']['P_Grid']
        Site['P_Load'] = jPFRD['Body']['Data']['Site']['P_Load']
        Site['P_PV'] = jPFRD['Body']['Data']['Site']['P_PV']
        Site['rel_Autonomy'] = jPFRD['Body']['Data']['Site']['rel_Autonomy']
        Site['rel_SelfConsumption'] = jPFRD['Body']['Data']['Site']['rel_SelfConsumption']
    return [Site, Inverters]


def MetersRealtimeData(jPFRD):
# Collect the Single Phase SmartMeter Data (convert to single row)
    Meters = dict()
# There could be more than 1 SmartMeter
    for i in jPFRD['Body']['Data']:
        Meters['Timestamp'] = jPFRD['Head']['Timestamp']
        Meters['DeviceId'] = i        
        Meters['Current_L1'] = jPFRD['Body']['Data'][i]['Current_AC_Phase_1']
        Meters['Voltage_L1'] = jPFRD['Body']['Data'][i]['Voltage_AC_Phase_1']
        Meters['Manufacturer'] = jPFRD['Body']['Data'][i]['Details']['Manufacturer']
        Meters['Model'] = jPFRD['Body']['Data'][i]['Details']['Model']
        Meters['Serial'] = jPFRD['Body']['Data'][i]['Details']['Serial']
        Meters['Grid_Frequency'] = jPFRD['Body']['Data'][i]['Frequency_Phase_Average']
        Meters['EnergyActiveMinus'] = jPFRD['Body']['Data'][i]['EnergyReal_WAC_Minus_Absolute']
        Meters['EnergyActivePlus'] = jPFRD['Body']['Data'][i]['EnergyReal_WAC_Plus_Absolute']
        Meters['EnergyActiveConsumed'] = jPFRD['Body']['Data'][i]['EnergyReal_WAC_Phase_1_Consumed']
        Meters['EnergyActiveProduced'] = jPFRD['Body']['Data'][i]['EnergyReal_WAC_Phase_1_Produced']
        Meters['EnergyReActiveConsumed'] = jPFRD['Body']['Data'][i]['EnergyReactive_VArAC_Phase_1_Consumed']
        Meters['EnergyReActiveProduced'] = jPFRD['Body']['Data'][i]['EnergyReactive_VArAC_Phase_1_Produced']
        Meters['PowerFactorL1'] = jPFRD['Body']['Data'][i]['PowerFactor_Phase_1']
        Meters['PowerApparentL1'] = jPFRD['Body']['Data'][i]['PowerApparent_S_Phase_1']
        Meters['PowerReActiveL1'] = jPFRD['Body']['Data'][i]['PowerReactive_Q_Phase_1']
        Meters['PowerReal_L1'] = jPFRD['Body']['Data'][i]['PowerReal_P_Phase_1']
    return [Meters]

        
def main():
    client = InfluxDBClient(url=Influx_url, token=Influx_token, org=Influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    while True:
        try:
            Site, Inverters = PowerFlowRealtimeData(GetPowerFlowRealtimeData())
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            write_api.write(Influx_site_bucket, Influx_org, 
                [{
                "measurement": "SiteValues", 
                "tags": {"location": "home", "Version": Site['Version']}, 
                "fields": 
                    {
                    "P_Akku": Site['P_Akku'], 
                    "P_Grid": Site['P_Grid'], 
                    "P_PV": Site['P_PV'], 
                    "P_Load": Site['P_Load'],
                    "rel_Autonomy": Site['rel_Autonomy'],
                    "rel_SelfConsumption": Site['rel_SelfConsumption']
                    }, 
                "time": str(now)}
                ])
            print ("SiteBucket Upload Successful!")
            time.sleep(2)

            Meters = MetersRealtimeData(GetMetersRealtimeData())
            for i in range(len(Meters)):
                now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                write_api.write(Influx_meter_bucket, Influx_org, 
                    [{
                    "measurement": "MeterValues", 
                    "tags": 
                        {
                        "location": "home", 
                        "MeterManufacturer": Meters[i]['Manufacturer'], 
                        "MeterModel": Meters[i]['Model'], 
                        "MeterSerial": Meters[i]['Serial']
                        }, 
                    "fields": 
                        {
                        "Current_L1": Meters[i]['Current_L1'], 
                        "Grid_Frequency": Meters[i]['Grid_Frequency'], 
                        "EnergyActiveMinus": Meters[i]['EnergyActiveMinus'], 
                        "EnergyActivePlus": Meters[i]['EnergyActivePlus'], 
                        "EnergyActiveConsumed": Meters[i]['EnergyActiveConsumed'], 
                        "EnergyActiveProduced": Meters[i]['EnergyActiveProduced'], 
                        "EnergyReActiveConsumed": Meters[i]['EnergyReActiveConsumed'], 
                        "EnergyReActiveProduced": Meters[i]['EnergyReActiveProduced'], 
                        "PowerFactorL1": Meters[i]['PowerFactorL1'], 
                        "PowerReal_L1": Meters[i]['PowerReal_L1'], 
                        "PowerApparentL1": Meters[i]['PowerApparentL1'], 
                        "PowerReActiveL1": Meters[i]['PowerReActiveL1'], 
                        "Voltage_L1": Meters[i]['Voltage_L1'] 
                        }, 
                    "time": str(now)}
                    ])
                print ("MeterBucket Upload Successful!")
            print ("Next upload in 3 seconds")    
            time.sleep(3)
        except:
            time.sleep(15)
            print("Exception Thrown - Sleeping for 15 seconds before retrying")


if __name__ == "__main__":
    main()