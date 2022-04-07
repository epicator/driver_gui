import paho.mqtt.client as pm
import pandas as pd
import time
import sys

try:
    # ---------------------|Start of MQTT Subscription|--------------------- #
    #
    #
    #

    # Packet ID:
    # Battery = 0
    # Speed = 1
    # Accel = 2


    def connection_test(client, userdata, flags, rc):
        if rc == 0:
            ConnectionFlag = True
            print('Connected successfully')
        else:
            print('Unable to connect to Broker, Returned code was =', rc)
            ConnectionFlag = False
            sys.exit(1)


    def data_logger(client, userdata, level, buf):
        print('Log: ' + buf)


    Shared_Battery = 0
    Shared_Speed = 0
    Shared_Accel = 0


    def the_message(client, userdata, message):
        global Shared_Battery
        global Shared_Speed
        global Shared_Accel
        global Shared_Array
        value = str(message.payload.decode("utf-8"))
        if message.topic == "BatteryLevel":
            Shared_Battery = value
            print('The battery level is ', Shared_Battery)
        elif message.topic == "SpeedLevel":
            Shared_Speed = value
            print('The speed level is ', Shared_Speed)
        elif message.topic == "AccelLevel":
            Shared_Accel = value
            print('The accel level is ', Shared_Accel)
        Shared_Array = [
            int(Shared_Battery),
            int(Shared_Speed),
            int(Shared_Accel)
        ]
        print('The Array currently is ', Shared_Array)
        df = pd.DataFrame([[Shared_Array[0], Shared_Array[1], Shared_Array[2]]],
                          columns=['BatteryLevel', 'SpeedLevel', 'AccelLevel'])
        df.to_csv('Secondary_Backup.csv', mode='a', index_label='log', index=True)

        print(df)


    ConnectionFlag = False
    Broker = 'localhost'
    RPSubscriber = pm.Client('RaspberryPi')
    try:
        RPSubscriber.connect(Broker)
    except:
        print('Unable to find the Broker, please check address')
        sys.exit()

    print('connecting to the Broker')
    RPSubscriber.subscribe([('BatteryLevel', 0), ('SpeedLevel', 1), ('AccelLevel', 2)])
    RPSubscriber.on_connect = connection_test
    # RPSubscriber.on_log = data_logger
    RPSubscriber.on_message = the_message

    if not RPSubscriber.is_connected():
        RPSubscriber.loop_forever()
    else:
        RPSubscriber.disconnect()
        sys.exit(1)
    #
    #
    #
    # ---------------------|End of MQTT Subscription|--------------------- #

except KeyboardInterrupt:
    print('KeyPress detected, closing')