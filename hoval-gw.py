# This file is just a sample file that does:
# 1) Open CAN bus can0 on the device (for example a raspberry PI using socketcan)
# 2) Listening for simple message coming from Hoval heater TTE-WEZ
# 3) Publishing each recognized data points to a MQTT broker located at IP 192.168.0.96 with username/password hoval/hoval

import can
import asyncio
import logging
import paho.mqtt.client as mqtt

# Change this to match your Home-Assistant / MQTT broker
broker = '192.168.0.96'
broker_username = "hoval"
broker_password = "hoval"

# This is extracted from http://www.hoval.com/misc/TTE/TTE-GW-Modbus-datapoints.xlsx
# (X,Y,Z) means X: Function group, Y: Function number, Z: Datapoint ID
data_idx = {
    (0,0,0): ("AF 1 - sonde extérieure 1","S16",1),
    (1,0,3050): ("Sélection fonctionnement chauffage","LIST",0),
    (1,0,3050): ("Sélection fonctionnement chauffage","LIST",0),
    (1,1,3050): ("Sélection fonctionnement chauffage","LIST",0),
    (1,2,3050): ("Sélection fonctionnement chauffage","LIST",0),
    (1,0,3051): ("température local normale mode chauffage","S16",1),
    (1,0,3053): ("température local éco mode chauffage","S16",1),
    (1,1,3051): ("température local normale mode chauffage","S16",1),
    (1,1,3053): ("température local éco mode chauffage","S16",1),
    (1,2,3051): ("température local normale mode chauffage","S16",1),
    (1,2,3053): ("température local éco mode chauffage","S16",1),
    (1,0,7009): ("temp. consigne comm. man.","S16",1),
    (1,1,7009): ("temp. consigne comm. man.","S16",1),
    (1,2,7009): ("temp. consigne comm. man.","S16",1),
    (1,0,7036): ("Dép. consigne demande const. chauffer","S16",1),
    (1,1,7036): ("Dép. consigne demande const. chauffer","S16",1),
    (1,2,7036): ("Dép. consigne demande const. chauffer","S16",1),
    (1,0,1001): ("Valeur de consigne pièce","S16",1),
    (1,1,1001): ("Valeur de consigne pièce","S16",1),
    (1,2,1001): ("Valeur de consigne pièce","S16",1),
    (2,0,5050): ("Sélection fonct. eau chaude","LIST",0),
    (2,0,5051): ("température d'eau chaude normale","S16",1),
    (2,0,5086): ("température eau chaude éco","U8",0),
    (2,0,1004): ("Eau chaude consigne","S16",1),
    (2,0,4): ("Eau chaude réelle SF","S16",1),
    (1,0,2051): ("Statut régulation circuit de chauffage","U8",0),
    (1,1,2051): ("Statut régulation circuit de chauffage","U8",0),
    (1,2,2051): ("Statut régulation circuit de chauffage","U8",0),
    (2,0,2052): ("Statut régulation eau chaude","U8",0),
    (10,1,29050): ("Quantité de chaleur chauffage_high","U32",3),
    (10,1,29050): ("Quantité de chaleur chauffage_low","U32",3),
    (10,1,2081): ("Heures de fonct. générateur de chaleur_high","U32",0),
    (10,1,2081): ("Heures de fonct. générateur de chaleur_low","U32",0),
    (0,0,21100): ("AF 2 - sonde extérieure 2","S16",1),
    (1,0,1): ("Valeur réelle pièce","S16",1),
    (1,1,1): ("Valeur réelle pièce","S16",1),
    (1,2,1): ("Valeur réelle pièce","S16",1),
    (1,0,2): ("Aller réel","S16",1),
    (1,1,2): ("Aller réel","S16",1),
    (1,2,2): ("Aller réel","S16",1),
    (10,1,2081): ("Heures de fonct. générateur de chaleur_high","U32",0),
    (10,1,2081): ("Heures de fonct. générateur de chaleur_low","U32",0),
    (10,1,2080): ("Cycles commutation générateur de chaleur_high","U32",0),
    (10,1,2080): ("Cycles commutation générateur de chaleur_low","U32",0),
    (60,254,0): ("Valeur cons. pour fonct. circ. chaleur","S16",1),
    (60,254,1): ("Valeur cons. pour fonct. acc.","S16",1),
    (60,254,8): ("Limitation de puissance","S16",1),
    (60,254,9): ("Mesure des émissions","U8",0),
    (60,254,12): ("Valeur cons. pour mode refroidissement","S16",1),
    (60,254,17): ("Température GDC","S16",1),
    (60,254,18): ("Température gaz comb. RS485","S16",1),
    (60,254,19): ("change Température active / passive","S16",1),
    (60,254,21): ("Flamme brûleur","U8",0),
    (60,254,22): ("Valeur de consigne chauffage","S16",1),
    (60,254,23): ("Valeur de consigne acc.","S16",1),
    (60,254,24): ("Val. de consigne GDC","S16",1),
    (60,254,25): ("Limitation maximale ext. chauffage","S16",1),
    (60,254,26): ("Limitation maximale ext. acc.","S16",1),
    (60,254,27): ("Code erreur automate","U8",0),
    (60,254,29): ("Température de retour","S16",1),
    (60,254,30): ("Puissance GDC","U8",0),
    (60,254,31): ("Puissance absolue","U8",0),
    (60,254,32): ("Pression hydraulique en bars","S16",1),
    (60,254,33): ("Statut GDC","U8",0),
    (60,254,34): ("Statut de service","U8",0),
    (60,254,35): ("Index ID brûleur","U8",0),
    (60,254,36): ("Extensions de fonctions","U8",0),
    (60,254,37): ("Information statut","U8",0),
    (60,254,38): ("Valeur de consigne refroid.","S16",1),
    (0,0,23084): ("Activer le test de relais","U8",0),
    (0,0,21031): ("CM1 sortie HW","U8",0),
    (0,0,21032): ("YK1+ sortie HW","U8",0),
    (0,0,21033): ("YK1- sortie HW","U8",0),
    (0,0,21034): ("DKP sortie HW","U8",0),
    (0,0,21035): ("SLP sortie HW","U8",0),
    (0,0,21036): ("VA1 sortie HW","U8",0),
    (0,0,21037): ("VA2 sortie HW","U8",0),
    (0,0,21039): ("VA1-FE1 Sortie HW","U8",0),
    (0,0,21040): ("VA2-FE1 Sortie HW","U8",0),
    (0,0,21041): ("VA3-FE1 Sortie HW","U8",0),
    (0,0,21043): ("VA1-FE2 Sortie HW","U8",0),
    (0,0,21044): ("VA2-FE2 Sortie HW","U8",0),
    (0,0,21045): ("VA3-FE2 Sortie HW","U8",0),
    (0,0,21078): ("VA0-10V/PWM sortie HW","U8",0),
    (0,0,21079): ("VA0-10V/PWM-FE1 sortie HW","U8",0),
    (10,1,9075): ("Sélection fonct. générateur de chaleur","LIST",0),
    (10,1,9020): ("Temp. consigne comm. man.","S16",1),
    (10,1,10110): ("Test d'émission limitation de puissance","U8",0),
    (10,1,23085): ("Activer test d'émission","U8",0),
    (0,0,29042): ("Erreur active 1_appearance_time","U16",0),
    (0,0,29042): ("Erreur active 1_appearance_date","U16",0),
    (0,0,29042): ("Erreur active 1_disappear_time","U16",0),
    (0,0,29042): ("Erreur active 1_disappear_date","U16",0),
    (0,0,29042): ("Erreur active 1_source","U16",0),
    (0,0,29042): ("Erreur active 1_function_group","U8",0),
    (0,0,29042): ("Erreur active 1_function_number","U8",0),
    (0,0,29042): ("Erreur active 1_error_type","U8",0),
    (0,0,29042): ("Erreur active 1_error_code","U16",0),
    (0,0,29043): ("Erreur active 2_appearance_time","U16",0),
    (0,0,29043): ("Erreur active 2_appearance_date","U16",0),
    (0,0,29043): ("Erreur active 2_disappear_time","U16",0),
    (0,0,29043): ("Erreur active 2_disappear_date","U16",0),
    (0,0,29043): ("Erreur active 2_source","U16",0),
    (0,0,29043): ("Erreur active 2_function_group","U8",0),
    (0,0,29043): ("Erreur active 2_function_number","U8",0),
    (0,0,29043): ("Erreur active 2_error_type","U8",0),
    (0,0,29043): ("Erreur active 2_error_code","U16",0),
    (0,0,29044): ("Erreur active 3_appearance_time","U16",0),
    (0,0,29044): ("Erreur active 3_appearance_date","U16",0),
    (0,0,29044): ("Erreur active 3_disappear_time","U16",0),
    (0,0,29044): ("Erreur active 3_disappear_date","U16",0),
    (0,0,29044): ("Erreur active 3_source","U16",0),
    (0,0,29044): ("Erreur active 3_function_group","U8",0),
    (0,0,29044): ("Erreur active 3_function_number","U8",0),
    (0,0,29044): ("Erreur active 3_error_type","U8",0),
    (0,0,29044): ("Erreur active 3_error_code","U16",0),
    (0,0,29045): ("Erreur active 4_appearance_time","U16",0),
    (0,0,29045): ("Erreur active 4_appearance_date","U16",0),
    (0,0,29045): ("Erreur active 4_disappear_time","U16",0),
    (0,0,29045): ("Erreur active 4_disappear_date","U16",0),
    (0,0,29045): ("Erreur active 4_source","U16",0),
    (0,0,29045): ("Erreur active 4_function_group","U8",0),
    (0,0,29045): ("Erreur active 4_function_number","U8",0),
    (0,0,29045): ("Erreur active 4_error_type","U8",0),
    (0,0,29045): ("Erreur active 4_error_code","U16",0),
    (0,0,29046): ("Erreur active 5_appearance_time","U16",0),
    (0,0,29046): ("Erreur active 5_appearance_date","U16",0),
    (0,0,29046): ("Erreur active 5_disappear_time","U16",0),
    (0,0,29046): ("Erreur active 5_disappear_date","U16",0),
    (0,0,29046): ("Erreur active 5_source","U16",0),
    (0,0,29046): ("Erreur active 5_function_group","U8",0),
    (0,0,29046): ("Erreur active 5_function_number","U8",0),
    (0,0,29046): ("Erreur active 5_error_type","U8",0),
    (0,0,29046): ("Erreur active 5_error_code","U16",0),
    (10,1,2053): ("Statut régulation générateur de chaleur","U8",0),
    (10,1,20051): ("Statut FA","U8",0),
    (10,1,1007): ("Générateur de chaleur consigne","S16",0),
    (10,1,7): ("Générateur de chaleur réel","S16",1),
    (10,1,20052): ("Modulation","U8",0),
    (10,1,2082): ("Heures de fonct. du gén. chaleur > 50%_high","U32",0),
    (10,1,2082): ("Heures de fonct. du gén. chaleur > 50%_low","U32",0),
    (10,1,2083): ("Cycles comm. générateur chaleur > 50%_high","U32",0),
    (10,1,2083): ("Cycles comm. générateur chaleur > 50%_low","U32",0),
    (10,1,29051): ("Puissance actuelle chauffage_high","U32",1),
    (10,1,29051): ("Puissance actuelle chauffage_low","U32",1),
    (10,1,29052): ("Quant. refroid. Froid_high","U32",3),
    (10,1,29052): ("Quant. refroid. Froid_low","U32",3),
    (10,1,29053): ("Puissance actuelle refroid._high","U32",1),
    (10,1,29053): ("Puissance actuelle refroid._low","U32",1),
    (10,1,20053): ("Signalisation de marche","U8",0),
    (10,1,20050): ("Pression hydraulique","S16",1),
    (10,1,1022): ("Pompe générateur de chaleur","U8",0),
    (10,1,22): ("Vitesse rot. Pompe principale","U8",0),
    (10,1,21105): ("Débit volumique","U16",2),
    (10,1,8): ("Température retour générateur de chaleur","S16",1),
    (10,0,2053): ("Statut régulation générateur de chaleur","U8",0),
    (10,0,1007): ("Générateur de chaleur consigne","S16",1),
    (10,0,1009): ("Valeur de consigne puiss. gén. chaleur","U8",0),
    (10,0,7): ("Générateur de chaleur réel","S16",1),
    (10,0,2081): ("Heures de fonct. générateur de chaleur_high","U32",0),
    (10,0,2081): ("Heures de fonct. générateur de chaleur_low","U32",0),
    (10,0,2080): ("Cycles commutation générateur de chaleur_high","U32",0),
    (10,0,2080): ("Cycles commutation générateur de chaleur_low","U32",0),
    (10,0,1100): ("Allure brûleur","U8",0),
    (10,0,29050): ("Quantité de chaleur chauffage_high","U32",3),
    (10,0,29050): ("Quantité de chaleur chauffage_low","U32",3),
    (10,0,29051): ("Puissance actuelle chauffage_high","U32",1),
    (10,0,29051): ("Puissance actuelle chauffage_low","U32",1),
    (10,0,21105): ("Débit volumique","U16",2),
    (10,0,1022): ("Pompe générateur de chaleur","U8",0),
    (10,0,22): ("Vitesse rot. Pompe principale","U8",0),
    (10,0,8): ("Température retour générateur de chaleur","S16",1),
    (3,0,96): ("Température départ install.","S16",1),
    (3,0,1096): ("Temp. installation cons. chauffage act.","S16",1),
    (3,0,1097): ("Température d'installation cons. EC act.","S8",0),
    (3,0,22098): ("Temp. installation cons. refroid. act.","S16",0),
    (3,0,2040): ("Puissance inst. cons. chauffage act.","S16",1),
    (3,0,2041): ("Puissance inst. cons. EC act.","S16",1),
    (3,0,2042): ("Puissance inst. cons. refroid. act.","S16",1),
    (4,0,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,0,2043): ("0 - 100% demande act. au GDC","U8",0),
    (4,1,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,1,2043): ("0 - 100% demande act. au GDC","U8",0),
    (4,2,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,2,2043): ("0 - 100% demande act. au GDC","U8",0),
    (4,3,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,3,2043): ("0 - 100% demande act. au GDC","U8",0),
    (4,4,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,4,2043): ("0 - 100% demande act. au GDC","U8",0),
    (4,5,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,5,2043): ("0 - 100% demande act. au GDC","U8",0),
    (4,6,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,6,2043): ("0 - 100% demande act. au GDC","U8",0),
    (4,7,1009): ("Valeur de consigne puiss. gén. chaleur","S16",1),
    (4,7,2043): ("0 - 100% demande act. au GDC","U8",0),
    (0,0,21120): ("Sonde info 1","S16",1),
    (0,0,21121): ("Sonde info 2","S16",1),
    (0,0,21122): ("Sonde info 3","S16",1),
    (0,0,21123): ("Sonde info 4","S16",1),
    (0,0,21124): ("Sonde info 5","S16",1),
    (1,0,7047): ("Dép. consigne demande const. refroid.","S16",1),
    (10,0,9058): ("Valeur consigne puiss. commande manuelle","U8",0),
    (10,1,9058): ("Valeur consigne puiss. commande manuelle","U8",0),
    (10,0,9075): ("Sélection fonct. générateur de chaleur","LIST",0),
    (10,0,9020): ("Temp. consigne comm. man.","S16",1),
    (1,0,1002): ("Aller consigne","S16",1),
    (1,1,1002): ("Aller consigne","S16",1),
    (1,2,1002): ("Aller consigne","S16",1),
    (1,0,3): ("Retour réel","S16",1),
    (1,1,3): ("Retour réel","S16",1),
    (1,2,3): ("Retour réel","S16",1),
    (1,0,1021): ("Mélangeur","S8",0),
    (1,1,1021): ("Mélangeur","S8",0),
    (1,2,1021): ("Mélangeur","S8",0),
    (1,0,1020): ("Pompe","U8",0),
    (1,1,1020): ("Pompe","U8",0),
    (1,2,1020): ("Pompe","U8",0),
    (2,0,1066): ("SLP pompe de charge eau chaude","U8",0),
    (2,0,118): ("température circuit de circulation","U16",1),
    (2,0,1065): ("Pompe de circulation eau chaude","U8",0),
    (0,0,22021): ("Température de consigne de départ FAV","S16",1),
    (0,0,21059): ("Temp. départ réelle AVF","S16",1),
    (0,0,22055): ("Pompe AVP","U8",0),
    (0,0,22056): ("Mélangeur YAV","S8",0),
    (3,0,97): ("Température départ install. eau chaude","S16",1),
    (3,0,21089): ("Température départ install. refroid","S16",1),
    (3,0,22024): ("Statut froid module activ UKA","U8",0),
    (3,0,22121): ("Statut froid module UHKA","U8",0),
    (4,0,2119): ("Générateur de chaleur principal","U8",0),
    (4,0,6020): ("Changement de séquence (temps)","U8",0),
    (10,1,9037): ("Surélévation temp. GDC","S16",1),
    (10,0,9037): ("Surélévation temp. GDC","S16",1),
    (1,1,7047): ("Dép. consigne demande const. refroid.","S16",1),
    (1,2,7047): ("Dép. consigne demande const. refroid.","S16",1),
    (0,0,21125): ("Info 1 0-10V","S16",1),
    (0,0,21126): ("Info 2 0-10V","S16",1),
    (0,0,21127): ("Info 3 0-10V","S16",1),
}

def convert_data(arr, msg):
    if msg[1] == 'U8' or msg[1] == 'U16' or msg[1] == 'U32':
        val = int.from_bytes(arr, byteorder='big', signed=False)
        return val * 10**(-msg[2])
    elif msg[1] == 'S8' or msg[1] == 'S16' or msg[1] == 'S32':
        val = int.from_bytes(arr, byteorder='big', signed=True)
        return val * 10**(-msg[2])
    elif  msg[1] == 'LIST':
        val = int.from_bytes(arr, byteorder='big', signed=False)
        return val
    else:
        print('Unknown ', msg[1])
        return None

def parse(msg):
    # For now we only care about ID 1FC00FFF
    if msg.arbitration_id == 0x1FC00FFF and len(msg.data) >= 7:
        id1 = msg.data[0]
        id2 = msg.data[1]
        # 42 looks like an answer, 40 is a request..
        if id1 == 0x01 and id2 == 0x42:
            # Data point
            function_group = msg.data[2]
            function_number = msg.data[3]
            datapoint = int.from_bytes(msg.data[4:6], byteorder='big', signed=False)
            idp = (function_group, function_number, datapoint)
            if idp in data_idx:
                point = data_idx[idp]
                out = convert_data(msg.data[6:], point)
                if out:
                    return (point[0], out)
    else:
        pass # skip
    return None

async def main():
    can0 = can.Bus(channel='can0', bustype='socketcan', receive_own_messages=False)
    reader = can.AsyncBufferedReader()
    #logger = can.Logger('logfile.log')

    listeners = [
        reader,         # AsyncBufferedReader() listener
        #logger          # Regular Listener object
    ]
    # Create Notifier with an explicit loop to use for scheduling of callbacks
    loop = asyncio.get_event_loop()
    notifier = can.Notifier(can0, listeners, loop=loop)

    client = mqtt.Client("hoval-client")
    client.username_pw_set(username=broker_username,password=broker_password)
    client.connect(broker)

    while True:
        # Wait for next message from AsyncBufferedReader
        msg = await reader.get_message()
        parsed = parse(msg)
        if parsed:
            client.publish("hoval-gw/"+parsed[0], parsed[1])

    # Clean-up
    notifier.stop()
    can0.shutdown()

# Get the default event loop
loop = asyncio.get_event_loop()
# Run until main coroutine finishes
loop.run_until_complete(main())
loop.close()