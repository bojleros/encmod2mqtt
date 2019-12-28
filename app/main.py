#!/usr/bin/python3

import serial
import time
import sys
import os
import json
import signal
import threading
from datetime import datetime
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt


def msg(text):
    print("%s : %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), text))

class SignalHandler:
    signum = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.signum = signum

def getenv():
    conf = {
        'serial': {
            'port': os.environ.get('UART_PORT', '/dev/ttyUSB0'),
            'baudrate': int(os.environ.get('UART_BAUD', 115200)),
            "bytesize": int(os.environ.get('UART_BYTESIZE', 8)),
            "parity": os.environ.get('UART_PARITY', "N"),
            "stopbits": int(os.environ.get('UART_STOPBITS', 1)),
            "timeout": float(os.environ.get('UART_TIMEOUT', 0.2))
        },
        'mqtt': {
            'address': os.environ.get('MQTT_ADDR', '127.0.0.1'),
            'port': int(os.environ.get('MQTT_PORT', 1883)),
            'username': os.environ.get('MQTT_USER', None),
            'password': os.environ.get('MQTT_PASS', None),
            'qos': int(os.environ.get('MQTT_QOS', 1)),
            'command_topics': {
                '0': os.environ.get('CH0_COMMAND', ''),
                '1': os.environ.get('CH1_COMMAND', ''),
                '2': os.environ.get('CH2_COMMAND', ''),
                '3': os.environ.get('CHTG_COMMAND', '')
            },
            'state_topics': {
                '0': os.environ.get('CH0_STATE', ''),
                '1': os.environ.get('CH1_STATE', ''),
                '2': os.environ.get('CH2_STATE', ''),
                '3': os.environ.get('CH2_STATE', '')
            }
        }
    }

    return conf


class Encmod:

    def __init__(self, serconf):
        """
        Method that initializes variables
        """
        self.serconf = serconf
        self.reinit_count = 3
        self.ser = False
        self.events = [{}, {}]
        self.event = False
        self.init_ts = datetime.now().timestamp()
        self.last_button_push_ts = self.init_ts
        self.last_button_release_ts = self.init_ts
        self.last_change_ts = self.init_ts

    def __del__(self):
        """
        Method for closing encmod serial line connection
        """
        if bool(self.ser):
            msg("Closing serial line")
            self.ser.close()
            del self.ser

    def connect(self):
        """
        Method that establishes connection with encmods on the other end of a
        serial line. By default it has 3 retries.
        """
        i = self.reinit_count
        while i > 0:
            try:
                self.ser = serial.Serial(**self.serconf)
            except Exception as e:
                msg("Serial init attempt #%s failed" % i)
                time.sleep(0.2)
                i -= 1
                if i == 0:
                    raise e
            else:
                msg("Serial line opened")
                i = 0

    def get_new_event(self):
        """
        This method listens for a new frames. Retuns True if event frame
        was received and correctly processed.
        """

        try:
            w = self.ser.in_waiting
        except:
            msg("Reconnecting serial line ...")
            try:
                self.connect()
            except Exception as e:
                msg("Unable to reconnect ...")
                self.event = False
                raise e
            else:
                w = self.ser.in_waiting

        if w == 0:
            # there is no data in buffer
            self.event = False
            return

        try:
            c = self.ser.read(1).decode("utf-8")
        except Exception as e:
            msg("Serial line is broken and reconnect will not help!")
            self.event = False
            raise e

        if c != ':':
            # it is not a start char
            self.event = False
            return

        tmp_frame = ':'

        try:
            tmp_frame += self.ser.readline().decode("utf-8").strip()
        except Exception as e:
            msg("Serial line is broken and reconnect will not help!")
            self.event = False
            raise e

        # calculate crc of received data
        crc = self.crc16( tmp_frame[0:16])

        # split into array
        tmp_frame = tmp_frame[1:].split(';')

        # convert from hex string to int
        tmp_frame[3] = int(tmp_frame[3], 16)

        if crc != tmp_frame[3]:
            msg("Crc error , malformed frames are ingored")
            self.event = False
            return

        # crc is correct , frame looks good
        # time to convert hex strings back to ints
        tmp_frame[0] = int(tmp_frame[0], 16)
        tmp_frame[1] = int(tmp_frame[1], 16)
        # unfuck negative values
        if tmp_frame[1] > 0x7fff:
            tmp_frame[1] -= 0x10000
        tmp_frame[2] = int(tmp_frame[2], 16)
        if tmp_frame[2] > 0x7fff:
            tmp_frame[2] -= 0x10000

        # write back old values
        self.events[1] = self.events[0]

        # write down a new value
        new = dict(
            button=tmp_frame[0],
            counter=tmp_frame[1],
            diff=tmp_frame[2],
            timestamp=datetime.now().timestamp()
        )
        self.events[0] = new

        # update timestamps
        if bool(self.events[1]):
            new['last_change_ts'] = self.events[1]['timestamp']
            if (self.events[0]['button'] == 0
                    and self.events[1]['button'] == 1):
                self.last_button_push_ts = self.events[0]['timestamp']

            if (self.events[0]['button'] == 1
                    and self.events[1]['button'] == 0):
                self.last_button_release_ts = self.events[0]['timestamp']

        # Timestamps are going to be included into event dict.
        new['last_button_push_ts'] = self.last_button_push_ts
        new['last_button_release_ts'] = self.last_button_release_ts

        self.events[0] = new
        self.event = new
        return

    @staticmethod
    def crc16(st, crc=0xffff):
        """
        Given a binary string and starting CRC, Calc a final CRC-16
        """
        table = (
            0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
            0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
            0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
            0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
            0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
            0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
            0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
            0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
            0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
            0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
            0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
            0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
            0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
            0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
            0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
            0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
            0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
            0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
            0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
            0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
            0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
            0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
            0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
            0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
            0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
            0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
            0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
            0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
            0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
            0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
            0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
            0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040)

        for ch in st:
            crc = (crc >> 8) ^ table[(crc ^ ord(ch)) & 0xFF]
        return crc


class Mqtt:

    def __init__(self, mqconf, callback):
        self.mqconf = mqconf
        self.callback = callback
        self.connhandlers = []

    def __del__(self):
        msg("Stopping all mq connections")
        for h in self.connhandlers:
            h.loop_stop()
            h.disconnect()

    def _consume_topic(self, channel):
        c = mqtt.Client()
        c.on_message = self.callback
        c.user_data_set({'channel': channel})
        if self.mqconf['username'] != None:
            c.username_pw_set(self.mqconf['username'], password=self.mqconf['password'])
        c.connect(self.mqconf['address'], port=self.mqconf['port'], keepalive=15)
        c.subscribe(self.mqconf['state_topics'][channel], qos=self.mqconf['qos'])
        c.loop_start()
        self.connhandlers.append(c)

    def consume_all(self):
        for ch, topic in self.mqconf['state_topics'].items():
            self._consume_topic(ch)

    def postback(self,ch,payload):
        auth = None
        if self.mqconf['username'] != None:
            auth = { 'username': self.mqconf['username'],
                    'password': self.mqconf['password']
                }

        try:
            publish.single(self.mqconf['command_topics'][str(ch)],
                hostname = self.mqconf['address'],
                port=self.mqconf['port'],
                auth=auth,
                payload=json.dumps(payload),
                qos=self.mqconf['qos'],
                keepalive=15,
                retain=True)
        except Exception as e:
            msg("Unable to send channel%s command : %s" % (ch,e))
        else:
            msg("Channel%s command sent" % ch)


class EventReactor:

    def __init__(self, mqconf):
        self.mqconf = mqconf
        self.lock = threading.Lock()
        self.channels = [ {'state': 'ON', 'brightness': 255},
                            {'state': 'ON', 'brightness': 255},
                            {'state': 'ON', 'brightness': 255} ]
        self.lastonoff = "OFF"
        self.callback = False

    def set_callback(self, callback):
        self.callback = callback

    def update_channel(self, client, userdata, message):
        ch = int(userdata['channel'])

        payload_str = str(message.payload.decode("utf-8"))

        try:
            payload = json.loads(payload_str)
        except Exception as e:
            msg("Channel%s : Malformed json message : %s" % (ch,e))
            return

        if type(payload) is not dict:
            msg("Channel%s : mqtt_json format expected , got %s!" % (ch,type(payload)))
            return

        self.lock.acquire(blocking=True, timeout=-1)
        msg("Channel%s update: %s" % (ch,payload))

        new = dict()
        for k,v in self.channels[ch].items():
            new[k]=v

        for k,v in payload.items():
            new[k]=v

        self.channels[ch] = new
        s = new.get('state', False)
        if bool(s):
            self.lastonoff = s
        self.lock.release()

    def process_event(self,events):

        now = datetime.now().timestamp()

        #stupid pushbutton
        if ( events[0]['button'] == 1 and
                now - events[0]['last_button_push_ts'] <1 ):
            self.callback(3, dict())



def main():
    """
    Main routine
    """

    msg("Starting ...")
    sh = SignalHandler()

    conf = getenv()

    enc = Encmod(conf['serial'])
    evr = EventReactor(conf['mqtt'])

    try:
        enc.connect()
    except Exception as e:
        msg("Unable to open serial line : \n%s" % (str(e)))
        sys.exit(-1)

    mq = Mqtt(conf['mqtt'], evr.update_channel)
    mq.consume_all()

    evr.set_callback(mq.postback)

    msg("Starting main program loop")
    while sh.signum == False:

        try:
            enc.get_new_event()
        except Exception as e:
            msg("Unable to read serial line events : \n%s : %s" % (e.errno, e.strerror))
            del enc
            sys.exit(-1)

        if bool(enc.event):
            msg("%s" % enc.event)
            evr.process_event(enc.events)

        time.sleep(0.01)

    msg("Stopping now on signal ...")
    del enc


if __name__ == "__main__":
    main()
