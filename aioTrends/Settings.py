# -*- coding: utf-8 -*-

import json
import logging
import os

from aiohttp import ClientResponse, ClientTimeout

# Create folders
for f in ['./data', './log', './proxies', './settings', './data/rawDaily', './data/rawMonthly', './data/scaledDaily', './data/IOT']:
    if not os.path.exists(f):
        os.mkdir(f)

# Default settings.json
if not os.path.exists('./settings/settings.json'):
    with open('./settings/settings.json', 'w') as f:
        F = {
            "hl": "en-US",
            "geo": "US",
            "tz": 360,
            "cat": 0,
            "gprop": "",
            "pathUserAgents": "./settings/userAgents.json",
            "pathProxies": "./proxies/proxies.txt",
            "pathQrys": "./data/qrys.pkl",
            "pathCookies": "./data/cookies.pkl",
            "pathWgt": "./data/widget.pkl",
            "pathDataIOT": "./data/IOT/",
            "pathRawDaily": "./data/rawDaily",
            "pathRawMonthly": "./data/rawMonthly",
            "pathScaledDaily": "./data/scaledDaily",
            "pathWgtEptyRes": "./data/widgetReceiveEmptyReponse.pkl",
            "timeout": 10
        }
        json.dump(F, f, indent=4)
else:
    F = json.load(open('./settings/settings.json', 'r'))

# checking user agents json file
if not os.path.exists('./settings/userAgents.json'):
    logging.error("Please download the json file from https://github.com/Said-Ait-Driss/user-agents/blob/main/userAgents.json and past it under the folder 'settings' ")

# checking proxy file
if not os.path.exists('./proxies/proxies.txt'):
    logging.error("Please put 'proxies.txt' under the folder 'proxies'")

class Settings:
    def __init__(self):
        self.hl = F['hl']
        self.geo = F['geo']
        self.tz = F['tz']
        self.cat = F['cat']
        self.gprop = F['gprop']
        self.pathUserAgents = F['pathUserAgents']
        self.pathProxies = F['pathProxies']
        self.pathQrys = F['pathQrys']
        self.pathCookies = F['pathCookies']
        self.pathWgt = F['pathWgt']
        self.pathDataIOT = F['pathDataIOT']
        self.pathRawDaily = F['pathRawDaily']
        self.pathRawMonthly = F['pathRawMonthly']
        self.pathScaledDaily = F['pathScaledDaily']
        self.pathWgtEptyRes = F['pathWgtEptyRes']
        self.timeout = ClientTimeout(total=int(F['timeout']))
        self.urls = {
            'token': 'https://trends.google.com/trends/api/explore',
            'multiline': 'https://trends.google.com/trends/api/widgetdata/multiline'} #'comparedgeo': 'https://trends.google.com/trends/api/widgetdata/comparedgeo', #'relatedsearches': 'https://trends.google.com/trends/api/widgetdata/relatedsearches', #'trending': 'https://trends.google.com/trends/hottrends/visualize/internal/data'
            
    async def status(self, tag, res: ClientResponse):
        if res.status == 200:
            return True
        elif res.status == 429:
            msg = f'{tag}|Error:429| Rate Limited'
            logging.error(msg)
            return False
        elif res.status == 500:
            msg = f'{tag}|Error:500| Unkown Server Errors' # <html lang=en><meta charset=utf-8><meta name=viewport content="initial-scale=1, minimum-scale=1, width=device-width"><title>Error 500 (Server Error)!!1</title><style nonce="wKnQRZBWY0XEqXV-4Y6h8Q">*{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{color:#222;text-align:unset;margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px;}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}pre{white-space:pre-wrap;}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px}</style><main id="af-error-container" role="main"><a href=//www.google.com><span id=logo aria-label=Google role=img></span></a><p><b>500.</b> <ins>That’s an error.</ins><p>There was an error. Please try again later. <ins>That’s all we know.</ins></main>
            logging.error(msg)
            return False
        elif res.status == 502:
            msg = f'{tag}|Error:502| Proxy Errors or Bad Gatways'
            logging.error(msg)
            return False
        else:
            msg = f'{tag}|Error:{res.status}|: {await res.text()}'
            logging.error(msg)
            return False

class CustomFormatter(logging.Formatter):
    
    grey = '\33[35m'
    blue = '\33[34m'
    yellow = '\33[93m'
    red = '\33[31m'
    green = '\33[92m'
    reset = '\33[0m'

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.green + self.fmt + self.reset
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setLog(logPath: str):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fmt = '%(asctime)s%(message)s'
    
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(CustomFormatter(fmt))
    
    file_handler = logging.FileHandler(
        filename=logPath,
        mode='w', encoding='utf-8'
        )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(fmt))
    
    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)

