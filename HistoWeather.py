# By Claudio Mazzoni November 5th, 2017
# Gets the historic temperatures of a specific region wolrd wide
# User can set the frequency also

import requests
import datetime as dt
import re
from dateutil import parser
# Documentation at:
# https://www.wunderground.com/weather/api/d/docs?d=data/index
# https://www.wunderground.com/weather/api/d/docs?d=resources/phrase-glossary
# https://www.wunderground.com/weather/api/d/docs?d=resources/country-to-iso-matching

class WeatherCall:

    urlstart = 'http://api.wunderground.com/api/YOURAPIKEY/'

    def __init__(self):
        pass

    def get_historical(self, gooddate, state, city):

        if type(gooddate) is dt.date:
            cleandt = gooddate.strftime('%Y%m%d')
        elif re.search('[a-zA-Z]', gooddate) is True:
            cleandt = parser.parse(gooddate).strftime('%Y%m%d')
        else:
            cleandt = gooddate

        urlend = 'history_{0}/q/{1}/{2}.json'.format(cleandt, state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()

    def get_almanac(self, state, city):
        urlend = 'almanac/q/{0}/{1}.json'.format(state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()

    def get_hourly(self,  state, city):
        urlend = 'hourly/q/{0}/{1}.json'.format(state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()

    def get_hourly10days(self, state, city):
        urlend = 'hourly10days/q/{0}/{1}.json'.format(state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()

    def get_forcast10days(self, state, city):
        urlend = 'forcast10days/q/{0}/{1}.json'.format(state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()

    def get_forcast(self,  state, city):
        urlend = 'forcast/q/{0}/{1}.json'.format(state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()

    def get_conditions(self, state, city):
        urlend = 'conditions/q/{0}/{1}.json'.format(state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()

    def get_webcams(self, state, city):
        urlend = 'webcams/q/{0}/{1}.json'.format(state, city)
        try:
            url = '{0}{1}'.format(self.urlstart, urlend)
            data = requests.get(url, timeout=2).json()
            return data
        except KeyError:
            print('Invalid City or Country')
            quit()


if __name__ == "__main__":
    from datetime import date
    from dateutil.rrule import rrule, DAILY
    a = date(2013, 12, 31)
    b = date(2013, 12, 31)
    instance = WeatherCall()
    for dt in rrule(DAILY, dtstart=a, until=b):
        weather = instance.get_historical(gooddate=dt.strftime("%Y%m%d"), state='CA', city='San Francisco')
        summary = weather['history']['dailysummary']
        for js in summary:
            print([dt.strftime("%Y%m%d"), js['precipi'], js['meantempi'],
                                    js['meanwindspdi'], js['humidity']])
