import json, requests, os

#get list of leagues
def get_config_value(key):
    with open('config.json') as r1:
        content = json.load(r1)
        value = content[key]
    return value

def get_league_list():
    url = get_config_value('url_leagues')
    response = requests.get(url).json()
    #print('{:20}{:20}'.format('League ID', 'Name'))
    #print('{}'.format('*'*100))
    temp = ''
    header = '{:20}{:20}'.format('League ID', 'Name')
    sep = '{}'.format('*'*80)
    temp += header + '\n' + sep
    for i in response:
        #print('{:<20}{:<20}'.format(i['id'], i['displayName']))
        temp += '\n'
        temp += '{:<20}{:<20}'.format(i['id'], i['displayName'])
    print(temp)
    return temp


def write_league_list():
    with open(get_config_value('file_leagues'), 'w') as w1:
        w1.write(get_league_list())


get_league_list()
#write_league_list()
