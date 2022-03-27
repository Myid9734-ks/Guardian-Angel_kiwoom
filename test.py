import requests
import time

def send_msg(msg=''):
        response = requests.post(
        'https://notify-api.line.me/api/notify',
        headers={
            'Authorization' : 'Bearer rs5WOIz2jZQ3NGOBZqN1cbm6WHmnqMCt2K4ydqxFkco'
        },
        data={
            'message': msg
            }
        )
        


def les1(x):
    xa = x
    for i in range(1,100):
        print(i,'000000000000')
        time.sleep(0.2)

