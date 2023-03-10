# -*- coding: utf-8 -*-
"""
Created on Tue Nov 22 16:20:34 2022

@author: ilepoutre
"""

import os
import pandas
import msal
import requests
import email


MSFT_CLIENT_ID = os.environ['MSFT_GRAPH_API_PYTHON_MAIL_API_CLIENTID']
MSFT_CLIENT_SECRET = os.environ['MSFT_GRAPH_API_PYTHON_MAIL_API_CLIENT_SECRET']
MSFT_TENANT_ID = os.environ['MSFT_GRAPH_API_PYTHON_MAIL_API_TENANTID']
MSFT_SCOPES = ['https://graph.microsoft.com/.default']
msft_authority = 'https://login.microsoftonline.com/' + MSFT_TENANT_ID

graph_URI = 'https://graph.microsoft.com'

FACTURES_MSFT_USER_ID =  os.environ['MSFT_GRAPH_API_PYTHON_MAIL_FACTURES_USER_ID']




def get_access_token():
    
    app = msal.ConfidentialClientApplication(MSFT_CLIENT_ID, 
                                             authority=msft_authority, 
                                             client_credential = MSFT_CLIENT_SECRET)
    
    access_token  = app.acquire_token_for_client(MSFT_SCOPES)

    return access_token



    
def get_request_headers():
    """
    """
    access_token = get_access_token()
    request_headers = {'Authorization': 'Bearer ' + access_token['access_token']}
    
    return request_headers



def retrieve_userid_from_mail(request_headers, target_mail):

    l_users = requests.get(graph_URI +'/v1.0/users', headers=request_headers).json()
    user_id = list(filter(lambda u: u['mail'] == target_mail, l_users['value']))[0]['id']

    return user_id


def datetime_to_isoformat(dt):    
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4] + 'Z'


def get_df_msgs(START_EXTRACTION, **data):
    '''
    data = dict(request_headers=request_headers)

    Parameters
    ----------
    START_EXTRACTION : TYPE
        DESCRIPTION.
    **data : TYPE
        DESCRIPTION.

    Returns
    -------
    df_msgs : TYPE
        DESCRIPTION.
    _l : TYPE
        DESCRIPTION.

    '''
    request_headers = data['request_headers']
    user_id = data.get('user_id', FACTURES_MSFT_USER_ID)

    LIMIT = data.get("limit", 1000)
    ORDER_BY = 'createdDateTime DESC'
    FILTER = "(createdDateTime ge %s)" % (datetime_to_isoformat(START_EXTRACTION))
    
    
    endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/messages?$filter=%s&$top={LIMIT}&$orderBy={ORDER_BY}' % (FILTER)

    resp2 = requests.get(endpoint, headers=request_headers).json()
    
    _l = []
    for msg_api in resp2['value']:
        
        d = {
            # "receivedDateTime": msg_api['receivedDateTime'], 
            # "createdDateTime": msg_api['createdDateTime'], 
            "sentDateTime": msg_api['sentDateTime'],
            "subject": msg_api['subject'],
            "bodyPreview": msg_api['bodyPreview'],
            "conversationId": msg_api['conversationId'],
            "sender": msg_api['sender']['emailAddress']['address'],
            "from": msg_api['from']['emailAddress']['address'],
            # "body": msg_api['body'],
            "to": msg_api['toRecipients'][0]['emailAddress']['address'],
            "id": msg_api['id'],
            'hasAttachments': msg_api['hasAttachments'],
            'isRead': msg_api['isRead']
        }
        
        d['date'] = pandas.to_datetime(d['sentDateTime']).to_pydatetime()
        
        _l.append(d)
    
    df_msgs = pandas.DataFrame.from_records(_l)
    
    assert ~df_msgs.id.duplicated().all()
    
    return df_msgs, _l


def get_mime_msg(id_message, **data):
        
    request_headers = data['request_headers']    
    user_id = data.get('user_id', FACTURES_MSFT_USER_ID)
    
    request_url = f'https://graph.microsoft.com/v1.0/users/{user_id}/messages/{id_message}/$value'
    
    r = requests.get(request_url, headers=request_headers)

    email_msg = email.message_from_bytes(r.content)
    
    return email_msg


def get_df_attachments(id_message, **data):

    request_headers = data['request_headers']    
    user_id = data.get('user_id', FACTURES_MSFT_USER_ID)
    
    endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/messages'
    
    request_url2 = f'{endpoint}/{id_message}/attachments'
    
    resp2 = requests.get(request_url2, headers=request_headers)
    r_json2 = resp2.json()
    
    _l2 = [{ k: d[k] for k in ['name', 'contentType', 'isInline', 'id'] } for d in r_json2['value']]
    
    df_attachments = pandas.DataFrame(_l2)
    
    return df_attachments, r_json2['value']



def save_attachment(id_message, id_attachment, filepath,  **data):

    user_id = data.get('user_id', FACTURES_MSFT_USER_ID)
    
    request_headers = data['request_headers']    
    endpoint = f'https://graph.microsoft.com/v1.0/users/{user_id}/messages'

    r_attachment = requests.get(f'{endpoint}/{id_message}/attachments/{id_attachment}/$value', headers=request_headers)
    
    with open(filepath, 'wb') as fp:
        fp.write(r_attachment.content) 




