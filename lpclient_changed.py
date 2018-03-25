#
#  Lazy Pirate client
#  Use zmq_poll to do a safe request-reply
#  To run, start lpserver and then randomly kill/restart it
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
from __future__ import print_function

import zmq
import json
import time

from datetime import datetime
from email.parser import BytesParser
from email.policy import default
from imaplib import IMAP4_SSL

from imap_credentials import imap_password, imap_username


def fetch_emails(addr, port, user, pwd,
                 uid=None, mail_limit=10, commit_limit=5):
    ''' returns up to "commit_limit" new emails from INBOX per run
        touches no more than "mail_limit" mails per multiple runs'''

    def fetch_and_parse(uids):
        ''' fetches and parses up to "commit_limit" new emails '''

        result = list()

        for uid in uids:
            email_dict = dict()
            reply, email_data = imap_server.uid('fetch', uid, '(RFC822)')
            if reply == 'OK':
                raw_email = email_data[0][1]
                email = BytesParser(policy=default).parsebytes(raw_email)
                email_dict['Date'] = datetime.strptime(
                    email['Date'], '%a, %d %b %Y %H:%M:%S %z')

                for header in ['From', 'To', 'Delivered-To',
                               'Message-ID', 'Subject']:
                    email_dict[header] = email[header]
                email_dict['plain'] = None
                email_dict['html'] = None
                for part in email.walk():
                    if part.get_content_type() == 'text/html':
                        email_dict['html'] = part.get_body().get_content()
                    elif part.get_content_type() == 'text/plain':
                        email_dict['plain'] = part.get_body().get_content()
                result.append(email_dict)

        return result

    imap_server = IMAP4_SSL('imap.gmail.com')
    imap_server.login(imap_username, imap_password)
    imap_server.select(mailbox='INBOX', readonly=True)

    if uid:
        reply, data = imap_server.uid('search', None, '{}:*'.format(uid))
    else:
        reply, data = imap_server.uid('search', None, 'ALL')
        uid = 0

    if reply == 'OK':
        uids_blist = data[0].split()
        len_uids_blist = len(uids_blist)

        if len_uids_blist < 2:
            if uids_blist[0] > str(uid).encode():
                #fetch_and_parse(uids_blist)

                # return '1 new mail'
                return int(uids_blist[0].decode()), False, fetch_and_parse(uids_blist)

            # return '0 new mails'
            return uid, False, False
        elif len_uids_blist > commit_limit:
            if len_uids_blist > mail_limit:
                #fetch_and_parse(uids_blist[-mail_limit:][:commit_limit])
                        return int(uids_blist[-mail_limit:][:commit_limit][-1].decode()), True, fetch_and_parse(uids_blist[-mail_limit:][:commit_limit])
            #else:
                #fetch_and_parse(uids_blist[:commit_limit])
            return int(uids_blist[:commit_limit][-1].decode()), True, fetch_and_parse(uids_blist[:commit_limit])

            # return 'Many new mails'
        else:
            #fetch_and_parse(uids_blist)

            #return 'Some new mails'
            return int(uids_blist[-1].decode()), False, fetch_and_parse(uids_blist)
    else:
        return 'Something wrong'


if __name__ == '__main__':
    for num in [17, 16, 15, 10, None]:
        print(fetch_emails('imap.gmail.com', 993, imap_username, imap_password, num))
REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3
SERVER_ENDPOINT = "tcp://localhost:5555"

context = zmq.Context(1)

print("I: Connecting to server...")
client = context.socket(zmq.REQ)
client.connect(SERVER_ENDPOINT)

poll = zmq.Poller()
poll.register(client, zmq.POLLIN)

sequence = 0
retries_left = REQUEST_RETRIES
expect_reply = False
phase = 0
while True:
    if not expect_reply:
        sequence += 1
        # request = str(sequence).encode()
        if phase == 0:
            words = "Ready to server, Master!"
        elif phase == 1:
            words = "I can download emails, Master!"
        elif phase == 2:
            words = "I need the uid of last mail i checked, Master!"
        else:
            phase = 0
            time.sleep(10)
            continue
        request = json.dumps([sequence, "Slave", phase, words])
        print("I: Sending (%s)" % request)
        client.send_json(request)

    expect_reply = True
    while expect_reply:
        socks = dict(poll.poll(REQUEST_TIMEOUT))
        if socks.get(client) == zmq.POLLIN:
            reply = client.recv_json()
            if not reply:
                break
#                continue
            r = json.loads(reply)
#            print(r)
            if int(r[0]) == sequence:
                print("I: {} replied OK ({})".format(r[1], r[0]))
                print(r[3])
                retries_left = REQUEST_RETRIES
                phase = r[2]+1
                expect_reply = False
            else:
                print("E: Malformed reply from server: %s" % reply)

        else:
            print("W: No response from server, retrying...")
            if retries_left > 0:
                retries_left -= 1
                break
            # Socket is confused. Close and remove it.
            client.setsockopt(zmq.LINGER, 0)
            client.close()
            poll.unregister(client)
#            retries_left -= 1
#            if retries_left == 0:
#                print("E: Server seems to be offline, abandoning")
#                break
            print("I: Reconnecting and resending (%s)" % request)
            # Create new connection
            client = context.socket(zmq.REQ)
            client.connect(SERVER_ENDPOINT)
            poll.register(client, zmq.POLLIN)
            retries_left = REQUEST_RETRIES
            client.send_json(request)

context.term()
