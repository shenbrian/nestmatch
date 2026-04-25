
import imaplib

mail = imaplib.IMAP4_SSL('imap.zoho.com.au', 993)

print(mail.login('buyers@nestmatch.com.au', 'VBtgS0z96GTE'))

