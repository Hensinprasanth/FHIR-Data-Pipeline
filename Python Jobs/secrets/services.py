# Note 
# If you have windows, save the passwords in the crendentials manager
# If you have linux, Python applications can use the keyring library to store and retrieve passwords using a generic keyring service

import keyring

keyring.set_password('anthem','elastic','#12erttttftttkk')
keyring.set_password('elastic','host','135.165.245.106')
keyring.set_password('elastic','port','9200')
keyring.get_password('elastic','ca_pem','/elk/elastic_certificate/elastic-certificate-kibana.pem')
keyring.get_password('aws','access_key_id','######@@@@@@@$FFFFESSSSSSSSSS')
keyring.get_password('aws','secret_access_key','3455556@@$')