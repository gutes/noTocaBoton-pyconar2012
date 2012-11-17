import boto
import time

from boto import dynamodb

# Usamos las credenciales del env, para mostrar como no necesitamos explicitarlo en el codigo

conn = boto.connect_dynamodb()
        
        
def wait_for_table( table, delay = 10 ):
    status = conn.describe_table(table.name)
    while status["Table"]["TableStatus"] != "ACTIVE":
        print "RETRY: '%s' not ACTIVE..." % (table.name
        time.sleep( delay )
        status = conn.describe_table(table.name)
    return table

asistentes_pycon = conn.create_schema(
        hash_key_name='asistente',
        hash_key_proto_value='S',
        range_key_name='edad',
        range_key_proto_value='N'
)        

table = conn.create_table(
        name='asistentes_pycon_ar',
        schema=asistentes_pycon,
        read_units=10,
        write_units=10
)
wait_for_table( table )

gutes = table.new_item(
     hash_key = "gutes",
     range_key = "30",
     attrs = {
        "email": "gutes@onapsis.com",
        "video_juego": "portal",
        "sexo": "masculino"
     }    
)
gutes.put()

fruss = table.new_item(
     hash_key = "fruss",
     range_key = "33",
     attrs = {
        "email": "fruss@coresecurity.com",
        "video_juego": "maniac manssion",
        "talle": "XXXL",
        "sexo": "si gracias."
     }    
)
fruss.put()

