import boto
from boto import dynamodb
from boto.dynamodb import condition

# Usamos las credenciales del env, para mostrar como no necesitamos explicitarlo en el codigo
conn = boto.connect_dynamodb()

table = conn.get_table("asistentes_pycon_ar")


print "--- get"
print table.get_item( 
    hash_key = 'gutes',
    range_key = '30'
)

print "--- scan"
for row in table.scan(scan_filter = {"edad":condition.EQ("30")} ):
    print row

print "--- query"
for row in table.query("gutes", range_key_condition = condition.EQ("30") ):
    print row    