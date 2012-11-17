import boto
import credenciales
from boto.ec2 import elb
from credenciales import access_key_id, secret_access_key


class EjemploEC2(object):
    access_key_id = credenciales.access_key_id
    secret_access_key = credenciales.secret_access_key
    
    def __init__(self):
        self.ec2Conn = boto.connect_ec2(credenciales.access_key_id, credenciales.secret_access_key)
        self.elbConn = elb.ELBConnection(credenciales.access_key_id, credenciales.secret_access_key)
        
        self.instancias = None
        self.lb = None
    
    def traer_instancias(self):
        ''' Trae las instancias desde AWS, las pone en un dict indexado por instance id'''
        
        self.instancias = dict([(i.id, i) for r in self.ec2Conn.get_all_instances() for i in r.instances])
        return self.instancias
        
    def asociar_ip(self, instancia, ip):
        ''' Asocia una elastic ip (e.g. 54.243.251.49) a una instancia '''
        
        if (instancia.state == "running"):
            return self.ec2Conn.associate_address(instancia.id, ip)         
        return False
    
    def load_balancers(self):
        self.lb = dict([(l.name, l) for l in self.elbConn.get_all_load_balancers()])
        return self.lb
        
    def registrar_webservers(self):
        ''' Registro los Web Servers prendidos en el laod balancer, aquellos que en el nombre tienen 'Web' '''
         
        return self.elbConn.register_instances('PruebaWeb', \
                                                [ iid for iid, ins in self.instancias.items() \
                                                    if ins.state == 'running' and 'Web' in ins.tags["Name"] ]
                )

def iniciar_demo_EC2():
    e = EjemploEC2()
    e.traer_instancias()
    #e.load_balancers()
    #e.registrar_webservers()
    
    return e

# Web Server 2
#e.instancias['i-21ffa15d'].stop()
#e.instancias['i-21ffa15d'].start()

# Web Server 1
#e.instancias['i-339bc54f'].stop()
#e.instancias['i-339bc54f'].start()


