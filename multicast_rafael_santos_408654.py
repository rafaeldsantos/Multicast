import socket
import struct
from threading import Thread
from operator import itemgetter
from random import *
import pickle
import time

DEBUG = True

MCAST_GRP = '224.0.0.1'
MCAST_PORT = 10000

ID = randint(0,1000000);
print ('ID Processo:', ID)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((MCAST_GRP, MCAST_PORT))
mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
 
sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

class enviador(Thread):
 	def __init__ (self, lista_msg):
 		Thread.__init__(self)
 		MCAST_GRP = '224.0.0.1'
 		MCAST_PORT = 10000

 		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
 		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
 		sock.bind((MCAST_GRP, MCAST_PORT))
 		mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
 		sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

 		self.sock = sock
 		self.lista_msg = lista_msg
 		self.total = 0
 	def run(self):
 		while True:
 			for msg in self.lista_msg:
 				sock.sendto(pickle.dumps(msg), (MCAST_GRP, MCAST_PORT))
 			time.sleep(0.8)

class recebedor(Thread):
 	def __init__ (self, sock,envia, ID, timestamp,DEBUG):
 		Thread.__init__(self)
 		self.sock = sock
 		self.envia = envia
 		self.processos = set()
 		self.fila = []
 		self.ACKS = {}
 		self.ID = ID
 		self.timestamp = timestamp
 	
 	def run(self):
 		def recebi_primeira_vez(msg):
 			return not (msg[0] in self.processos) and not msg[1]
 		def cria_lista_ACK(msg):
 			return msg[1] and not (msg[0] in self.ACKS)
 		def resposta_ACK(msg):
 			return (msg[1]) and not (msg[2] in self.ACKS[ msg[0] ])
 		
 		while True:
 			if len(self.fila) > 0:
 				if (self.ACKS[ self.fila[0][0] ] ==  self.processos and len(self.processos) == 3):
 					l = self.fila.pop(0)
 					print ("POP {",l,"}")

 			msg = pickle.loads(sock.recv( 1024 ))

 			#se eh a primeira vez que recebe a mensagem
 			#if not (msg[0] in self.processos) and not msg[1]:
 			if recebi_primeira_vez(msg):
 				if DEBUG: print("Recebi a msg de ", msg[0])
 				self.processos.add(msg[0])
 				
 				self.fila.append(msg)
 				self.fila.sort(key=itemgetter(3, 0))

 				if(self.timestamp < msg[3]): self.timestamp = msg[3]
 				self.timestamp+=1
 				ACK = [msg[0], True, self.ID,self.timestamp]
 				envia.lista_msg.append(ACK)
 				self.ACKS[msg[0]] = set()
 				self.ACKS[msg[0]].add(msg[0])

 				#ACK de alguem que nao foi identificado ainda =()
 			#se for a segunda(ACK), e esse ACK nao foi recebido
 			#if msg[1] and not (msg[0] in self.ACKS):
 			if cria_lista_ACK(msg):
 				self.ACKS[msg[0]] = []
 			elif (msg[1]) and not (msg[2] in self.ACKS[ msg[0] ]):
 				if DEBUG: print("Recebi o ACK de ", msg[2],"para a msg de",msg[0])
 				self.ACKS[ msg[0] ].add(msg[2])
 				time.sleep(0.2)


timestamp = randint(0,100)
print("Meu timestamp é",timestamp)
msg = [[ID, False, False, timestamp]]
print("Minha mensagem é", msg)

envia = enviador(msg)
recebe = recebedor(sock, envia, ID,timestamp,DEBUG)

envia.start()
recebe.start()
envia.join()
recebe.join()