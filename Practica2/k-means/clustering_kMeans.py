import csv
import pprint
import matplotlib.pyplot as plt
from scipy.spatial import distance
from operator import add
import numpy
#import matplotlib
#matplotlib.use('Agg')
#from scipy.cluster.vq import *
import pylab
#pylab.close()

# Asignatura: SGDI, Practica 2 
# Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
# Este documento es fruto exclusivamente del trabajo de sus miembros


def read_file( filename):
	
	# abrimos el csv
	infile = open(filename, 'r')
	reader = csv.reader(infile)
	pp = pprint.PrettyPrinter(indent=3)


	l = []
	i = 0
	for row in reader:
		if i > 0:
			#Pasamos a float todos los atributos
			aux_row = [float(x) for x in row]
			l = l + [aux_row]
		i = i + 1
	
	
	
	
	#print l
	#print pp.pprint(l)
	
	
	return l



def kmeans(k, instancias , centroides):
	
	#Si no nos dan los centroides iniciales, los calculamos
	if centroides == None:
		centroides = centroidesIni(instancias, k)
	
	
	#Repetir hasta que los clusters no cambien
	clustering = inicia_cluster(k)
	cluster_aux = inicia_cluster(k)
	cambio = True
	#Mientras las instancias cambien de cluster
	while(cambio):
	
		for i in instancias:
			# Se asigna cada instancia al cluster con centroide mas cercano
			# distMin es un par (distancia, centroide)
			distMin = distanciaMin(k, centroides, i)
			cluster_aux[distMin[1]] = cluster_aux[distMin[1]] + [i]
		
		#Comprobamos si hay cambios
		if(cluster_aux == clustering):
			cambio = False
			
		#Guardamos el resultado y reinicializamos la variable auxiliar
		clustering = cluster_aux
		cluster_aux = inicia_cluster(k)
		
		
		
		#Calcular los nuevos centroides de cada cluster
		centroides = recalculaCentroides(k, clustering)
	

	return (clustering, centroides)

# Inicializa el cluster como un diccionario con k claves y k listas vacias
def inicia_cluster(k):
	cluster = {}
	for c in range(k):
		cluster[c] = []
	
	return cluster
	
# Calcula los centroides iniciales
def centroidesIni(instancias, k):
	centroides = list(range(k)) # Lista con los valores de 1..k
	centroides[0] = instancias[0] # El primer centroide es la primera instancia
	# Por cada futuro cluster
	for c in range(1, k):
		distMax = 0
		# Elijo como siguiente centroide aquella instancia cuya minima distancia a los demas centroides es la maxima.
		for i in instancias:
			distMin = distanciaMin(c, centroides, i) # Distancia minima de la instancia a cualquier centroide
			# Actualizamos la distancia maxima
			if(distMin[0] > distMax):
				distMax = distMin[0]
				centroides[c] = i
		
	return centroides
	



# Calcula la distancia minima entre la instancia y los centroides (la distancia al centroide mas cercano)
# k es el numero actual de centroides calculados
# Devolvemos un par (distancia, centroide)
def distanciaMin(k, centroides, instancia):	
	d = {}
	for c in range(k):
			# Guardamos  la distancia al centroide junto con su posicion
		
			d[c] = (distance.euclidean(instancia, centroides[c]), c)	
	 
	return d[min(d, key=d.get)]

# Calcula los nuevos centroides de cada cluster
def recalculaCentroides(k, clusters):

	centroides = list(range(k))
	# Por cada cluster calculamos la media de las instancias de ese cluster
	for c in clusters:
	
		centroides[c] = media(clusters[c])
		
	return centroides
	
# Calcula la media de cada atributo de las instancias
def media(instancias):
	
	suma = instancias[0]
	for i in instancias[1:]:
		suma = map(add, suma, i)
	
	media = numpy.array(suma)
	
	return list(media/len(instancias))

# Calcula el diametro de un cluster como medida de coherencia
def diametro(cluster):

	d = []
	for i in cluster:
		for i2 in cluster:
			d = d + [distance.euclidean(i, i2)]

	return max(d)
	
	
# Calcula el radio de un cluster como medida de coherencia
def radio(cluster, centroide):
	d =  []
	for i in cluster:
		d = d + [distance.euclidean(i, centroide)]
		
	return max(d)

# Calcula la media del cuadrado de las distancias, es una medida de coherencia
def distanciaPromedio(cluster, centroide):

	d =  []
	for i in cluster:
		d = d + [distance.euclidean(i, centroide)]
	d = map(cuadrado, d)
	
	return numpy.mean(d)
		
# Calcula el cuadrado de un numero
def cuadrado(n):
	return n ** 2

	
# Calcula la medida de cohesion de los cluster para cada k y dibuja en graficas
# la evolucion de estas medidas en funcion de k
def cohesion(l):
	
	# medias para las medidas de cohesion de cada cluster
	media_radio = []
	media_diametro = []
	media_dist_promedio = []
	# para cada k
	for k in range(2,21):
		(clustering, centroides) = kmeans(k, l , None)
		l_radio = []
		l_diametro = []
		dist_promedio = []
		# para cada cluster calculamos su radio, diametro y dist_promedio
		for key, cluster in clustering.iteritems():
			l_radio += [radio(cluster,centroides[key])]
			l_diametro += [diametro(cluster)]
			dist_promedio += [distanciaPromedio(cluster,centroides[key])]
		# actualizamos las medias con lo calculado
		media_radio += [numpy.mean(l_radio)]
		media_diametro += [numpy.mean(l_diametro)]
		media_dist_promedio += [numpy.mean(dist_promedio)]

	# Dibujamos las graficas
	valores_k = list(range(2,21))

	print(valores_k)
	print(media_radio)
	print(media_diametro)
	print(media_dist_promedio)

	plt.figure(1)

	plt.subplot(221)
	plt.plot(valores_k, media_radio)
	plt.title('Radio')
	plt.ylabel('Cohesion')
	plt.xlabel('k')
	plt.show()
		
	plt.subplot(222)
	plt.plot(valores_k, media_diametro)
	plt.title('Diametro')
	plt.ylabel('Cohesion')
	plt.xlabel('k')
	plt.show()

	plt.subplot(223)
	plt.plot(valores_k, media_dist_promedio)
	plt.title('Distancia promedio')
	plt.ylabel('Cohesion')
	plt.xlabel('k')
	plt.show()


	

if __name__ == '__main__':
	l = read_file("customers.csv") # Leemos el archivo
	cluster = kmeans(3, l , None) # calculamos el kmeans con k = 3
	print cluster
	cohesion(l) # calculamos medidas de cohesion con k = 2..20

	



