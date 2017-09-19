from __future__ import division
from mrjob.job import MRJob
import csv
import os
import pprint
from scipy.spatial import distance


# Asignatura: SGDI, Practica 2 
# Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
# Este documento es fruto exclusivamente del trabajo de sus miembros.


#DUDAS: Se asume que todos los atributos son continuos?
#		El test hay que hacerlo con iris e iris_test?
def read_file( filename):
	
	infile = open(filename, 'r')
	reader = csv.reader(infile)
	pp = pprint.PrettyPrinter(indent=3)


	l = []
	i = 0
	for row in reader:
		if i > 0:
			# los pasamos a float a todos menos la clase, que la concatenamos al final
			aux_row = [float(x) for x in row[0:len(row)-1]] + [row[len(row)-1]]
			l = l + [aux_row]
		i = i + 1
	
	
	#print l
	#print pp.pprint(l)
	
	return l


def knn( k, i, c):
	
	s = vecinos_cercanos(k,c,i)
	return moda(s)


def vecinos_cercanos(k, c, i):
	
	v = {}
	for instance in c:
		#Distancia:
		#Si i tiene atributo clase, lo quitamos
		if(len(i)==len(instance)):
			d = distance.euclidean(i[0:len(i)-1], instance[0:len(instance)-1])
		#Si no, solo quitamos el atributo clase de la instancia del conjunto
		else :
			d = distance.euclidean(i, instance[0:len(instance)-1])
		#llenamos la lista
		if len(v) < k :
			v[d] = instance
		#una vez la lista esta llena se van almacenando solo los vecinos mas cercanos, eliminando los que no lo son
		elif d < max(v) :
			v.pop(max(v))
			v[d] = instance
		
			
	#devuelve la lista de vecinos mas cercanos
	return v.values()


def moda(s):
	clase = {}
	
	for v in s:
		#obtenemos la clase del vecino actual
		c = v[len(v)-1]
		#si ya tenemos la clase en el diccionario se suma uno al contador
		if(clase.has_key(c)):
			clase[c] = clase[c] + 1
		#si no, se inicia el contador a 1
		else:
			clase[c] = 1
		
	#devolvemos la clase que mas veces se repite
	return max(clase.iterkeys(), key=(lambda key: clase[key]))
	
	
	
def test(k, trainset, testset):
	

	aciertos = 0;
	for test in testset:
		#guardamos la clase original
		clase = test[len(test)-1]
		#guardamos la clase predicha
		p = knn(k, test, trainset)
		#Si esta correctamente clasificado, se aumenta el contador
		if(clase == p):
			aciertos = aciertos + 1
	
	#devuelve la proporcion de instancias correctamente clasificadas
	return aciertos/len(testset)


class kNN(MRJob):

	k = None
	test_file = None
	test = None

	@staticmethod
	def moda(s):
		clase = {}
		
		for v in s:
			#si ya tenemos la clase en el diccionario se suma uno al contador
			if(clase.has_key(v)):
				clase[v] = clase[v] + 1
			#si no, se inicia el contador a 1
			else:
				clase[v] = 1
			
		#devolvemos la clase que mas veces se repite
		return max(clase.iterkeys(), key=(lambda key: clase[key]))
		
	def init(self):
		self.k = 5
		self.test_file = "E:\SGDI\practica2\iris_test.csv"
		#self.test_file = "C:\Users\Valentina\Documents\UCM\SGDI\practica2\iris_test.csv"
		
		infile = open(self.test_file, 'r')
		reader = csv.reader(infile)
		
		self.test = []
		i = 0
		for row in reader:
			if i > 0:
				# pasamos a float a todos menos la clase, que la concatenamos al final
				aux_row = [float(x) for x in row[0:len(row)-1]] + [row[len(row)-1]]
				self.test = self.test + [aux_row]
			i = i + 1
	
	def mapper_init(self):
		self.init()
		
	def reducer_init(self):
		self.init()
	
	
	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):
		
		word = line.split(',')
		
		# pasamos los datos a float
		l = []
		for w in word[0:len(word)-1]:
				l = l + [float(w)]
		# recorremos el conjunto de test y calculamos distancias de cada instancia de test
		# para la instancia de entrenamiento actual
		i = 0
		for instance in self.test:
			#Distancia:
			#quitamos el atributo clase
			d = distance.euclidean(l, instance[0:len(instance)-1])
			
			#devolvemos parejas (numero instancia test, (clase instancia train,distancia) )
			yield(i, (word[len(word)-1],d))
			
			#aumentamos contador
			i = i + 1
		

	
	# Fase REDUCER
	def reducer(self, key, line):
		
		mezcla = []
	
		# convertimos line en dos listas: una para moda y otra con distancias
		mezcla = map(list, zip(*line))
		
		# k maximas distancias
		distancias = []
		pos_distancias = []
		i = 0

		for d in mezcla[1]:
			if i == 0:
				maximo = d
				max_pos = i
			#llenamos la lista
			if len(distancias) < self.k:
				pos_distancias = pos_distancias + [i]
				distancias = distancias + [d]
			#una vez la lista esta llena se van almacenando solo los vecinos mas cercanos, eliminando los que no lo son
			elif d < maximo:
				distancias[max_pos] = d
				pos_distancias[max_pos] = i
			maximo = max(distancias)
			max_pos = distancias.index(max(distancias))
			i = i+1
			
		# nos quedamos con las clases de k
		clases = []
		for p in pos_distancias:
			clases = clases + [(mezcla[0])[p]]
			
		# devolvemos la instancia y su moda
		yield(self.test[key], kNN.moda(clases))
		

	

if __name__ == '__main__':
	l = read_file("iris.csv")
	lt = read_file("iris_test.csv")
	print test(5, l, lt)
	#mrjob
	kNN.run()
