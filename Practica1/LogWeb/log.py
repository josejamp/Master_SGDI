from mrjob.job import MRJob
from collections import Counter
import os
import sys
import string

# Asignatura: SGDI, Practica 1 
# Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
# Este documento es fruto exclusivamente del trabajo de sus miembros

class Log(MRJob):

	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):

		word = line.split()

		#posiciones
		posHost = 0
		posFecha = 1
		posVerbo = 2
		posCodigoHTTP = len(word)-2
		posNumBytes = len(word)-1
		
		error = 0
		if word[posCodigoHTTP].startswith('5', 0,len(word[posCodigoHTTP])) or word[posCodigoHTTP].startswith('4', 0,len(word[posCodigoHTTP])):
			error = 1
			
		bytes = 0
		if word[posNumBytes] != '-':
			bytes = int(word[posNumBytes])
		
		#devolvemos parejas: (host, (1, numero de bytes, si hay error))
		yield word[posHost], (1, bytes, error)

	
	# Fase COMBINER
	def combiner(self, key, line):

		# contamos los bytes de los archivos servidos y
		# el numero de veces que salen errores
		totBytes = 0
		totCod = 0
		length = 0 
		for v in line:
			length = length + v[0]
			totBytes = totBytes + v[1]
			totCod = totCod + v[2]

		yield key, (length, totBytes, totCod)
	


	# Fase REDUCE (key es una cadena texto, values un generador de valores)
	def reducer(self, key, values):

		peticiones = 0
		numBytes = 0
		numCod = 0

		for v in values:
			peticiones = peticiones + v[0]
			numBytes = numBytes + v[1]
			numCod = numCod + v[2]

		yield key, (peticiones, numBytes, numCod)
			


if __name__ == '__main__':
    Log.run()
