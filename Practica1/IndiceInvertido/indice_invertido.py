from mrjob.job import MRJob
from collections import Counter
import os
import string

# Asignatura: SGDI, Practica 1 
# Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
# Este documento es fruto exclusivamente del trabajo de sus miembros

class Indice(MRJob):

	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):
		word = (line.lower()).split()
		
		# Quitamos lo que no sean letras
		aux = []
		for w in word:
			pal = "".join(filter(str.isalpha, w))
			if str.isalpha(pal):
				aux = aux + [pal]
	
		#conseguimos el nombre del archivo que estamos leyendo
		filename = os.environ['map_input_file']
		
		#devolvemos parejas: (palabra, fichero con la palabra)
		for a in aux:
			yield a, filename

	# Fase REDUCE (key es una cadena texto, values un generador de valores)
	def reducer(self, key, values):

		# contamos el numero de veces que aparece la palabra en un fichero
		dict = Counter(values)
		
		#buscamos si aparece en algun fichero mas de 20 veces
		algunoMayor = False
		for k in dict.keys():
			if dict[k] > 20:
				algunoMayor = True
		
		#si sale en alguno mas de 20 veces generamos una lista, la ordenamos
		# y la devolvemos
		lista = []
		if algunoMayor:
			for k in dict.keys():
				lista = lista + [(k,dict[k])]
			lista.sort(key=lambda x: x[1], reverse=True)
			yield key, str(lista).strip('[]')


if __name__ == '__main__':
    Indice.run()
