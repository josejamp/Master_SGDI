from mrjob.job import MRJob

# Asignatura: SGDI, Practica 1 
# Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
# Este documento es fruto exclusivamente del trabajo de sus miembros

class Felicidad(MRJob):

	CONST_POS_WORD = 0
	CONST_POS_MEDIA = 2
	CONST_POS_RANK = 4

	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):
		word = line.split("\t")
		if float(word[self.CONST_POS_MEDIA]) < 2 and word[self.CONST_POS_RANK] != "--":
			yield "triste", word[self.CONST_POS_WORD]

	# Fase REDUCE (key es una cadena texto, values un generador de valores)
	def reducer(self, key, values):

		lista = []
		for v in values:
			lista = lista + [v]
			
		yield len(lista), lista


if __name__ == '__main__':
    Felicidad.run()
