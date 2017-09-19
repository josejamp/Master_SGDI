from __future__ import division
import csv
import pprint
import math
from scipy.spatial import distance


# Asignatura: SGDI, Practica 2 
# Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
# Este documento es fruto exclusivamente del trabajo de sus miembros.

# Inicializa el diccionario de atributos con la primera fila
# tambien devolvemos una lista auxiliar con las claves del diccionario en orden
# de inclusion, es decir, si dict = {"a":(0,[]),"b":(1,[])}, lista = [a,b]
def init_attrib_dict( primera_fila ):
	
	attrib_dict = {}
	list_aux = []
	j = 0
	for elem in primera_fila:
		attrib_dict[elem] = (j, [])
		list_aux = list_aux + [elem]
		j = j + 1
	return (attrib_dict, list_aux)
	
# Completa el diccionario de atributos
def completa_attrib_dict( fila, attrib_dict, lista_aux):

	j = 0
	for elem in fila:
		# Si no se ha incluido ya el atributo se incluye
		# ((attrib_dict[ lista_aux[j] ])[0] es la columna
		# ((attrib_dict[ lista_aux[j] ])[1] es la lista de atributos
		if not(elem in (attrib_dict[ lista_aux[j] ])[1]):
			(attrib_dict[ lista_aux[j] ]) = ((attrib_dict[ lista_aux[j] ])[0] ,(attrib_dict[ lista_aux[j] ])[1] + [elem])
		j = j + 1
	return attrib_dict
	
# Lee el archivo "filename" y genera una lista de instancias "insts",
# un diccionario de atributos con sus posibles valores y la columna que ocupan "attrib_dic",
# una lista de las posibles clases "classess",
def read_file( filename):

	# Leemos el csv
	infile = open(filename, 'r')
	reader = csv.reader(infile)
	pp = pprint.PrettyPrinter(indent=3)


	inst = []
	primera_fila = []
	attrib_dic = {}
	lista_aux = []
	classes = []
	i = 0
	for row in reader:
		 # Si no es la primera fila: metemos la fila en la lista de instancias,
		 # su clase en la lista de clases y
		 # mete la fila tambien en el diccionario de atributo
		if i > 0:
			inst = inst + [row]
			attrib_dic = completa_attrib_dict(row[0:len(row)-1], attrib_dic, lista_aux)
			if not(row[len(row)-1] in classes):
				classes = classes + [row[len(row)-1]]
		else: # En la primera fila inicializamos el diccionario de atributos
			primera_fila = row[0:len(row)-1]
			(attrib_dic, lista_aux) = init_attrib_dict( row[0:len(row)-1] )
				
		i = i + 1
	
	
	
	#print l
	#print pp.pprint(attrib_dic)
	#print pp.pprint(classes)
	
	return( inst, attrib_dic, classes)

# Calcula la proporcion de instancias de cada clase en un grupo de instancias
def proporcionInstancias(classes, inst):
	
	proporcion = {}
	for instance in inst:
		clase = instance[len(instance)-1]
		if proporcion.has_key(clase):
			proporcion[clase] += 1
		else:
			proporcion[clase] = 1
	return proporcion
	

# Calcula la entropia de un grupo de instancias, en proporcion se guarda
# el numero de instancias de cada clase
def entropia(insts, proporcion):
	
	operaciones = []
	n = len(insts)
	for key in proporcion:
		s_i = proporcion[key]
		div = s_i / n
		operaciones += [ div * math.log(div,2) ]
	return - sum(operaciones) 
	
# Separa un grupo de instancias en varios segun el atributo "atrib"
def separaInst(insts, attrib_dict, attrib):

	conjunto = {}
	# Conseguimos la posiciones del atributo y sus posibles valores
	(pos, atributos) = attrib_dict[attrib]
	# Metemos cada instancia en un grupo distinto segun el valor de su atributo "atrib"
	for at in atributos:
		conjunto[at] = []
		for inst in insts:
			if inst[pos]==at:
				conjunto[at] += [inst]
	return conjunto
				
# Calcula la entropia media de una particion de instancias
def entropiaParticion(separacion, total, classes):

	entropiaFinal = 0
	# Por cada conjunto de instancias
	for key in separacion:
		entropiaFinal += (len(separacion[key])/total)*entropia(separacion[key], proporcionInstancias(classes, separacion[key]))
		
	return entropiaFinal
	
# Calcula la clase que se repite mas veces en el conjunto de instancias "s" y el numero de veces que aparecen
def moda(s):

	clase = {}
	for v in s:
		# Obtenemos la clase del actual
		c = v[len(v)-1]
		# Si ya tenemos la clase en el diccionario se suma uno al contador
		if(clase.has_key(c)):
			clase[c] = clase[c] + 1
		# Si no, se inicia el contador a 1
		else:
			clase[c] = 1
		
	# Devolvemos la clase que mas veces se repite
	return (max(clase.iterkeys(), key=(lambda key: clase[key])), clase[max(clase.iterkeys(), key=(lambda key: clase[key]))])
	

# Calcula el arbol de clasificacion id3 dada una lista de instancias "insts",
# un diccionario de atributos con sus posibles valores y la columna que ocupan "attrib_dic",
# una lista de las posibles clases "classess", y una lista de atributos a partir de los cuales
# se puede clasificar "candidates".
# Nuestro nodos tienen la forma de tuplas (clase, None) si son hojas o
# (atrib, {valor_atrib_1 : arbol_1, valor_atrib_2 : arbol_2, ..., valor_atrib_n : arbol_n} ) si es un nodo interno,
# donde el diccionario de la tupla representa los hijos
def id3( insts , attrib_dic , classes , candidates):

	# Calculamos la clase que aparece mas veces y el numero de veces que aparece
	(cp, num_apariciones) = moda(insts)
	
	# Si todas las instancias son de una determinada clase o ya no tenemos mas atributos
	# devolvemos una hoja con la clase que aparece mas veces
	if num_apariciones == len(insts) or not candidates:
		return (cp, None)

	# Se calcula la entropia inicial
	E_inicial = entropia(insts, proporcionInstancias(classes, insts))
	
	# Se separan las instancias segun los atributos candidatos, se calculan sus entropia
	# y nos quedamos con la mejor ganancia de informacion
	ganancias = {}
	for key in candidates:
		separacion = separaInst(insts, attrib_dict, key)
		ganancias[key] = E_inicial - entropiaParticion(separacion, len(insts), classes)
	mejor = max(ganancias.iterkeys(), key=(lambda key: ganancias[key]))
	
	n = (mejor, {})
	# Separamos las instancias en grupos segun los valores del mejor atributo
	conjuntos = separaInst(insts, attrib_dict, mejor)
	# Para cada valor del atributo
	for conjunto in conjuntos:
		# Si no hay instancias se devuelve un ahoja con la clase mayoritaria
		if not conjuntos[conjunto]:
			n_prima = (cp, None)
		else:
		# Si no se llama recursivamente al id3 con los conjuntos de instancias generados
		# antes y se quita el atributo por el que hemos partido las instancias de la lista
		# de atributos candidatos
			nueva = list(candidates)
			nueva.remove(mejor)
			n_prima = id3(conjuntos[conjunto], attrib_dict, classes, nueva)
		# Se agrega el nodo que hemos generado (ya sea hoja o arbol) al arbol actual
		(n[1])[conjunto] = n_prima
	
	return n
	
# Actualiza las apariciones del nombre de nodo en el diccionario de apariciones
def actualiza_apariciones(apariciones, key):
	
	if apariciones.has_key(key):
		apariciones[key] += 1
	else:
		apariciones[key] = 0
	
# Calcula las aristas que van al nodo actual
def aristas(key, apariciones, nombre_padre, nombre_enlace):

	return [nombre_padre+' -> '+str(key)+str(apariciones[key])+' [label="'+nombre_enlace+'"];']
	
# Calcula la lista de nodos del nodo actual y de sus hijos, y la lista de aristas
# de los hijos del nodo actual
def nodos_e_hijo(key, valor, apariciones):

	n = []
	a = []
	# Si el arbol es una hoja se pone un nodo circular
	if valor == None:
		n += [str(key)+str(apariciones[key])+' [label='+'"'+key+'"];']
	else:
		# Si no es un arbol, se pone un nodo cuadrado, y se calculan los
		# nodos y las aristas del arbol recursivamente
		n += [key+str(apariciones[key])+'[label='+'"'+key+'",shape="box"];']
		for attrib in valor:
			(n_aux, a_aux) = nodos_y_aristas(valor[attrib], apariciones, key+str(apariciones[key]), attrib)
			n += n_aux
			a += a_aux
			n_aux = []
			a_aux = []
	
	return (n,a)

# Funcion que recorre el arbol id3 y genera una lista de nodos y otra de aristas,
# es recursiva, necesita el nombre del nodo padre y el nombre que se le tiene
# que poner a la arista que va entre el padre y el arbol actual
def nodos_y_aristas(id3_tree, apariciones, nombre_padre, nombre_enlace):

	# Como se puede repetir el nombre de un nodo, necesitamos un identificador
	# que sirva para diferenciar dos nodos con el mismo nombre, para eso
	# utilizamos un diccionario que contiene el numero de apariciones de los 
	# nombres que les ponemos a los nodos
	key = id3_tree[0]
	valor = id3_tree[1]
	actualiza_apariciones(apariciones, key)

	a = []
	n = []
	
	# Calculamos la lista de aristas del nodo actual
	a += aristas(key, apariciones, nombre_padre, nombre_enlace)
	
	n_aux = []
	a_aux = []
	# Calculamos la lista de nodos del nodo actual y de su hijo
	(n_aux, a_aux) = nodos_e_hijo(key, valor, apariciones)
	n += n_aux
	a += a_aux
			
	return (n,a)
			
# Funcion que recorre el arbol id3 y genera una lista de nodos y otra de aristas,
# como es recursivo, esta funcion contiene la llamada inicial
def nodos_y_aristas_inicial(id3_tree):

	apariciones = {}
	
	# Como se puede repetir el nombre de un nodo, necesitamos un identificador
	# que sirva para diferenciar dos nodos con el mismo nombre, para eso
	# utilizamos un diccionario que contiene el numero de apariciones de los 
	# nombres que les ponemos a los nodos
	key = id3_tree[0]
	valor = id3_tree[1]
	actualiza_apariciones(apariciones, key)

	n = []
	a = []
	n_aux = []
	a_aux = []
	# Calculamos la lista de nodos del nodo actual y de su hijo
	(n_aux, a_aux) = nodos_e_hijo(key, valor, apariciones)
	n += n_aux
	a += a_aux		
	
	return (n,a)
	
# Genera un fichero "filename.dot" con la representacion en .dot
# del arbol id3_tree
def write_dot_tree ( id3_tree , filename ):

	with open(filename,'w') as out:
		out.write('digraph G {\n')
		
		# Generamos una lista de nodos y otra de aristas
		(n,a) = nodos_y_aristas_inicial(id3_tree)
		
		# Escribimos los nodos
		for nodo in n:
			out.write(nodo+'\n')
			
		# Separacion estetica de nodos y aristas
		out.write('\n')
		
		# Escribimos las aristas
		for arista in a:
			out.write(arista+'\n')
		
		out.write('}\n')
	
if __name__ == '__main__':
	(insts, attrib_dict, classes) = read_file("lens.csv")
	arbol = id3(insts, attrib_dict, classes, list(attrib_dict.keys()))
	write_dot_tree(arbol, "arbolID3.dot")
