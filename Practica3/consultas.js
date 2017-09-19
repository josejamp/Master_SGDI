/***
Autores: Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
Grupo 05

Este codigo es fruto UNICAMENTE del trabajo de sus miembros. Declaramos no 
haber colaborado de ninguna manera con otros grupos, haber compartido el codigo 
con otros ni haberlo obtenido de una fuente externa.
***/


/*******************************************************************************
**************************** AGGREGATION FRAMEWORK *****************************
*******************************************************************************/

// Listado de pais-numero de usuarios ordenado de mayor a menor por numero de 
// usuarios.
function agg1(){
	return (db.agg.aggregate(
	  [
		{ $project : { country : "$country" } } ,
		{ $group : { _id : {country:"$country"} , number : { $sum : 1 } } },
		{ $sort : { number : -1 } }
	  ]
	) );
}


// Listado de pais-numero total de posts de los 3 paises con mayor numero total 
// de posts, ordenado de mayor a menor por numero de posts.
function agg2(){
	return (db.agg.aggregate(
	  [
		{ $project : { country : "$country", num_posts: "$num_posts" } } ,
		{ $group : { _id : {country:"$country"} , number : { $sum : "$num_posts" } } },
		{ $sort : { number : -1 } },
		{ $limit : 3 }
	  ]
	) );
}

  
// Listado de aficion-numero de usuarios ordenado de mayor a menor numero de 
// usuarios.
function agg3(){
	return (db.agg.aggregate(
	  [
		{ $unwind : "$likes" },
		{ $group : { _id : "$likes" , number : { $sum : 1} } },
		{ $sort : { number : -1 } }
	  ]
	) );
}  
  
  
// Listado de aficion-numero de usuarios restringido a usuarios espanoles y
// ordenado de mayor a menor numero de usuarios.
function agg4(){
	return (db.agg.aggregate(
	  [
		{ $match: { country : "Spain" } },
		{ $unwind : "$likes" },
		{ $group : { _id : "$likes" , number : { $sum : 1} } },
		{ $sort : { number : -1 } }
	  ]
	) );
}



/*******************************************************************************
********************************** MAPREDUCE ***********************************
*******************************************************************************/
  
// Listado de aficion-numero de usuarios restringido a usuarios espanoles.
function mr1(){
	var mapFunction1 = function() {
						if(this.likes){
							for(var i = 0; i < this.likes.length; i++){
								emit( this.likes[i], 1);
						   }
					   }
                   };
	var reduceFunction1 = function(keyLike, valuesUsers) {
                          return Array.sum(valuesUsers);
                      };
	return db.agg.mapReduce(
		mapFunction1,
		reduceFunction1,
		{ 
			query : { country : "Spain" },
			out : "map_reduce_results"
		}
	);
}


// Listado de numero de aficiones-numero de usuarios, es decir, cuAntos
// usuarios tienen 0 aficiones, cuantos una aficion, cuantos dos aficiones, etc.
function mr2(){
	var mapFunction1 = function() {
						if(this.likes){
							emit( this.likes.length, 1);
					   }
					   else{
							emit(0, 1)
					   }
                   };
	var reduceFunction1 = function(keyLike, valuesUsers) {
                          return Array.sum(valuesUsers);
                      };
	return db.agg.mapReduce(
		mapFunction1,
		reduceFunction1,
		{ 
			out : "map_reduce_results"
		}
	);
}


// Listado de pais-numero de usuarios que tienen mas posts que contestaciones.
function mr3(){
	var mapFunction1 = function() {
						if(this.country && this.num_posts > this.num_answers){
							emit( this.country, 1);
					   }
                   };
	var reduceFunction1 = function(keyLike, valuesUsers) {
							return Array.sum(valuesUsers);
                      };
	return db.agg.mapReduce(
		mapFunction1,
		reduceFunction1,
		{ 
			out : "map_reduce_results"
		}
	);
}


// Listado de pais-media de posts por usuario.
function mr4(){
	var mapFunction1 = function() {
						if(this.country){
							emit( this.country, this.num_posts);
					   }
                   };
	var reduceFunction1 = function(keyLike, valuesUsers) {
							return Array.sum(valuesUsers)/valuesUsers.length;
                      };
	return db.agg.mapReduce(
		mapFunction1,
		reduceFunction1,
		{ 
			out : "map_reduce_results"
		}
	);
}




