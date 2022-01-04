


var mongo = require('mongodb');

function getDB() {
	var MongoClient = require('mongodb').MongoClient;
	var url = "mongodb://mongodb017.westeurope.cloudapp.azure.com:32002";

	return new Promise((resolve, reject) => {

		MongoClient.connect(url, function(err, db) {
			if (err){
				reject(err);
			}else{
				console.log("Database connnection commpleted!");
				var dbo = db.db("db_credit");
				resolve(dbo);
			}			
		});    
	});	
}

/*
getDB().then((dbo) => {

    dbo.listCollections().toArray().then((collections) => {

        console.log(collections)
    })
} )
*/

function q_test(){
	return new Promise((resolve, reject) =>{
		getDB().then( db => {

			db.collection('members').find().toArray(function(err, result) {
				if (err) reject(err);
				resolve(result);
			});
		})
	})
}

// q_test().then((res) => {console.log(res[0])})



function q1(){
	return new Promise((resolve, reject) =>{
		getDB().then( db => {

			db.collection('members').aggregate([{
                "$unwind": "$charges"
              },
              {
                "$match": {
                  "$and": [
                    {
                      "charges.category_desc": "Real Estate loan"
                    },
                    {
                      "location.street": {
                        $regex: "Circle"
                      }
                    }
                  ]
                }
              }]
              ).toArray(function(err, result) {
				if (err) reject(err);
				resolve(result);
			});
		})
	})
}

q1().then((res) => {console.log(res[0])})